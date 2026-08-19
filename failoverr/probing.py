"""ffprobe/ffmpeg invocation, verdict classification, and concurrency.

classify() is pure and carries the project's most destructive failure mode:
misreading a provider's rate limit as a dead stream. Read the ordering
comment before changing anything here.
"""

import json
import re
import subprocess
import threading
import time
from collections import defaultdict
from typing import NamedTuple

from .state import INCONCLUSIVE, INVALID, VALID

# Checked FIRST. A provider returning a 403 error page also makes ffprobe
# report "Invalid data found", so an invalid-first check would classify a
# rate limit as a dead stream and eventually detach a working one.
INCONCLUSIVE_PATTERNS = [
    r"timed out",
    r"connection reset",
    r"connection refused",
    r"network is unreachable",
    r"no route to host",
    r"name or service not known",
    r"temporary failure in name resolution",
    r"failed to resolve",
    r"could not resolve",
    r"\b401\b|unauthorized",
    r"\b403\b|forbidden",
    r"\b429\b|too many requests",
    r"\b5\d\d\b",  # any 5xx is the provider's problem, not the stream's
    r"connection limit",
    r"max(imum)?\s+(number of\s+)?connections",
    r"end of file",
    r"i/o error",
    r"interrupted",
]

# Only affirmatively recognised defects. Anything not listed here and not
# above stays inconclusive.
INVALID_PATTERNS = [
    r"\b404\b|not found",
    r"\b410\b|gone",
    r"invalid data found",
    r"no such file or directory",
    r"moov atom not found",
    r"could not find codec parameters",
]

_INCONCLUSIVE_RE = re.compile("|".join(INCONCLUSIVE_PATTERNS), re.IGNORECASE)
_INVALID_RE = re.compile("|".join(INVALID_PATTERNS), re.IGNORECASE)


class ProbeResult(NamedTuple):
    verdict: str
    stats: dict
    reason: str


def _fraction(value):
    """Parse ffprobe-style frame rates as 'num/den'."""
    try:
        text = str(value or "0")
        if "/" in text:
            num, den = text.split("/", 1)
            den = float(den)
            return round(float(num) / den, 3) if den else 0.0
        return float(text)
    except (TypeError, ValueError, ZeroDivisionError):
        return 0.0


def _int(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _build_stats(video, audio, container):
    bitrate = _int(container.get("bit_rate")) or _int(video.get("bit_rate"))
    return {
        "video_codec": video.get("codec_name") or "",
        "resolution": f"{_int(video.get('width'))}x{_int(video.get('height'))}",
        "video_bitrate": bitrate // 1000,
        "source_fps": _fraction(video.get("avg_frame_rate")),
        "audio_codec": audio.get("codec_name") or "",
        "audio_channels": _int(audio.get("channels")),
    }


def classify(returncode, stdout, stderr):  # noqa: PLR0911
    """Bucket an ffprobe run into valid / invalid / inconclusive."""
    stderr = stderr or ""

    if returncode is None:
        return ProbeResult(INCONCLUSIVE, {}, "probe timed out")

    match = _INCONCLUSIVE_RE.search(stderr)
    if match:
        return ProbeResult(INCONCLUSIVE, {}, f"transient: {match.group(0)}")

    match = _INVALID_RE.search(stderr)
    if match:
        return ProbeResult(INVALID, {}, f"defect: {match.group(0)}")

    if returncode != 0:
        # Recognised nothing. Default to inconclusive so an unfamiliar
        # provider error can never contribute to removing a stream.
        return ProbeResult(
            INCONCLUSIVE, {}, f"unrecognised failure (exit {returncode})"
        )

    try:
        payload = json.loads(stdout or "")
    except ValueError:
        return ProbeResult(INCONCLUSIVE, {}, "ffprobe output was not valid JSON")

    streams = payload.get("streams") or []
    videos = [
        s for s in streams
        if s.get("codec_type") == "video"
        and _int(s.get("width")) > 0
        and _int(s.get("height")) > 0
    ]
    audios = [s for s in streams if s.get("codec_type") == "audio"]

    if not videos:
        return ProbeResult(INVALID, {}, "no video stream with a real resolution")
    if not audios:
        return ProbeResult(INVALID, {}, "no audio stream")

    stats = _build_stats(videos[0], audios[0], payload.get("format") or {})
    return ProbeResult(VALID, stats, f"ok {stats['resolution']} {stats['video_codec']}")


# A blank verdict requires essentially the whole sample to be black.
BLANK_FRACTION = 0.9
_BLACK_DURATION = re.compile(r"black_duration:\s*([0-9.]+)")


def run_command(argv, timeout):
    """Default runner.

    Returns (returncode, stdout, stderr); returncode is None on timeout,
    which classify() reads as inconclusive.
    """
    try:
        proc = subprocess.run(
            argv, capture_output=True, text=True, timeout=timeout, check=False
        )
    except subprocess.TimeoutExpired:
        return None, "", ""
    return proc.returncode, proc.stdout or "", proc.stderr or ""


def probe(url, ffprobe_path, timeout, runner=run_command):
    argv = [
        ffprobe_path,
        "-v", "error",
        "-print_format", "json",
        "-show_streams",
        "-show_format",
        "-analyzeduration", "5000000",
        "-probesize", "5000000",
        url,
    ]
    try:
        returncode, stdout, stderr = runner(argv, timeout)
    except Exception as exc:  # noqa: BLE001
        return ProbeResult(INCONCLUSIVE, {}, f"could not run ffprobe: {exc}")
    return classify(returncode, stdout, stderr)


def is_blank(url, ffmpeg_path, seconds, runner=run_command):
    """Check if the sampled window is essentially entirely black.

    Fails open: any error at all returns False, leaving the stream valid.
    """
    seconds = max(1.0, float(seconds or 1))
    argv = [
        ffmpeg_path,
        "-t", str(seconds),
        "-i", url,
        "-vf", "blackdetect=d=0.5:pix_th=0.10",
        "-an", "-f", "null", "-",
    ]
    try:
        returncode, _stdout, stderr = runner(argv, seconds * 4 + 10)
        if returncode != 0:
            return False
        black = sum(float(d) for d in _BLACK_DURATION.findall(stderr or ""))
        return black >= seconds * BLANK_FRACTION
    except Exception:  # noqa: BLE001
        return False


# Never abort a provider on a sample of one: a provider that legitimately
# carries a single dead stream is not a provider that is down.
PROVIDER_ABORT_MINIMUM = 5


def should_abort_provider(verdicts):
    """Determine if a provider should be aborted based on its probing verdicts.

    Returns True when a provider has produced enough verdicts, all of them bad.
    """
    if len(verdicts) < PROVIDER_ABORT_MINIMUM:
        return False
    return all(v != VALID for v in verdicts)


class Prober:
    """Runs probes under a per-provider cap and a global cap.

    Different providers are probed in parallel while each stays serialized
    internally. That is most of the available speedup.
    """

    def __init__(self, ffprobe_path, timeout, per_account, global_limit,  # noqa: PLR0913,PLR0917
                 cooldown, probe_fn=probe, sleep_fn=time.sleep):
        self.ffprobe_path = ffprobe_path
        self.timeout = timeout
        self.cooldown = cooldown
        self.probe_fn = probe_fn
        self.sleep_fn = sleep_fn
        self._per_account = max(1, int(per_account))
        self._global = threading.Semaphore(max(1, int(global_limit)))
        self._locks = {}
        self._locks_guard = threading.Lock()
        self._verdicts = defaultdict(list)
        self._verdicts_guard = threading.Lock()
        self.aborted_providers = set()

    def _semaphore(self, provider_id):
        with self._locks_guard:
            if provider_id not in self._locks:
                self._locks[provider_id] = threading.Semaphore(self._per_account)
            return self._locks[provider_id]

    def probe_one(self, provider_id, url):
        if provider_id in self.aborted_providers:
            return ProbeResult(
                INCONCLUSIVE, {},
                "provider aborted this run; existing ranking left untouched",
            )

        with self._global, self._semaphore(provider_id):
            result = self.probe_fn(url, self.ffprobe_path, self.timeout)
            if self.cooldown:
                self.sleep_fn(self.cooldown)

        with self._verdicts_guard:
            verdicts = self._verdicts[provider_id]
            verdicts.append(result.verdict)
            if should_abort_provider(verdicts):
                self.aborted_providers.add(provider_id)
        return result
