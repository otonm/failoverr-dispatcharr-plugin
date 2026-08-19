"""ffprobe/ffmpeg invocation, verdict classification, and concurrency.

classify() is pure and carries the project's most destructive failure mode:
misreading a provider's rate limit as a dead stream. Read the ordering
comment before changing anything here.
"""

import json
import re
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
