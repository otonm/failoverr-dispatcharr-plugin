import json
import threading

import pytest

from failoverr.probing import (
    Prober,
    ProbeResult,
    classify,
    is_blank,
    probe,
    should_abort_provider,
)
from failoverr.state import INCONCLUSIVE, INVALID, VALID


def ffprobe_json(streams):
    return json.dumps({"streams": streams, "format": {"bit_rate": "5000000"}})


VIDEO = {
    "codec_type": "video", "codec_name": "hevc",
    "width": 1920, "height": 1080, "avg_frame_rate": "25/1",
}
AUDIO = {"codec_type": "audio", "codec_name": "aac", "channels": 2}


# --- Valid -----------------------------------------------------------------

def test_video_plus_audio_is_valid():
    result = classify(0, ffprobe_json([VIDEO, AUDIO]), "")
    assert result.verdict == VALID


def test_valid_result_populates_the_stats_keys_dispatcharr_uses():
    result = classify(0, ffprobe_json([VIDEO, AUDIO]), "")
    assert result.stats["video_codec"] == "hevc"
    assert result.stats["resolution"] == "1920x1080"
    assert result.stats["source_fps"] == 25.0
    assert result.stats["video_bitrate"] == 5000
    assert result.stats["audio_codec"] == "aac"
    assert result.stats["audio_channels"] == 2


# --- Invalid ---------------------------------------------------------------

def test_missing_audio_is_invalid():
    """Requirement 3: 'without audio' is a rejection."""
    assert classify(0, ffprobe_json([VIDEO]), "").verdict == INVALID


def test_missing_video_is_invalid():
    assert classify(0, ffprobe_json([AUDIO]), "").verdict == INVALID


def test_zero_dimension_video_is_invalid():
    broken = dict(VIDEO, width=0, height=0)
    assert classify(0, ffprobe_json([broken, AUDIO]), "").verdict == INVALID


def test_no_streams_at_all_is_invalid():
    assert classify(0, ffprobe_json([]), "").verdict == INVALID


@pytest.mark.parametrize("stderr", [
    (
        "[http @ 0x55d] HTTP error 404 Not Found\n"
        "http://p.example/1.ts: Server returned 404 Not Found"
    ),
    (
        "[mpegts @ 0x55d] Invalid data found when processing input\n"
        "http://p.example/1.ts: Invalid data found when processing input"
    ),
    "http://p.example/1.ts: No such file or directory",
    "[mov @ 0x55d] moov atom not found",
])
def test_recognised_defects_are_invalid(stderr):
    assert classify(1, "", stderr).verdict == INVALID


# --- Inconclusive ----------------------------------------------------------

@pytest.mark.parametrize("stderr", [
    "http://p.example/1.ts: Connection timed out",
    "[tcp @ 0x55d] Connection to tcp://p.example:80 failed: Connection refused",
    (
        "[http @ 0x55d] HTTP error 403 Forbidden\n"
        "http://p.example/1.ts: Server returned 403 Forbidden"
    ),
    "[http @ 0x55d] HTTP error 429 Too Many Requests",
    "[http @ 0x55d] HTTP error 401 Unauthorized",
    (
        "[tcp @ 0x55d] Failed to resolve hostname p.example: "
        "Name or service not known"
    ),
    "Connection limit reached for this account",
    "[http @ 0x55d] HTTP error 503 Service Unavailable",
    "[tcp @ 0x55d] Network is unreachable",
])
def test_transient_failures_are_inconclusive(stderr):
    assert classify(1, "", stderr).verdict == INCONCLUSIVE


def test_timeout_signalled_by_none_returncode_is_inconclusive():
    assert classify(None, "", "").verdict == INCONCLUSIVE


def test_unrecognised_failure_defaults_to_inconclusive():
    """Never guess 'dead' from an error we do not recognise."""
    assert classify(1, "", "some novel error nobody has seen").verdict == INCONCLUSIVE


def test_unparseable_stdout_on_success_is_inconclusive():
    assert classify(0, "not json at all", "").verdict == INCONCLUSIVE


def test_rate_limit_that_also_reports_invalid_data_is_inconclusive():
    """The trap: a 403 error page makes ffprobe report invalid data too.

    Classifying this as invalid is what wipes channels overnight.
    """
    stderr = (
        "[http @ 0x55d] HTTP error 403 Forbidden\n"
        "[mpegts @ 0x55d] Invalid data found when processing input"
    )
    assert classify(1, "", stderr).verdict == INCONCLUSIVE


@pytest.mark.parametrize("stderr", [
    "http://p.example/1.ts: Protocol not found",
    "Decoder (codec none) not found for input stream #0:1",
    "Unrecognized option 'print_format'. Option not found",
    "http://p.example/live/u/p/404.ts: some novel transport error",
])
def test_unrelated_not_found_and_url_digits_are_not_misclassified_as_invalid(stderr):
    """404/not found must be anchored to real HTTP failures.

    A missing ffmpeg protocol/decoder, our own bad argv, or a stream-id
    digit that happens to read '404' inside the URL are not affirmatively
    recognised defects and must fall through to inconclusive.
    """
    assert classify(1, "", stderr).verdict == INCONCLUSIVE


@pytest.mark.parametrize("stderr", [
    "rtsp://p.example:554/ch1: Invalid data found when processing input",
    "http://p.example/live/u/p/512.ts: Invalid data found when processing input",
])
def test_url_digits_in_5xx_range_do_not_prevent_a_real_invalid_verdict(stderr):
    """A port (554) or stream id (512) must not match as a 5xx status.

    Once the bogus inconclusive match is gone, 'Invalid data found when
    processing input' is still an affirmatively recognised defect and the
    result must classify invalid.
    """
    assert classify(1, "", stderr).verdict == INVALID


def test_every_result_carries_a_reason():
    for result in (
        classify(0, ffprobe_json([VIDEO, AUDIO]), ""),
        classify(0, ffprobe_json([VIDEO]), ""),
        classify(1, "", "Connection timed out"),
    ):
        assert result.reason


# --- Invocation and blank detection -----------------------------------------


def fake_runner(returncode, stdout="", stderr="", record=None):
    def runner(argv, timeout):
        if record is not None:
            record.append((argv, timeout))
        return returncode, stdout, stderr
    return runner


def test_probe_passes_the_url_and_path_to_the_runner():
    calls = []
    probe(
        "http://p.example/1.ts", "/usr/local/bin/ffprobe", 15,
        runner=fake_runner(0, ffprobe_json([VIDEO, AUDIO]), record=calls),
    )
    argv, timeout = calls[0]
    assert argv[0] == "/usr/local/bin/ffprobe"
    assert argv[-1] == "http://p.example/1.ts"
    assert timeout == 15


def test_probe_requests_json_output():
    calls = []
    probe("http://p.example/1.ts", "ffprobe", 15,
          runner=fake_runner(0, ffprobe_json([VIDEO, AUDIO]), record=calls))
    assert "json" in " ".join(calls[0][0])


def test_probe_returns_the_classified_result():
    result = probe("http://p.example/1.ts", "ffprobe", 15,
                   runner=fake_runner(0, ffprobe_json([VIDEO, AUDIO])))
    assert result.verdict == VALID


def test_probe_reports_a_runner_exception_as_inconclusive():
    def exploding_runner(_argv, _timeout):
        raise OSError("ffprobe binary is missing")

    result = probe("http://p.example/1.ts", "ffprobe", 15, runner=exploding_runner)
    assert result.verdict == INCONCLUSIVE
    assert "missing" in result.reason


# --- Blank detection: opt-in and fail-open ---------------------------------

BLACK_STDERR = (
    "[blackdetect @ 0x55d] black_start:0 black_end:5 black_duration:5\n"
)
PARTIAL_BLACK_STDERR = (
    "[blackdetect @ 0x55d] black_start:0 black_end:0.6 black_duration:0.6\n"
)


def test_fully_black_sample_is_blank():
    assert is_blank("http://p.example/1.ts", "ffmpeg", 5,
                    runner=fake_runner(0, "", BLACK_STDERR)) is True


def test_briefly_black_sample_is_not_blank():
    """A fade or a short black frame must not reject a working stream."""
    assert is_blank("http://p.example/1.ts", "ffmpeg", 5,
                    runner=fake_runner(0, "", PARTIAL_BLACK_STDERR)) is False


def test_no_blackdetect_output_means_not_blank():
    assert is_blank("http://p.example/1.ts", "ffmpeg", 5,
                    runner=fake_runner(0, "", "")) is False


def test_ffmpeg_error_fails_open():
    """Spec §9: any ffmpeg error leaves the stream valid."""
    assert is_blank("http://p.example/1.ts", "ffmpeg", 5,
                    runner=fake_runner(1, "", "Connection timed out")) is False


def test_ffmpeg_exception_fails_open():
    def exploding_runner(_argv, _timeout):
        raise OSError("ffmpeg binary is missing")

    assert is_blank("http://p.example/1.ts", "ffmpeg", 5,
                    runner=exploding_runner) is False


def test_malformed_black_duration_fails_open():
    """Malformed duration values must not crash; fail-open is mandatory."""
    malformed_stderr = "[blackdetect @ 0x55d] black_duration:1.2.3\n"
    assert is_blank("http://p.example/1.ts", "ffmpeg", 5,
                    runner=fake_runner(0, "", malformed_stderr)) is False


# --- Concurrency and provider abort -------------------------------------------


def test_provider_not_aborted_below_the_minimum_sample():
    """One genuinely dead stream must not abort a healthy provider."""
    assert should_abort_provider([INVALID] * 4) is False


def test_provider_aborted_when_every_verdict_is_bad():
    assert should_abort_provider([INCONCLUSIVE] * 5) is True
    assert should_abort_provider([INVALID] * 5) is True
    assert should_abort_provider([INVALID, INCONCLUSIVE] * 3) is True


def test_a_single_success_prevents_abort():
    assert should_abort_provider([INCONCLUSIVE] * 9 + [VALID]) is False


def test_per_provider_concurrency_is_never_exceeded():
    live = {"A": 0}
    peak = {"A": 0}
    lock = threading.Lock()

    def counting_probe(_url, _ffprobe_path, _timeout, _runner=None):
        with lock:
            live["A"] += 1
            peak["A"] = max(peak["A"], live["A"])
        try:
            return ProbeResult(VALID, {}, "ok")
        finally:
            with lock:
                live["A"] -= 1

    prober = Prober("ffprobe", 15, per_account=1, global_limit=8, cooldown=0,
                    probe_fn=counting_probe, sleep_fn=lambda _s: None)
    threads = [
        threading.Thread(target=prober.probe_one, args=("A", f"u{i}"))
        for i in range(8)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert peak["A"] == 1, "provider connection cap was exceeded"


def test_probes_on_an_aborted_provider_return_inconclusive_without_running():
    calls = []

    def failing_probe(url, _ffprobe_path, _timeout, _runner=None):
        calls.append(url)
        return ProbeResult(INCONCLUSIVE, {}, "connection limit")

    prober = Prober("ffprobe", 15, per_account=1, global_limit=4, cooldown=0,
                    probe_fn=failing_probe, sleep_fn=lambda _s: None)
    for i in range(5):
        prober.probe_one("A", f"u{i}")
    assert "A" in prober.aborted_providers

    before = len(calls)
    result = prober.probe_one("A", "u-after-abort")
    assert len(calls) == before, "no further probes may be sent to this provider"
    assert result.verdict == INCONCLUSIVE, (
        "an aborted provider's streams must never be marked invalid"
    )


def test_cooldown_is_applied_between_probes_on_one_provider():
    slept = []
    prober = Prober("ffprobe", 15, per_account=1, global_limit=4, cooldown=2,
                    probe_fn=lambda *_a, **_k: ProbeResult(VALID, {}, "ok"),
                    sleep_fn=slept.append)
    prober.probe_one("A", "u1")
    prober.probe_one("A", "u2")
    assert slept == [2, 2]
