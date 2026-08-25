import json
import threading

import pytest

from failoverr.probing import (
    Prober,
    ProbeResult,
    classify,
    is_blank,
    probe,
)
from failoverr.state import INCONCLUSIVE, INVALID, VALID


def ffprobe_json(streams, packets=None, format_bitrate="5000000"):
    payload = {"streams": streams, "format": {}}
    if format_bitrate is not None:
        payload["format"]["bit_rate"] = format_bitrate
    if packets is not None:
        payload["packets"] = packets
    return json.dumps(payload)


def video_packets(count, size=25000, duration_time=0.025, stream_index=0):
    return [
        {
            "stream_index": stream_index,
            "size": str(size),
            "duration_time": str(duration_time),
        }
        for _ in range(count)
    ]


VIDEO = {
    "index": 0, "codec_type": "video", "codec_name": "hevc",
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


def test_declared_bitrate_takes_priority_over_packet_calc():
    """Prefer a declared bit_rate over the packet-based estimate.

    Live MPEG-TS rarely declares bit_rate, but when it does, trust it over
    the noisier packet-based estimate.
    """
    packets = video_packets(40, size=99999)  # would compute a very different value
    result = classify(0, ffprobe_json([VIDEO, AUDIO], packets=packets), "")
    assert result.stats["video_bitrate"] == 5000


def test_packet_based_bitrate_fills_in_when_no_declared_bitrate():
    """Fall back to a packet-based estimate when nothing declares bit_rate.

    Live streams almost never declare bit_rate. Sum packet size over
    packet duration for the video stream as a fallback estimate.
    """
    packets = video_packets(40, size=25000, duration_time=0.025)  # 8000 kbps
    result = classify(
        0, ffprobe_json([VIDEO, AUDIO], packets=packets, format_bitrate=None), ""
    )
    assert result.stats["video_bitrate"] == 8000


def test_packet_based_bitrate_ignores_other_streams():
    """Audio packets must not dilute the video bitrate estimate."""
    video_pkts = video_packets(40, size=25000, duration_time=0.025, stream_index=0)
    audio_pkts = video_packets(100, size=999999, duration_time=0.01, stream_index=1)
    result = classify(
        0,
        ffprobe_json(
            [VIDEO, AUDIO], packets=video_pkts + audio_pkts, format_bitrate=None
        ),
        "",
    )
    assert result.stats["video_bitrate"] == 8000


def test_packet_based_bitrate_needs_a_minimum_sample():
    """Leave the estimate unset below the reliability floor.

    Too few packets makes the estimate noise-dominated (observed: a
    2-packet sample once spiked to 22924 kbps). Below the floor, leave it
    unset rather than persist a misleading number.
    """
    packets = video_packets(29, size=25000, duration_time=0.025)
    result = classify(
        0, ffprobe_json([VIDEO, AUDIO], packets=packets, format_bitrate=None), ""
    )
    assert result.stats["video_bitrate"] == 0


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


def test_run_command_timeout_returns_none_returncode():
    """run_command translates TimeoutExpired to (None, '', '')."""
    import subprocess

    from failoverr.probing import probe

    def slow_runner(argv, timeout):
        raise subprocess.TimeoutExpired(cmd=argv, timeout=timeout)

    result = probe("http://example.com/stream.ts", "ffprobe", 1, runner=slow_runner)
    assert result.verdict == INCONCLUSIVE
    assert "could not run ffprobe" in result.reason


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


# --- URL scheme allow-list (security) ---------------------------------------


def test_probe_rejects_a_file_url_before_calling_ffprobe():
    """A file:// URL must never reach ffprobe's argv (local filesystem access)."""
    called = []
    result = probe(
        "file:///etc/passwd", "ffprobe", 15,
        runner=fake_runner(0, ffprobe_json([VIDEO, AUDIO]), record=called),
    )
    assert result.verdict == INCONCLUSIVE
    assert not called, "file:// URL must not reach the ffprobe runner"


def test_probe_rejects_a_concat_url_before_calling_ffprobe():
    """concat: is ffmpeg's own protocol — must not reach argv."""
    called = []
    result = probe(
        "concat:1.ts|2.ts", "ffprobe", 15,
        runner=fake_runner(0, ffprobe_json([VIDEO, AUDIO]), record=called),
    )
    assert result.verdict == INCONCLUSIVE
    assert not called, "concat: URL must not reach the ffprobe runner"


def test_probe_rejects_a_leading_dash_url():
    """A URL starting with - could be read as a flag by ffprobe."""
    called = []
    result = probe(
        "-something", "ffprobe", 15,
        runner=fake_runner(0, ffprobe_json([VIDEO, AUDIO]), record=called),
    )
    assert result.verdict == INCONCLUSIVE
    assert not called, "leading-dash URL must not reach the ffprobe runner"


def test_is_blank_rejects_a_file_url():
    """file:// must not reach ffmpeg either."""
    called = []
    assert is_blank(
        "file:///etc/passwd", "ffmpeg", 5,
        runner=fake_runner(0, "", BLACK_STDERR, record=called),
    ) is False
    assert not called, "file:// URL must not reach the ffmpeg runner"


def test_probe_allows_an_http_url():
    """A valid http URL must still reach the runner."""
    calls = []
    probe(
        "http://p.example/1.ts", "ffprobe", 15,
        runner=fake_runner(0, ffprobe_json([VIDEO, AUDIO]), record=calls),
    )
    assert len(calls) == 1


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


def test_probe_requests_packets_for_the_bitrate_fallback():
    calls = []
    probe("http://p.example/1.ts", "ffprobe", 15,
          runner=fake_runner(0, ffprobe_json([VIDEO, AUDIO]), record=calls))
    argv = calls[0][0]
    assert "-show_packets" in argv
    assert "-read_intervals" in argv


def test_probe_returns_the_classified_result():
    result = probe("http://p.example/1.ts", "ffprobe", 15,
                   runner=fake_runner(0, ffprobe_json([VIDEO, AUDIO])))
    assert result.verdict == VALID


def test_probe_attaches_response_time_on_a_valid_result():
    result = probe("http://p.example/1.ts", "ffprobe", 15,
                   runner=fake_runner(0, ffprobe_json([VIDEO, AUDIO])))
    assert isinstance(result.response_time_ms, int)
    assert result.response_time_ms >= 0


def test_probe_does_not_attach_response_time_on_an_invalid_result():
    result = probe(
        "http://p.example/1.ts", "ffprobe", 15,
        runner=fake_runner(1, "", "Invalid data found when processing input"),
        sleep_fn=lambda _s: None,
    )
    assert result.response_time_ms is None


def test_probe_does_not_attach_response_time_on_an_inconclusive_result():
    result = probe("http://p.example/1.ts", "ffprobe", 15,
                   runner=fake_runner(1, "", "Connection timed out"))
    assert result.response_time_ms is None


def test_probe_reports_a_runner_exception_as_inconclusive():
    def exploding_runner(_argv, _timeout):
        raise OSError("ffprobe binary is missing")

    result = probe("http://p.example/1.ts", "ffprobe", 15, runner=exploding_runner)
    assert result.verdict == INCONCLUSIVE
    assert "missing" in result.reason


def test_probe_retries_once_and_recovers_from_a_transient_invalid_result():
    """A live stream's transient join glitch must not read as dead."""
    outcomes = [
        (1, "", "Invalid data found when processing input"),
        (0, ffprobe_json([VIDEO, AUDIO]), ""),
    ]

    def flaky_runner(_argv, _timeout):
        return outcomes.pop(0)

    result = probe(
        "http://p.example/1.ts", "ffprobe", 15,
        runner=flaky_runner, sleep_fn=lambda _s: None,
    )
    assert result.verdict == VALID


def test_probe_does_not_mask_a_genuinely_dead_stream():
    """A defect that reproduces on retry must still be reported as invalid."""
    calls = []
    result = probe(
        "http://p.example/1.ts", "ffprobe", 15,
        runner=fake_runner(
            1, "", "Invalid data found when processing input", record=calls,
        ),
        sleep_fn=lambda _s: None,
    )
    assert result.verdict == INVALID
    assert len(calls) == 2


def test_probe_waits_before_retrying_an_invalid_result():
    slept = []
    probe(
        "http://p.example/1.ts", "ffprobe", 15,
        runner=fake_runner(1, "", "Invalid data found when processing input"),
        sleep_fn=slept.append,
    )
    assert slept == [2]


def test_probe_does_not_sleep_when_the_first_attempt_is_valid():
    slept = []
    probe(
        "http://p.example/1.ts", "ffprobe", 15,
        runner=fake_runner(0, ffprobe_json([VIDEO, AUDIO])),
        sleep_fn=slept.append,
    )
    assert slept == []


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


# --- Concurrency -----------------------------------------------------------


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


def test_cooldown_is_applied_between_probes_on_one_provider():
    slept = []
    prober = Prober("ffprobe", 15, per_account=1, global_limit=4, cooldown=2,
                    probe_fn=lambda *_a, **_k: ProbeResult(VALID, {}, "ok"),
                    sleep_fn=slept.append)
    prober.probe_one("A", "u1")
    prober.probe_one("A", "u2")
    assert slept == [2, 2]


def test_one_stream_going_bad_does_not_affect_a_sibling_on_the_same_provider():
    """A verdict must be strictly per-stream, never influenced by siblings."""
    def probe_fn(url, _ffprobe_path, _timeout, _runner=None):
        return ProbeResult(INVALID, {}, "dead") if url == "bad" else \
            ProbeResult(VALID, {}, "ok")

    prober = Prober("ffprobe", 15, per_account=1, global_limit=4, cooldown=0,
                    probe_fn=probe_fn, sleep_fn=lambda _s: None)
    for _ in range(10):
        prober.probe_one("A", "bad")
    result = prober.probe_one("A", "good")
    assert result.verdict == VALID, (
        "a healthy stream must still be probed and pass, no matter how many "
        "other streams on the same provider just failed"
    )
