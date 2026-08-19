import json

import pytest

from failoverr.probing import classify, is_blank, probe
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
