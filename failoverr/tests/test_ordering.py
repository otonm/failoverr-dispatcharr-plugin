import pytest

from failoverr.ordering import DEFAULT_CODEC_PRIORITY, quality_key


def stats(width, height, codec, fps=25, bitrate=5000):
    return {
        "video_codec": codec,
        "resolution": f"{width}x{height}",
        "bitrate_kbps": bitrate,
        "fps": fps,
        "audio_codec": "aac",
        "audio_channels": 2,
    }


def test_higher_resolution_tier_outranks_better_codec():
    """A 1080p h264 stream beats a 720p HEVC one. Tier dominates."""
    assert quality_key(stats(1920, 1080, "h264")) > quality_key(stats(1280, 720, "hevc"))


def test_codec_breaks_ties_within_a_tier():
    assert quality_key(stats(1920, 1080, "hevc")) > quality_key(stats(1920, 1080, "h264"))


def test_unlisted_codec_sorts_last_within_a_tier():
    assert quality_key(stats(1920, 1080, "mpeg2")) < quality_key(stats(1920, 1080, "avc"))


def test_fps_breaks_ties_after_codec():
    assert quality_key(stats(1920, 1080, "h264", fps=50)) > quality_key(
        stats(1920, 1080, "h264", fps=25)
    )


def test_bitrate_breaks_ties_after_fps():
    assert quality_key(stats(1920, 1080, "h264", bitrate=8000)) > quality_key(
        stats(1920, 1080, "h264", bitrate=3000)
    )


@pytest.mark.parametrize("height,expected_tier", [
    (2160, 4), (3000, 4), (1440, 3), (1080, 2), (1200, 2),
    (720, 1), (900, 1), (576, 0), (0, 0),
])
def test_resolution_tiers(height, expected_tier):
    assert quality_key(stats(height * 16 // 9, height, "h264"))[0] == expected_tier


def test_missing_stats_sort_last_and_do_not_raise():
    assert quality_key({}) < quality_key(stats(640, 480, "mpeg2"))


def test_garbage_values_do_not_raise():
    assert quality_key(
        {"resolution": "unknown", "fps": "n/a", "bitrate_kbps": None, "video_codec": None}
    ) == quality_key({})


def test_custom_codec_priority_is_respected():
    priority = ("h264", "hevc")
    assert quality_key(stats(1920, 1080, "h264"), priority) > quality_key(
        stats(1920, 1080, "hevc"), priority
    )


def test_default_codec_priority_prefers_hevc():
    assert DEFAULT_CODEC_PRIORITY[0] == "hevc"
