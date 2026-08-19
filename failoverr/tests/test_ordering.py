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


from failoverr.ordering import Candidate, order_candidates

# Spec §16. Note the lies: A's "4K" is really 720p, B's "SD" is really
# 1080p HEVC. Ranking must follow the stats, not the names.
A_UHD = Candidate(1, "RAI 1 UHD", "A", stats(3840, 2160, "hevc"))
B_4K = Candidate(2, "Rai 1 4K", "B", stats(3840, 2160, "hevc"))
A_HEVC = Candidate(3, "RAI 1 HEVC", "A", stats(1920, 1080, "hevc"))
B_SD = Candidate(4, "RAI 1 SD", "B", stats(1920, 1080, "hevc"))
A_HD = Candidate(5, "RAI 1 HD", "A", stats(1920, 1080, "h264"))
A_4K = Candidate(6, "RAI 1 4K", "A", stats(1280, 720, "h264"))
B_HD = Candidate(7, "RAI 1 HD", "B", stats(1280, 720, "h264"))

# Deliberately shuffled: the function must sort, not preserve input order.
SEVEN = [A_4K, B_SD, A_UHD, B_HD, A_HEVC, B_4K, A_HD]


def as_pairs(result):
    return [(c.provider_id, c.name) for c in result]


def test_quality_first_matches_the_spec_fixture():
    assert as_pairs(order_candidates(SEVEN, strategy="quality_first")) == [
        ("A", "RAI 1 UHD"),
        ("B", "Rai 1 4K"),
        ("A", "RAI 1 HEVC"),
        ("B", "RAI 1 SD"),
        ("A", "RAI 1 HD"),
        ("A", "RAI 1 4K"),
        ("B", "RAI 1 HD"),
    ]


def test_provider_first_matches_the_spec_fixture():
    assert as_pairs(order_candidates(SEVEN, strategy="provider_first")) == [
        ("A", "RAI 1 UHD"),
        ("B", "Rai 1 4K"),
        ("A", "RAI 1 HEVC"),
        ("B", "RAI 1 SD"),
        ("A", "RAI 1 HD"),
        ("B", "RAI 1 HD"),
        ("A", "RAI 1 4K"),
    ]


def test_lying_names_are_ignored():
    """The whole reason this plugin exists: 'SD' outranks 'HD' and '4K'."""
    result = as_pairs(order_candidates(SEVEN))
    assert result.index(("B", "RAI 1 SD")) < result.index(("A", "RAI 1 4K"))


def test_quality_first_alternates_providers_within_a_tier():
    result = order_candidates(SEVEN)
    assert result[0].provider_id != result[1].provider_id


def test_no_candidate_is_lost_or_duplicated():
    for strategy in ("quality_first", "provider_first"):
        result = order_candidates(SEVEN, strategy=strategy)
        assert sorted(c.stream_id for c in result) == sorted(
            c.stream_id for c in SEVEN
        )


def test_single_provider_degrades_to_plain_quality_order():
    only_a = [c for c in SEVEN if c.provider_id == "A"]
    assert as_pairs(order_candidates(only_a)) == [
        ("A", "RAI 1 UHD"), ("A", "RAI 1 HEVC"), ("A", "RAI 1 HD"), ("A", "RAI 1 4K"),
    ]


def test_empty_input_gives_empty_output():
    assert order_candidates([]) == []


def test_uneven_provider_counts_do_not_drop_entries():
    """zip_longest must not truncate to the shortest provider."""
    lopsided = [A_UHD, A_HEVC, A_HD, B_4K]
    assert len(order_candidates(lopsided, strategy="provider_first")) == 4


def test_unknown_strategy_falls_back_to_quality_first():
    assert order_candidates(SEVEN, strategy="nonsense") == order_candidates(SEVEN)
