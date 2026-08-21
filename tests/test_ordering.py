import pytest

from failoverr.ordering import (
    DEFAULT_CODEC_PRIORITY,
    Candidate,
    order_candidates,
    quality_key,
    rewrite_plan,
)


def stats(width, height, codec, fps=25, bitrate=5000):
    """Build a stats dict using Dispatcharr's real stream_stats key names.

    Confirmed via Diagnose against the live pool (CLAUDE.md §4).
    """
    return {
        "video_codec": codec,
        "resolution": f"{width}x{height}",
        "video_bitrate": bitrate,
        "source_fps": fps,
        "audio_codec": "aac",
        "audio_channels": 2,
    }


def test_higher_resolution_tier_outranks_better_codec():
    """A 1080p h264 stream beats a 720p HEVC one. Tier dominates."""
    assert quality_key(stats(1920, 1080, "h264")) > quality_key(
        stats(1280, 720, "hevc")
    )


def test_codec_breaks_ties_within_a_tier():
    assert quality_key(stats(1920, 1080, "hevc")) > quality_key(
        stats(1920, 1080, "h264")
    )


def test_unlisted_codec_sorts_last_within_a_tier():
    assert quality_key(stats(1920, 1080, "mpeg2")) < quality_key(
        stats(1920, 1080, "avc")
    )


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
        {
            "resolution": "unknown",
            "source_fps": "n/a",
            "video_bitrate": None,
            "video_codec": None,
        }
    ) == quality_key({})


def test_custom_codec_priority_is_respected():
    priority = ("h264", "hevc")
    assert quality_key(stats(1920, 1080, "h264"), priority) > quality_key(
        stats(1920, 1080, "hevc"), priority
    )


def test_default_codec_priority_prefers_hevc():
    assert DEFAULT_CODEC_PRIORITY[0] == "hevc"


def test_response_time_bucketing_creates_ties_for_ranking():
    a, b = stats(1920, 1080, "hevc"), stats(1920, 1080, "hevc")
    assert quality_key(
        a, response_time_ms=240, response_time_bucket_ms=250
    ) == quality_key(b, response_time_ms=10, response_time_bucket_ms=250)
    assert quality_key(
        a, response_time_ms=260, response_time_bucket_ms=250
    ) != quality_key(b, response_time_ms=240, response_time_bucket_ms=250)


def test_missing_response_time_sorts_worst():
    assert quality_key(
        stats(1920, 1080, "hevc"), response_time_ms=None
    ) < quality_key(stats(1920, 1080, "hevc"), response_time_ms=5000)


def test_response_time_decides_before_codec_when_enabled():
    fast_bad_codec = stats(1920, 1080, "avc")
    slow_good_codec = stats(1920, 1080, "hevc")
    assert quality_key(fast_bad_codec, response_time_ms=100) > quality_key(
        slow_good_codec, response_time_ms=2000
    )


def test_disabling_response_time_lets_codec_decide_instead():
    fast_bad_codec = stats(1920, 1080, "avc")
    slow_good_codec = stats(1920, 1080, "hevc")
    assert quality_key(
        slow_good_codec, response_time_ms=2000, rank_by_response_time=False
    ) > quality_key(fast_bad_codec, response_time_ms=100, rank_by_response_time=False)


def test_disabling_resolution_lets_codec_decide_instead():
    fhd_bad_codec = stats(1920, 1080, "avc")
    hd_good_codec = stats(1280, 720, "hevc")
    assert quality_key(fhd_bad_codec) > quality_key(hd_good_codec)
    assert quality_key(hd_good_codec, rank_by_resolution=False) > quality_key(
        fhd_bad_codec, rank_by_resolution=False
    )


def test_disabling_codec_lets_fps_decide_instead():
    good_codec_slow_fps = stats(1920, 1080, "hevc", fps=25)
    bad_codec_fast_fps = stats(1920, 1080, "avc", fps=50)
    assert quality_key(good_codec_slow_fps) > quality_key(bad_codec_fast_fps)
    assert quality_key(bad_codec_fast_fps, rank_by_codec=False) > quality_key(
        good_codec_slow_fps, rank_by_codec=False
    )


def test_disabling_fps_lets_bitrate_decide_instead():
    fast_fps_low_bitrate = stats(1920, 1080, "h264", fps=50, bitrate=1000)
    slow_fps_high_bitrate = stats(1920, 1080, "h264", fps=25, bitrate=9000)
    assert quality_key(fast_fps_low_bitrate) > quality_key(slow_fps_high_bitrate)
    assert quality_key(fast_fps_low_bitrate, rank_by_fps=False) < quality_key(
        slow_fps_high_bitrate, rank_by_fps=False
    )


def test_disabling_bitrate_ties_when_nothing_else_differs():
    high_bitrate = stats(1920, 1080, "h264", bitrate=9000)
    low_bitrate = stats(1920, 1080, "h264", bitrate=1000)
    assert quality_key(high_bitrate) > quality_key(low_bitrate)
    assert quality_key(high_bitrate, rank_by_bitrate=False) == quality_key(
        low_bitrate, rank_by_bitrate=False
    )


def test_order_candidates_forwards_toggles_to_quality_key():
    fast_bad = Candidate(1, "fast bad codec", "A", stats(1920, 1080, "avc"), 100)
    slow_good = Candidate(2, "slow good codec", "A", stats(1920, 1080, "hevc"), 2000)

    result = order_candidates([fast_bad, slow_good])
    assert result[0].stream_id == 1

    result_no_response_time = order_candidates(
        [fast_bad, slow_good], rank_by_response_time=False
    )
    assert result_no_response_time[0].stream_id == 2


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


class UniqueOrderStore:
    """Simulates a unique (channel, order) constraint."""

    def __init__(self, initial):
        self.rows = dict(initial)

    def apply(self, plan):
        for stream_id, new_order in plan:
            for other_id, other_order in self.rows.items():
                if other_id != stream_id and other_order == new_order:
                    raise AssertionError(
                        f"unique constraint violated: order {new_order} "
                        f"already held by stream {other_id}"
                    )
            self.rows[stream_id] = new_order


def test_a_naive_swap_would_violate_the_constraint():
    """Establishes that the offset trick is solving a real problem."""
    store = UniqueOrderStore({1: 0, 2: 1})
    with pytest.raises(AssertionError, match="unique constraint"):
        store.apply([(2, 0), (1, 1)])


def test_offset_plan_reverses_order_without_collision():
    store = UniqueOrderStore({1: 0, 2: 1})
    store.apply(rewrite_plan({1: 0, 2: 1}, [2, 1], use_offset=True))
    assert store.rows[2] == 0
    assert store.rows[1] == 1


def test_offset_plan_handles_a_full_reshuffle():
    current = {10: 0, 11: 1, 12: 2, 13: 3}
    desired = [13, 11, 10, 12]
    store = UniqueOrderStore(current)
    store.apply(rewrite_plan(current, desired, use_offset=True))
    assert [store.rows[s] for s in desired] == [0, 1, 2, 3]


def test_detached_rows_keep_offset_values_and_never_collide():
    """Stream 12 is being truncated away; it must not block final positions."""
    current = {10: 0, 11: 1, 12: 2}
    store = UniqueOrderStore(current)
    store.apply(rewrite_plan(current, [11, 10], use_offset=True))
    assert store.rows[11] == 0 and store.rows[10] == 1
    assert store.rows[12] >= 100000


def test_without_offset_the_plan_is_just_final_assignments():
    assert rewrite_plan({1: 0, 2: 1}, [2, 1], use_offset=False) == [(2, 0), (1, 1)]


def test_new_streams_not_in_current_are_assigned():
    plan = rewrite_plan({1: 0}, [1, 99], use_offset=True)
    assert (99, 1) in plan


def test_empty_desired_produces_no_final_assignments():
    """A channel that matched nothing is never cleared — spec §12."""
    assert rewrite_plan({1: 0, 2: 1}, [], use_offset=True) == []

