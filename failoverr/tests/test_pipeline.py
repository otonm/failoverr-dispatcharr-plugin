import csv
import pathlib
import re

import pytest

from failoverr.naming import normalize
from failoverr.pipeline import (
    LOCK_TTL_SECONDS,
    Budget,
    StreamRow,
    acquire_lock,
    build_index,
    clear_lock,
    find_matches,
    load_settings,
    lock_status,
    plan_channel,
    release_lock,
    report_path,
    write_report,
)
from failoverr.state import INCONCLUSIVE, INVALID, VALID, State


def row(stream_id, name, provider="A", height=1080, codec="h264"):
    return StreamRow(
        stream_id=stream_id,
        name=name,
        provider_id=provider,
        url=f"http://{provider}.example/{stream_id}.ts",
        stats={
            "video_codec": codec,
            "resolution": f"{height * 16 // 9}x{height}",
            "video_bitrate": 5000,
            "source_fps": 25,
            "audio_codec": "aac",
            "audio_channels": 2,
        },
        tokens=normalize(name),
    )


POOL = [
    row(1, "IT: RAI 1 HD", "A"),
    row(2, "IT: Rai 1 4K", "B"),
    row(3, "IT: RAI1 FHD", "A"),
    row(4, "IT: RAI 2 HD", "A"),
    row(5, "IT: RAI Sport 1 HD", "B"),
    row(6, "IT: RAI News 24 HD", "A"),
]


# --- Indexing and matching -------------------------------------------------

def test_index_groups_streams_that_reduce_to_the_same_tokens():
    index = build_index(POOL)
    assert {r.stream_id for r in index[frozenset(("rai", "1"))]} == {1, 2, 3}


def test_strict_matching_finds_only_the_right_channel():
    index = build_index(POOL)
    found = find_matches(normalize("RAI 1"), index, mode="strict")
    assert {r.stream_id for r in found} == {1, 2, 3}


def test_strict_matching_excludes_rai_2_and_rai_sport():
    index = build_index(POOL)
    found = {
        r.stream_id for r in find_matches(normalize("RAI 1"), index, mode="strict")
    }
    assert 4 not in found and 5 not in found and 6 not in found


def test_channel_with_no_matches_returns_empty():
    index = build_index(POOL)
    assert find_matches(normalize("BBC One"), index, mode="strict") == []


def test_fuzzy_matching_widens_the_net():
    index = build_index(POOL)
    found = {r.stream_id for r in
             find_matches(normalize("RAI 1"), index, mode="fuzzy", threshold=60)}
    assert 5 in found, "fuzzy at 60 should pull in RAI Sport 1 (scores 62)"


def test_empty_pool_indexes_cleanly():
    assert build_index([]) == {}


# --- Per-channel planning --------------------------------------------------

def make_state(tmp_path, verdicts):
    state = State(tmp_path / "state.json")
    for stream_id, (verdict, count) in verdicts.items():
        for _ in range(count):
            url = f"http://A.example/{stream_id}.ts"
            state.record(stream_id, url, verdict, now=0.0)
    return state


def plan(state, attached=(), candidates=POOL[:3], max_streams=10):
    return plan_channel(
        attached_ids=set(attached),
        candidates=candidates,
        state=state,
        threshold=3,
        max_streams=max_streams,
        strategy="quality_first",
        codec_priority=("hevc", "h265", "h264", "avc"),
    )


def test_valid_streams_are_attached(tmp_path):
    state = make_state(tmp_path, {1: (VALID, 1), 2: (VALID, 1), 3: (VALID, 1)})
    ordered, detach = plan(state)
    assert set(ordered) == {1, 2, 3}
    assert detach == []


def test_unprobed_streams_are_never_attached(tmp_path):
    """Attaching something we have not confirmed defeats the whole plugin."""
    state = make_state(tmp_path, {1: (VALID, 1)})
    ordered, _ = plan(state)
    assert ordered == [1]


def test_a_channel_with_no_valid_streams_is_never_cleared(tmp_path):
    """Spec §12: empty result means leave alone."""
    state = make_state(tmp_path, {})
    ordered, detach = plan(state, attached=(1, 2, 3))
    assert ordered == []
    assert detach == [], "an empty result must not detach anything"


def test_one_failure_demotes_instead_of_detaching(tmp_path):
    state = make_state(tmp_path, {1: (VALID, 1), 2: (VALID, 1), 3: (INVALID, 1)})
    ordered, detach = plan(state, attached=(1, 2, 3))
    assert detach == []
    assert ordered[-1] == 3, "a single failure moves the stream to the bottom"


def test_three_consecutive_failures_detach(tmp_path):
    state = make_state(tmp_path, {1: (VALID, 1), 2: (VALID, 1), 3: (INVALID, 3)})
    ordered, detach = plan(state, attached=(1, 2, 3))
    assert detach == [3]
    assert 3 not in ordered


def test_inconclusive_attached_streams_are_kept(tmp_path):
    """A timeout must not cost a stream its place."""
    state = make_state(tmp_path, {1: (VALID, 1), 2: (INCONCLUSIVE, 5)})
    ordered, detach = plan(state, attached=(1, 2))
    assert 2 in ordered
    assert detach == []


def test_inconclusive_streams_that_are_not_attached_are_not_added(tmp_path):
    state = make_state(tmp_path, {1: (VALID, 1), 2: (INCONCLUSIVE, 1)})
    ordered, _ = plan(state, attached=(1,))
    assert 2 not in ordered


def test_never_probed_attached_streams_are_kept(tmp_path):
    """No verdict at all (not even inconclusive) must not read as dead.

    Happens on a first run, or whenever the probe budget runs out before
    every attached candidate is (re-)probed.
    """
    state = make_state(tmp_path, {1: (VALID, 1)})  # stream 2 never probed
    ordered, detach = plan(state, attached=(1, 2))
    assert 2 in ordered
    assert detach == []


def test_truncation_detaches_the_excess(tmp_path):
    state = make_state(tmp_path, {1: (VALID, 1), 2: (VALID, 1), 3: (VALID, 1)})
    ordered, detach = plan(state, attached=(1, 2, 3), max_streams=2)
    assert len(ordered) == 2
    assert len(detach) == 1
    assert set(ordered) | set(detach) == {1, 2, 3}


def test_truncation_never_detaches_never_probed_streams(tmp_path):
    """Never-probed attached streams must survive truncation too.

    Only ranked (VALID/demoted-INVALID) streams count against max_streams;
    streams with no probe history yet are exempt and must never land in
    detach purely because the channel is over its cap.
    """
    state = make_state(tmp_path, {1: (VALID, 1)})  # streams 2 and 3 never probed
    ordered, detach = plan(state, attached=(1, 2, 3), max_streams=1)
    assert 2 not in detach
    assert 3 not in detach
    assert set(ordered) | set(detach) == {1, 2, 3}


def test_result_is_provider_interleaved(tmp_path):
    state = make_state(tmp_path, {1: (VALID, 1), 2: (VALID, 1), 3: (VALID, 1)})
    ordered, _ = plan(state)
    providers = [r.provider_id for sid in ordered for r in POOL if r.stream_id == sid]
    assert providers[0] != providers[1]


def test_planning_is_idempotent(tmp_path):
    state = make_state(tmp_path, {1: (VALID, 1), 2: (VALID, 1), 3: (VALID, 1)})
    first, _ = plan(state, attached=(1, 2, 3))
    second, _ = plan(state, attached=set(first))
    assert first == second


# --- Settings and reporting -------------------------------------------------

def test_write_report_creates_a_csv_with_a_header(tmp_path):
    path = write_report(
        [{"channel": "RAI 1", "position": 0, "stream": "IT: Rai 1 4K",
          "provider": "B", "verdict": "valid", "resolution": "3840x2160",
          "codec": "hevc", "action": "attach"}],
        tmp_path / "report.csv",
    )
    with pathlib.Path(path).open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["channel"] == "RAI 1"
    assert rows[0]["action"] == "attach"


def test_write_report_creates_missing_directories(tmp_path):
    path = write_report([{"channel": "X", "position": 0, "stream": "s",
                          "provider": "A", "verdict": "valid",
                          "resolution": "", "codec": "", "action": "keep"}],
                        tmp_path / "deep" / "nested" / "report.csv")
    assert pathlib.Path(path).read_text()


def test_write_report_with_no_rows_still_writes_a_header(tmp_path):
    path = write_report([], tmp_path / "empty.csv")
    assert "channel" in pathlib.Path(path).read_text()


def test_report_path_is_version_independent_and_well_formed():
    """Regression: this used to call datetime.UTC, a Python 3.11+ attribute."""
    path = report_path("preview")
    assert isinstance(path, pathlib.Path)
    assert path.parent == pathlib.Path("/data/exports")
    assert re.match(r"^failoverr-preview-\d{8}-\d{6}\.csv$", path.name)


def test_load_settings_applies_defaults_for_missing_keys():
    settings = load_settings({"settings": {}})
    assert settings["dry_run"] is True
    assert settings["match_mode"] == "strict"
    assert settings["removal_failure_threshold"] == 3


def test_load_settings_coerces_numeric_strings():
    """Dispatcharr may hand back numbers as strings."""
    settings = load_settings({"settings": {"max_streams_per_channel": "5",
                                           "probe_ttl_hours": "12"}})
    assert settings["max_streams_per_channel"] == 5
    assert settings["probe_ttl_hours"] == 12


def test_load_settings_coerces_boolean_strings():
    """bool("false") is True in plain Python - this must not leak through."""
    settings = load_settings({"settings": {
        "map_number_words": "false", "blank_detect": "true",
    }})
    assert settings["map_number_words"] is False
    assert settings["blank_detect"] is True


def test_load_settings_passes_through_a_real_bool():
    settings = load_settings({"settings": {"dry_run": False}})
    assert settings["dry_run"] is False


def test_load_settings_parses_the_token_lists():
    settings = load_settings({"settings": {
        "strip_tokens": "hd, 4k ,uhd",
        "codec_priority": "h264,hevc",
    }})
    assert settings["strip_tokens"] == ("hd", "4k", "uhd")
    assert settings["codec_priority"] == ("h264", "hevc")


def test_load_settings_falls_back_to_default_tokens_when_blank():
    settings = load_settings({"settings": {"strip_tokens": "  "}})
    assert "hevc" in settings["strip_tokens"]


def test_load_settings_parses_the_channel_name_list():
    settings = load_settings({"settings": {"channel_names": "RAI 1\n RAI 2 \n\n"}})
    assert settings["channel_names"] == ["RAI 1", "RAI 2"]


@pytest.fixture(autouse=True)
def _reset_lock():
    clear_lock()
    yield
    clear_lock()


def test_lock_is_acquired_when_free():
    assert acquire_lock("run", now=0.0) is True


def test_second_acquisition_is_refused():
    acquire_lock("run", now=0.0)
    assert acquire_lock("preview", now=10.0) is False


def test_lock_is_reusable_after_release():
    acquire_lock("run", now=0.0)
    release_lock()
    assert acquire_lock("run", now=10.0) is True


def test_stale_lock_is_stolen_after_the_ttl():
    """A run killed mid-flight must not block the plugin forever."""
    acquire_lock("run", now=0.0)
    assert acquire_lock("run", now=LOCK_TTL_SECONDS + 1) is True


def test_lock_status_reports_the_holder():
    acquire_lock("probe_only", now=0.0)
    assert lock_status()["holder"] == "probe_only"


def test_clear_lock_releases_a_held_lock():
    acquire_lock("run", now=0.0)
    clear_lock()
    assert lock_status()["holder"] is None


# --- Budgets ---------------------------------------------------------------

def test_budget_allows_probes_up_to_the_limit():
    budget = Budget(max_probes=3, max_minutes=60, now_fn=lambda: 0.0)
    for _ in range(3):
        assert budget.allow()
        budget.spend()
    assert budget.allow() is False
    assert "probe" in budget.reason


def test_budget_stops_on_wall_clock():
    clock = {"t": 0.0}
    budget = Budget(max_probes=1000, max_minutes=10, now_fn=lambda: clock["t"])
    assert budget.allow()
    clock["t"] = 11 * 60
    assert budget.allow() is False
    assert "time" in budget.reason


def test_a_fresh_budget_has_no_reason():
    assert Budget(10, 10, now_fn=lambda: 0.0).reason is None
