import csv
import pathlib
import re
import subprocess
import sys
import threading
import time
import types

import pytest

from failoverr import models_access as models_access_module
from failoverr import pipeline as pipeline_module
from failoverr import probing as probing_module
from failoverr.naming import normalize
from failoverr.pipeline import (
    Budget,
    StreamRow,
    acquire_lock,
    build_index,
    clear_lock,
    find_matches,
    load_settings,
    lock_status,
    plan_channel,
    refresh_lock,
    release_lock,
    report_path,
    run_pipeline,
    write_report,
)
from failoverr.probing import ProbeResult
from failoverr.state import INCONCLUSIVE, INVALID, VALID, State

# Lock TTL is a private constant in pipeline.py; tests use the value directly.
LOCK_TTL_SECONDS = 1800


def row(stream_id, name, provider=1, height=1080, codec="h264"):
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
    row(1, "IT: RAI 1 HD", 1),
    row(2, "IT: Rai 1 4K", 2),
    row(3, "IT: RAI1 FHD", 1),
    row(4, "IT: RAI 2 HD", 1),
    row(5, "IT: RAI Sport 1 HD", 2),
    row(6, "IT: RAI News 24 HD", 1),
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


def test_plan_channel_ranks_by_response_time_from_state(tmp_path):
    # slow is listed *before* fast in candidates, so a pre-fix ranked() that
    # never consults state.response_time_ms() would tie on every factor
    # (both rows share identical stats) and fall back to insertion order,
    # i.e. [1, 2] - producing ordered[0] == 1 instead of 2. That makes this
    # assertion fail unambiguously before the fix and pass only once
    # ranked() actually reads response time from state.
    state = State(tmp_path / "state.json")
    slow = row(1, "IT: RAI 1 HD", "A", codec="hevc")
    fast = row(2, "IT: RAI 1 HD", "A", codec="hevc")
    state.record(1, slow.url, VALID, now=0.0, response_time_ms=5000)
    state.record(2, fast.url, VALID, now=0.0, response_time_ms=100)

    ordered, _detach = plan_channel(
        attached_ids=set(), candidates=[slow, fast], state=state, threshold=3,
        max_streams=10, strategy="quality_first", codec_priority=("hevc",),
    )

    assert ordered[0] == 2


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


def test_plan_channel_truncation_drops_unattached_valid_streams(tmp_path):
    """Non-attached VALID streams beyond max_streams are silently dropped.

    plan_channel only keeps max_streams from the ranked (VALID/demoted) list.
    Unattached VALID streams that fall outside this limit are neither in
    ordered nor in detach - they are simply not included in the result.
    This test pins the current behavior so it cannot change silently.
    """
    state = make_state(tmp_path, {
        1: (VALID, 1), 2: (VALID, 1), 3: (VALID, 1),
        4: (VALID, 1), 5: (VALID, 1),  # not attached, only VALID
    })
    ordered, detach = plan(state, attached=(1, 2), max_streams=3)
    # Only 3 streams kept (max_streams), none detached because unattached
    # VALID streams don't count as "detached" - they were never attached.
    assert len(ordered) == 3
    assert detach == []
    assert 1 in ordered and 2 in ordered
    # One of 3,4,5 made it in; the other two were silently dropped.
    # The exact one depends on provider interleaving/ranking.
    assert set(ordered).issubset({1, 2, 3, 4, 5})
    assert len(set(ordered) & {3, 4, 5}) == 1


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


def test_write_report_prunes_old_reports_down_to_max_reports(tmp_path):
    """Regression: exports accumulated one file per run with no cleanup.

    An unattended cron deployment (one run/day by default) used to leave one
    CSV in /data/exports per run forever - unbounded disk growth with no
    rotation, retention limit, or cleanup anywhere. write_report now prunes
    the directory down to _MAX_REPORTS (100) after each write.
    """
    import os
    import time

    max_reports = 100

    exports = tmp_path / "exports"
    exports.mkdir()
    # Seed more reports than the cap, each one second older than the next so
    # the mtime sort is deterministic. Seed mtimes are in the past so the
    # freshly-written report below is the newest of all.
    base = time.time() - 20000
    for i in range(max_reports + 5):
        p = exports / f"failoverr-run-{i:04d}.csv"
        p.write_text("old")
        os.utime(p, (base + i, base + i))

    newest = exports / "failoverr-run-newest.csv"
    write_report(
        [{"channel": "X", "position": 0, "stream": "s", "provider": "A",
          "verdict": "valid", "resolution": "", "codec": "", "action": "keep"}],
        newest,
    )

    remaining = sorted(exports.glob("failoverr-*.csv"))
    assert len(remaining) == max_reports, (
        f"rotation must prune down to {max_reports}, found {len(remaining)}"
    )
    assert newest in remaining, "the report just written must survive rotation"
    # The oldest 6 seeds (the first-seeded, lowest mtimes) are the ones pruned.
    assert not (exports / "failoverr-run-0000.csv").exists()
    assert not (exports / "failoverr-run-0005.csv").exists()


def test_write_report_rotation_leaves_unrelated_files_alone(tmp_path):
    """Only failoverr-*.csv files are pruned; other files in the dir survive."""
    max_reports = 100

    exports = tmp_path / "exports"
    exports.mkdir()
    unrelated = exports / "not-a-failoverr-report.csv"
    unrelated.write_text("keep me")

    for i in range(max_reports + 2):
        (exports / f"failoverr-run-{i:04d}.csv").write_text("old")

    write_report([], exports / "failoverr-run-newest.csv")

    assert unrelated.exists(), "rotation must not touch non-report files"


def test_report_path_is_version_independent_and_well_formed():
    """Regression: this used to call datetime.UTC, a Python 3.11+ attribute."""
    path = report_path("preview")
    assert isinstance(path, pathlib.Path)
    assert path.parent == pathlib.Path("/data/exports")
    assert re.match(r"^failoverr-preview-\d{8}-\d{6}\.csv$", path.name)


def test_report_row_renders_a_zero_response_time_without_blank():
    """Regression: the 0ms CSV rendering bug (e6f8a30) lives in one place now.

    A measured 0ms is a real value, not "never measured" - it must render as
    "0", not "". The pre-dedup code hid it behind ``or ""`` (falsy) at three
    separate sites; the _report_row helper is the single site now, so this
    locks the ``is not None`` check against drift.
    """
    from failoverr.pipeline import _report_row

    state = State(path=pathlib.Path("/nonexistent/does-not-matter.json"))
    state.record(1, "http://a.example/1.ts", VALID, response_time_ms=0)
    stream = row(1, "IT: RAI 1 HD", "A")

    rendered = _report_row(
        types.SimpleNamespace(name="RAI 1"), 0, 1, stream, state, "attach",
    )

    assert rendered["response_time_ms"] == 0, (
        "a genuine 0ms must render as 0, not be blanked by a falsy check"
    )
    assert rendered["resolution"] == "1920x1080"


def test_report_row_blanks_the_stats_columns_on_a_detach():
    """A detached stream's resolution/codec/response_time are blanked.

    detach is the one action that discards the row's stats even when the row
    is present - the stream is being removed, so its last resolution/codec
    must not appear next to an action that says 'detach'.
    """
    from failoverr.pipeline import _report_row

    state = State(path=pathlib.Path("/nonexistent/does-not-matter.json"))
    state.record(1, "http://a.example/1.ts", VALID, response_time_ms=180)
    stream = row(1, "IT: RAI 1 HD", "A")

    rendered = _report_row(
        types.SimpleNamespace(name="RAI 1"), "", 1, stream, state, "detach",
    )

    assert rendered["action"] == "detach"
    assert rendered["resolution"] == ""
    assert rendered["codec"] == ""
    assert rendered["response_time_ms"] == "", (
        "detach must blank response_time_ms even when state has a value"
    )
    assert rendered["stream"] == "IT: RAI 1 HD", "the stream name is still shown"


def test_report_row_falls_back_to_the_stream_id_when_the_row_is_missing():
    """A kept/detach row whose stream isn't in the candidate set shows its id."""
    from failoverr.pipeline import _report_row

    state = State(path=pathlib.Path("/nonexistent/does-not-matter.json"))

    rendered = _report_row(
        types.SimpleNamespace(name="RAI 1"), "", 42, None, state, "detach",
    )

    assert rendered["stream"] == 42
    assert rendered["provider"] == ""


def test_load_settings_applies_defaults_for_missing_keys():
    settings = load_settings({"settings": {}})
    assert settings["dry_run"] is True
    assert settings["match_mode"] == "strict"
    assert settings["removal_failure_threshold"] == 3


def test_load_settings_defaults_the_ranking_toggles_to_true():
    settings = load_settings({"settings": {}})
    assert settings["rank_by_resolution"] is True
    assert settings["rank_by_response_time"] is True
    assert settings["rank_by_codec"] is True
    assert settings["rank_by_fps"] is True
    assert settings["rank_by_bitrate"] is True
    assert settings["response_time_bucket_ms"] == 250


def test_load_settings_coerces_the_ranking_toggles():
    settings = load_settings({"settings": {
        "rank_by_fps": "false", "response_time_bucket_ms": "500",
    }})
    assert settings["rank_by_fps"] is False
    assert settings["response_time_bucket_ms"] == 500


def test_load_settings_coerces_numeric_strings():
    """Dispatcharr may hand back numbers as strings."""
    settings = load_settings({"settings": {"max_streams_per_channel": "5",
                                           "probe_ttl_hours": "12"}})
    assert settings["max_streams_per_channel"] == 5
    assert settings["probe_ttl_hours"] == 12


def test_load_settings_falls_back_to_default_on_an_overflowing_number():
    """Regression: OverflowError.

    A settings-form value like "1e400" raises OverflowError, not
    TypeError/ValueError - it must fall back, not crash every action.
    """
    settings = load_settings({"settings": {"max_streams_per_channel": "1e400"}})
    assert settings["max_streams_per_channel"] == 10


def test_load_settings_logs_when_a_bad_number_falls_back_to_default(caplog):
    """Regression: a failed coercion used to fall back to default silently.

    A value that failed coercion used to fall back silently, so a typo was
    invisible in docker logs - only the raw form value was logged
    (Plugin.run), not the fact that a default was substituted for it.
    """
    import logging

    with caplog.at_level(logging.WARNING, logger="failoverr"):
        settings = load_settings({"settings": {"max_streams_per_channel": "1e400"}})

    assert settings["max_streams_per_channel"] == 10
    matched = [
        r for r in caplog.records
        if "max_streams_per_channel" in r.message and "1e400" in r.message
    ]
    assert matched, caplog.records


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
def _reset_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline_module, "_LOCK_PATH", str(tmp_path / "run.lock"))
    monkeypatch.setattr(pipeline_module, "_CANCEL_PATH", str(tmp_path / "cancel.flag"))
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


def test_clear_lock_refuses_a_still_active_run():
    """Clear Lock must not free the lock out from under a genuinely active run.

    Freeing it would let a second run start while the first is still
    probing - two runs writing state.json/attaching streams at once. It
    must refuse and point at Stop instead, and must not itself set the
    cancel flag - that would silently turn Clear Lock into a second Stop
    button with different wording, which is exactly the confusion Stop was
    added to resolve.
    """
    acquire_lock("run", now=1000.0)
    result = clear_lock(now=1005.0)  # well within LOCK_TTL_SECONDS
    assert result["status"] == "error"
    assert lock_status()["holder"] == "run"
    assert pipeline_module.cancel_requested() is False


def test_clear_lock_force_releases_a_stale_lock_and_drops_any_cancel_flag():
    acquire_lock("run", now=0.0)
    pipeline_module.request_cancel()
    clear_lock(now=LOCK_TTL_SECONDS + 1)
    assert lock_status()["holder"] is None
    assert pipeline_module.cancel_requested() is False


def test_clear_lock_reports_already_clear_when_nothing_is_running():
    result = clear_lock()
    assert result["status"] == "ok"


def test_corrupt_lock_file_is_treated_as_empty_and_logged(caplog):
    """A truncated/corrupt lock file must not block the plugin and must log."""
    import logging
    lock_file = pathlib.Path(pipeline_module._LOCK_PATH)
    lock_file.write_text("{not valid json")

    with caplog.at_level(logging.WARNING, logger="failoverr"):
        assert acquire_lock("run", now=0.0) is True

    assert lock_status()["holder"] == "run"
    assert any("lock file corrupt" in r.message for r in caplog.records)


def test_stop_run_requests_cancellation_for_an_active_run():
    acquire_lock("run", now=time.time())
    result = pipeline_module.stop_run()
    assert result["status"] == "ok"
    assert lock_status()["holder"] == "run", "Stop must not itself release the lock"
    assert pipeline_module.cancel_requested() is True


def test_stop_run_is_a_noop_when_nothing_is_running():
    result = pipeline_module.stop_run()
    assert result["status"] == "ok"
    assert pipeline_module.cancel_requested() is False


def test_clear_state_resets_the_probe_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline_module, "STATE_PATH", tmp_path / "state.json")
    state = State(tmp_path / "state.json")
    state.record(1, "http://a.example/1.ts", VALID)
    state.meta["last_run"] = 123.0
    state.save()

    result = pipeline_module.clear_state()

    assert result["status"] == "ok"
    reloaded = State.load(tmp_path / "state.json")
    assert reloaded.streams == {}
    assert reloaded.meta == {}


def test_clear_state_refuses_while_a_run_is_genuinely_active(tmp_path, monkeypatch):
    """Clearing mid-run would be silently undone by that run's next save().

    It holds its own State instance in memory, so it must be refused rather
    than racing - same reasoning as clear_lock's active/stale split.
    """
    monkeypatch.setattr(pipeline_module, "STATE_PATH", tmp_path / "state.json")
    acquire_lock("run", now=time.time())

    result = pipeline_module.clear_state()

    assert result["status"] == "error"


def test_clear_state_succeeds_when_the_lock_is_only_stale(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline_module, "STATE_PATH", tmp_path / "state.json")
    acquire_lock("run", now=0.0)  # far enough in the past to read as stale

    result = pipeline_module.clear_state()

    assert result["status"] == "ok"


# --- run_preview (Preview action) -------------------------------------------


def test_run_preview_assembles_matched_and_attached_candidates_via_the_shared_merge(
    tmp_path, monkeypatch,
):
    """Regression: run_preview hand-copied _channel_candidates' merge logic.

    A future change to the matched+attached candidate assembly could be
    applied to run_pipeline's copy and missed in run_preview's, so Preview
    could show a plan Run doesn't actually produce. run_preview now calls
    _channel_candidates, so the two share one merge. This proves an
    attached-but-not-matched stream (3) still reaches the candidate set and
    shows up in the preview - the exact merge rule the duplication risked
    drifting on.
    """
    channel = types.SimpleNamespace(name="RAI 1")
    pool = [
        row(1, "IT: RAI 1 HD", "A"),
        row(2, "IT: RAI 1 4K", "B"),
        row(4, "IT: RAI 1 FHD", "C"),
    ]
    attached_not_matched = row(3, "IT: RAI 3 HD", "A")  # name doesn't reduce to rai/1
    state = _make_state(tmp_path)
    state.record(1, pool[0].url, VALID)
    state.record(2, pool[1].url, VALID)
    state.record(4, pool[2].url, INVALID)  # matched but not attached: would be probed

    monkeypatch.setattr(models_access_module, "resolve_models", object)
    monkeypatch.setattr(pipeline_module, "iter_pool", lambda *_a, **_kw: iter(pool))
    monkeypatch.setattr(
        pipeline_module, "select_channels", lambda *_a, **_kw: [channel]
    )
    monkeypatch.setattr(
        pipeline_module, "iter_attached_rows",
        lambda *_a, **_kw: iter([attached_not_matched]),
    )
    monkeypatch.setattr(
        pipeline_module.State, "load", staticmethod(lambda *_a, **_kw: state)
    )
    monkeypatch.setattr(
        pipeline_module, "report_path", lambda *_a, **_kw: tmp_path / "preview.csv"
    )

    result = pipeline_module.run_preview({"settings": {}})

    assert result["status"] == "ok"
    assert result["channels"] == 1
    with pathlib.Path(result["report"]).open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    actions = {r["stream"]: r["action"] for r in rows}
    # The attached-but-not-matched stream 3 reached the candidate set via the
    # shared merge and is kept - the rule the duplicated copy risked dropping.
    assert actions.get("IT: RAI 3 HD") == "keep"
    # Matched-but-invalid stream 4 is labelled "would be probed", not attached.
    assert actions.get("IT: RAI 1 FHD") == "matched - would be probed"
    assert actions.get("IT: RAI 1 HD") == "attach"


def test_channel_candidates_reads_the_link_table_once_per_channel_per_pass(
    tmp_path, monkeypatch,
):
    """Regression: the attached-link table was queried up to 4x per channel.

    _channel_candidates used to call both attached_rows() and
    iter_attached_rows() - two queries for data available in one - and was
    itself called twice per channel (the stale-count pre-pass plus the main
    loop): 4 link-table reads per channel per run. attached_rows is gone now
    and attached_ids is derived from iter_attached_rows' StreamRows, so the
    link table is read once per pass: 2 per channel (pre-pass + main), not 4.
    """
    channel_a = types.SimpleNamespace(name="RAI 1")
    channel_b = types.SimpleNamespace(name="RAI 2")
    attached = {
        "RAI 1": [row(1, "IT: RAI 1 HD", "A")],
        "RAI 2": [row(2, "IT: RAI 2 HD", "A")],
    }
    state = _make_state(tmp_path)
    for rows in attached.values():
        for r in rows:
            state.record(r.stream_id, r.url, VALID)  # fresh: no probing

    calls = {"n": 0}

    def counting_iter(_resolved, ch, _settings):
        calls["n"] += 1
        return iter(attached[ch.name])

    monkeypatch.setattr(
        pipeline_module, "select_channels",
        lambda *_a, **_kw: [channel_a, channel_b],
    )
    monkeypatch.setattr(pipeline_module, "iter_attached_rows", counting_iter)
    monkeypatch.setattr(pipeline_module, "iter_pool", lambda *_a, **_kw: iter([]))
    monkeypatch.setattr(
        pipeline_module.State, "load", staticmethod(lambda *_a, **_kw: state)
    )
    monkeypatch.setattr(
        pipeline_module, "report_path", lambda *_a, **_kw: tmp_path / "run.csv"
    )
    monkeypatch.setattr(models_access_module, "resolve_models", object)
    monkeypatch.setattr(pipeline_module, "_close_old_connections", lambda: None)
    monkeypatch.setattr(pipeline_module, "_close_connection", lambda: None)
    monkeypatch.setattr(
        models_access_module, "apply_channel_plan",
        lambda *_a, **_kw: {"attached": 0, "detached": 0},
    )

    run_pipeline({"settings": {}}, mode="run")

    # 2 channels x (1 pre-pass + 1 main loop) = 4 link-table reads. The
    # pre-merge code did attached_rows + iter_attached_rows per pass = 8.
    assert calls["n"] == 4, (
        f"link table read {calls['n']} times for 2 channels; expected 4 "
        "(one per channel per pass), not the pre-merge 8"
    )


def test_refresh_lock_prevents_a_steal_at_the_original_ttl_deadline():
    """Finding: a long run must keep its own lock fresh.

    Without a heartbeat, a second run could steal the lock once
    LOCK_TTL_SECONDS has passed since the ORIGINAL acquire.
    """
    acquire_lock("run", now=0.0)
    # Well before the original deadline, the run heartbeats its lock.
    refresh_lock(now=1000.0)
    # At the moment the lock would have gone stale relative to the original
    # acquisition (now=0.0), it must still be held - the refresh worked.
    assert acquire_lock("preview", now=LOCK_TTL_SECONDS + 1) is False
    # It does go stale relative to the refreshed timestamp, once enough
    # time has passed since THAT.
    assert acquire_lock("preview", now=1000.0 + LOCK_TTL_SECONDS + 1) is True


def test_release_lock_no_ops_when_it_no_longer_owns_the_lock():
    """Regression: release_lock wiped any lock, not just its own.

    A run whose lock was force-cleared (Clear Lock) and then re-acquired by
    a second run would still call release_lock() on its way out, clearing
    the SECOND run's lock and letting a third start - the two-runs-at-once
    hazard the lock exists to prevent. release_lock now checks it still
    owns the lock (holder matches) before clearing.
    """
    acquire_lock("run", now=0.0)
    release_lock()  # force-clear (the Clear Lock path, holder=None)
    acquire_lock("reorder_only", now=10.0)
    # Run A's release_lock("run") must NOT clear B's lock.
    release_lock("run")
    assert lock_status()["holder"] == "reorder_only"


def test_refresh_lock_no_ops_when_it_no_longer_owns_the_lock():
    """Regression: refresh_lock bumped any lock's since, not just its own.

    A stale run heartbeating after its lock was cleared and re-acquired
    would bump the NEW run's since, making the new lock look freshly held
    by the wrong run. refresh_lock now checks it still owns the lock.
    """
    acquire_lock("run", now=0.0)
    release_lock()
    acquire_lock("reorder_only", now=10.0)
    # Run A's refresh_lock("run") must NOT touch B's lock timestamp.
    refresh_lock("run", now=9999.0)
    assert lock_status()["holder"] == "reorder_only"
    assert lock_status()["since"] == 10.0


def test_acquire_lock_uses_a_ttl_that_outlives_max_run_minutes():
    """LOCK_TTL_SECONDS (1800) was shorter than max_run_minutes (3600 default).

    A legitimate run still within its own budget could have its lock
    auto-stolen by a second acquire_lock partway through. acquire_lock now
    takes a ttl that outlives the run's budget, and start() passes one that
    does, so a run within budget is not stealable until past that ttl.
    """
    big_ttl = LOCK_TTL_SECONDS + 3600  # a longer-than-default run's ttl
    acquire_lock("run", now=0.0, ttl=big_ttl)
    # A second run with the same adaptive ttl can't steal within the budget:
    # past the old default TTL (1801s) but inside this run's budget (big_ttl).
    assert acquire_lock("preview", now=LOCK_TTL_SECONDS + 1, ttl=big_ttl) is False
    # Only past the adaptive ttl is it stealable.
    assert acquire_lock("preview", now=big_ttl + 1, ttl=big_ttl) is True


def test_run_pipeline_heartbeats_the_lock_between_channels(
    tmp_path, monkeypatch,
):
    """The lock heartbeat fired only every 25 probes, never during a reorder_only run.

    reorder_only skips probing entirely, and the 25-probe cadence never
    fires between channels either, so a non-probing run's lock could go
    stale and be force-cleared mid-run. The lock now refreshes at every
    channel boundary and after indexing.
    """
    channel_a = types.SimpleNamespace(name="RAI 1")
    channel_b = types.SimpleNamespace(name="RAI 2")
    attached = {
        "RAI 1": [row(1, "IT: RAI 1 HD", "A")],
        "RAI 2": [row(2, "IT: RAI 2 HD", "A")],
    }
    state = _make_state(tmp_path)
    for rows in attached.values():
        for r in rows:
            state.record(r.stream_id, r.url, VALID)

    refreshes = []
    real_refresh = pipeline_module.refresh_lock

    def counting_refresh(*args, **kwargs):
        refreshes.append(True)
        return real_refresh(*args, **kwargs)

    monkeypatch.setattr(pipeline_module, "refresh_lock", counting_refresh)
    monkeypatch.setattr(
        pipeline_module, "select_channels",
        lambda *_a, **_kw: [channel_a, channel_b],
    )
    monkeypatch.setattr(
        pipeline_module, "iter_attached_rows",
        lambda _r, ch, _s: iter(attached[ch.name]),
    )
    monkeypatch.setattr(
        pipeline_module.State, "load", staticmethod(lambda *_a, **_kw: state)
    )
    monkeypatch.setattr(
        pipeline_module, "report_path", lambda *_a, **_kw: tmp_path / "r.csv"
    )
    monkeypatch.setattr(models_access_module, "resolve_models", object)
    monkeypatch.setattr(pipeline_module, "_close_old_connections", lambda: None)
    monkeypatch.setattr(pipeline_module, "_close_connection", lambda: None)
    monkeypatch.setattr(
        models_access_module, "apply_channel_plan",
        lambda *_a, **_kw: {"attached": 0, "detached": 0},
    )

    run_pipeline({"settings": {}}, mode="reorder_only")

    # 1 after-index heartbeat + 2 per-channel heartbeats (reorder_only never
    # probes, so the 25-probe cadence contributes nothing).
    assert len(refreshes) == 3, (
        f"expected 3 lock heartbeats (after-index + 2 channels), "
        f"got {len(refreshes)}"
    )


def test_lock_is_visible_across_separate_processes():
    """The lock must stop a celery-worker run from overlapping a uwsgi one.

    Those are two separate OS processes with no shared Python memory, so an
    in-memory dict (the old implementation) could never see across them.
    Proving it here means actually spawning a second process against the
    same lock file, not just calling the function twice in this one.
    """
    lock_path = pipeline_module._LOCK_PATH
    acquire_lock("run", now=0.0)

    script = (
        "import sys; sys.path.insert(0, sys.argv[1]); "
        "from failoverr import pipeline; "
        "pipeline._LOCK_PATH = sys.argv[2]; "
        "print(pipeline.acquire_lock('scheduled_run', now=10.0))"
    )
    repo_root = str(pathlib.Path(__file__).resolve().parents[1])
    result = subprocess.run(
        [sys.executable, "-c", script, repo_root, lock_path],
        capture_output=True, text=True, check=True,
    )
    assert result.stdout.strip() == "False"


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


def test_budget_clamps_non_positive_values_to_one():
    """Budget clamps non-positive max_probes/max_minutes to 1.

    This prevents a misconfigured zero or negative budget from causing
    infinite loops or immediate exhaustion.
    """
    budget = Budget(max_probes=0, max_minutes=0, now_fn=lambda: 0.0)
    assert budget.max_probes == 1
    assert budget.max_seconds == 60  # 1 minute * 60

    budget = Budget(max_probes=-5, max_minutes=-10, now_fn=lambda: 0.0)
    assert budget.max_probes == 1
    assert budget.max_seconds == 60


def test_budget_stops_and_flags_canceled_when_cancel_fn_fires():
    budget = Budget(1000, 60, now_fn=lambda: 0.0, cancel_fn=lambda: True)
    assert budget.allow() is False
    assert budget.canceled is True
    assert "cancel" in budget.reason


# --- run_pipeline mode-routing ----------------------------------------------
#
# run_pipeline touches the Django ORM through a handful of module-level
# helpers (select_channels, iter_attached_rows, iter_pool). These tests stub
# those helpers, following the monkeypatch-the-seam pattern already used for
# Django-touching code in test_diagnose.py and test_models_access.py, so the
# mode-routing logic can run offline.


def _make_state(tmp_path):
    return State(path=tmp_path / "state.json")


def _patch_common(monkeypatch, tmp_path, channel, attached, state):
    monkeypatch.setattr(
        pipeline_module, "select_channels", lambda *_a, **_kw: [channel]
    )
    monkeypatch.setattr(
        pipeline_module, "iter_attached_rows", lambda *_a, **_kw: iter(attached)
    )
    monkeypatch.setattr(
        pipeline_module.State, "load", staticmethod(lambda *_a, **_kw: state)
    )
    monkeypatch.setattr(
        pipeline_module, "report_path", lambda mode: tmp_path / f"{mode}.csv"
    )
    monkeypatch.setattr(models_access_module, "resolve_models", object)
    # Real Django isn't installed offline; run_pipeline/start() call these
    # seams unconditionally now (Fix 2), so give them a harmless default.
    # Individual tests override one of these to assert it was really called.
    monkeypatch.setattr(pipeline_module, "_close_old_connections", lambda: None)
    monkeypatch.setattr(pipeline_module, "_close_connection", lambda: None)


def test_reorder_only_never_detaches_via_a_stale_cross_run_failure_counter(
    tmp_path, monkeypatch
):
    """Finding 1 regression.

    A stream that racked up 3 consecutive INVALID verdicts under prior
    Probe Only runs must not be detached by a later Reorder Only run:
    that action's own description promises "nothing attached or detached".
    """
    channel = types.SimpleNamespace(name="RAI 1")
    healthy = row(1, "IT: RAI 1 HD", "A")
    failing = row(2, "IT: RAI 1 4K", "B")
    state = _make_state(tmp_path)
    state.record(1, healthy.url, VALID)
    for _ in range(3):
        state.record(2, failing.url, INVALID)
    _patch_common(monkeypatch, tmp_path, channel, [healthy, failing], state)

    apply_calls = []

    def fake_apply(resolved, ch, ordered, detach, dry_run):  # noqa: ARG001
        apply_calls.append((ordered, detach))
        return {"attached": 0, "detached": len(detach)}

    monkeypatch.setattr(models_access_module, "apply_channel_plan", fake_apply)

    result = run_pipeline({"settings": {}}, mode="reorder_only")

    assert result["detached"] == 0
    assert apply_calls == [([1], [])], (
        "stream 2 has 3 consecutive failures and would normally be "
        "detached by plan_channel, but Reorder Only must discard that"
    )


def test_reorder_only_report_includes_the_response_time_column(tmp_path, monkeypatch):
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID, response_time_ms=180)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)
    monkeypatch.setattr(
        models_access_module, "apply_channel_plan",
        lambda *_a, **_kw: {"attached": 0, "detached": 0},
    )

    result = run_pipeline({"settings": {}}, mode="reorder_only")

    with pathlib.Path(result["report"]).open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["response_time_ms"] == "180"


@pytest.mark.parametrize("mode", ["reorder_only", "probe_only"])
def test_non_run_modes_never_index_the_pool_or_match_by_name(
    mode, tmp_path, monkeypatch
):
    """Finding 2: reorder_only/probe_only must not discover new streams.

    Also covers probe_only never calling apply_channel_plan: the spy below
    records zero calls for that mode.
    """
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID)  # fresh: probe_only won't re-probe it
    _patch_common(monkeypatch, tmp_path, channel, attached, state)

    def _forbidden(*_args, **_kwargs):
        raise AssertionError(f"must not run in {mode} mode")

    monkeypatch.setattr(pipeline_module, "iter_pool", _forbidden)
    monkeypatch.setattr(pipeline_module, "find_matches", _forbidden)

    apply_calls = []

    def fake_apply(resolved, ch, ordered, detach, dry_run):  # noqa: ARG001
        apply_calls.append((ordered, detach))
        return {"attached": 0, "detached": 0}

    monkeypatch.setattr(models_access_module, "apply_channel_plan", fake_apply)

    result = run_pipeline({"settings": {}}, mode=mode)

    assert result["status"] == "ok"
    if mode == "reorder_only":
        assert apply_calls == [([1], [])]
    else:
        assert apply_calls == [], "probe_only must never call apply_channel_plan"


# --- Run-ending status: completed / canceled / interrupted -----------------


def _stub_probe_one(monkeypatch):
    """Skip the real ffprobe subprocess.

    These tests care about run-ending status, not probe mechanics (already
    covered by test_probing.py).
    """
    monkeypatch.setattr(
        probing_module.Prober, "probe_one",
        lambda self, provider_id, url: ProbeResult(VALID, {}, "ok"),  # noqa: ARG005
    )


def test_run_pipeline_reports_canceled_when_the_cancel_flag_is_set(
    tmp_path, monkeypatch
):
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]  # never probed -> not fresh
    state = _make_state(tmp_path)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)
    _stub_probe_one(monkeypatch)
    monkeypatch.setattr(pipeline_module, "cancel_requested", lambda: True)

    result = run_pipeline({"settings": {}}, mode="probe_only")

    assert result["status"] == "canceled"
    assert result["probed"] == 0, "canceled before the first probe was spent"


def test_cancel_mid_run_stops_writes_for_remaining_channels(tmp_path, monkeypatch):
    """Blocker regression: Stop must halt attach/detach/reorder, not just probing.

    Two channels; cancel_requested() flips True only once channel 1's
    apply_channel_plan has run, simulating Stop pressed mid-run. Channel 2
    must never reach apply_channel_plan.
    """
    channel_a = types.SimpleNamespace(name="RAI 1")
    channel_b = types.SimpleNamespace(name="RAI 2")
    attached = {
        "RAI 1": [row(1, "IT: RAI 1 HD", "A")],
        "RAI 2": [row(2, "IT: RAI 2 HD", "A")],
    }
    state = _make_state(tmp_path)
    state.record(1, attached["RAI 1"][0].url, VALID)
    state.record(2, attached["RAI 2"][0].url, VALID)

    monkeypatch.setattr(
        pipeline_module, "select_channels",
        lambda *_a, **_kw: [channel_a, channel_b],
    )
    monkeypatch.setattr(
        pipeline_module, "iter_attached_rows",
        lambda _resolved, ch, _settings: iter(attached[ch.name]),
    )
    monkeypatch.setattr(
        pipeline_module.State, "load", staticmethod(lambda *_a, **_kw: state)
    )
    monkeypatch.setattr(
        pipeline_module, "report_path", lambda mode: tmp_path / f"{mode}.csv"
    )
    monkeypatch.setattr(models_access_module, "resolve_models", object)
    monkeypatch.setattr(pipeline_module, "_close_old_connections", lambda: None)
    monkeypatch.setattr(pipeline_module, "_close_connection", lambda: None)

    apply_calls = []
    canceled = {"flag": False}

    def fake_apply(resolved, ch, ordered, detach, dry_run):  # noqa: ARG001
        apply_calls.append(ch.name)
        canceled["flag"] = True  # Stop pressed right after channel 1's write
        return {"attached": 0, "detached": 0}

    monkeypatch.setattr(models_access_module, "apply_channel_plan", fake_apply)
    monkeypatch.setattr(pipeline_module, "cancel_requested", lambda: canceled["flag"])

    result = run_pipeline({"settings": {}}, mode="reorder_only")

    assert apply_calls == ["RAI 1"], (
        "Stop pressed after channel 1 must skip channel 2's destructive writes"
    )
    assert result["status"] == "canceled"


def test_run_pipeline_reports_interrupted_when_a_budget_is_exhausted(
    tmp_path, monkeypatch
):
    """Covers both budgets: probe and wall-clock.

    Both count as INTERRUPTED, not CANCELED (nobody asked for this one to
    stop) and not a clean finish.
    """
    channel = types.SimpleNamespace(name="RAI 1")
    # Two never-probed candidates against a budget of 1: the first is spent,
    # the second is what trips budget.allow() into False.
    attached = [row(1, "IT: RAI 1 HD", "A"), row(2, "IT: Rai 1 4K", "B")]
    state = _make_state(tmp_path)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)
    _stub_probe_one(monkeypatch)
    monkeypatch.setattr(
        pipeline_module, "load_settings",
        lambda _c: {**load_settings({"settings": {}}), "max_probes_per_run": 1},
    )

    result = run_pipeline({"settings": {}}, mode="probe_only")

    assert result["status"] == "interrupted"
    assert result["probed"] == 1


def test_run_pipeline_reports_ok_on_a_normal_finish(tmp_path, monkeypatch):
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID)  # fresh: nothing left to probe
    _patch_common(monkeypatch, tmp_path, channel, attached, state)

    result = run_pipeline({"settings": {}}, mode="probe_only")

    assert result["status"] == "ok"


def test_run_pipeline_finally_populates_state_meta(tmp_path, monkeypatch):
    """run_pipeline's finally block must populate state.meta for Show Status.

    The finally block updates last_run, last_mode, degraded_providers, and
    budget_stop. Show Status depends on this metadata being present even
    when the run is canceled or interrupted.
    """
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)
    _stub_probe_one(monkeypatch)
    acquire_lock("probe_only")

    # Normal finish
    run_pipeline({"settings": {}}, mode="probe_only")
    reloaded = State.load(tmp_path / "state.json")
    assert reloaded.meta["last_mode"] == "probe_only"
    assert reloaded.meta["last_run"] > 0
    assert reloaded.meta["degraded_providers"] == []
    assert reloaded.meta["budget_stop"] is None

    # Canceled run
    state2 = _make_state(tmp_path / "state2.json")
    state2.record(1, attached[0].url, VALID)
    acquire_lock("probe_only")
    monkeypatch.setattr(
        pipeline_module.State, "load", staticmethod(lambda *_a, **_kw: state2)
    )
    monkeypatch.setattr(pipeline_module, "cancel_requested", lambda: True)

    run_pipeline({"settings": {}}, mode="probe_only")
    reloaded2 = State.load(tmp_path / "state2.json")
    assert reloaded2.meta["last_mode"] == "probe_only"
    assert reloaded2.meta["budget_stop"] == "canceled by user"


# --- Live progress (Show Status) --------------------------------------------


def test_run_pipeline_publishes_per_channel_progress_to_the_lock_file(
    tmp_path, monkeypatch
):
    """Show Status reads this back to build its "channel N of M" message.

    update_progress() only writes when it still owns the lock, so the test
    has to acquire it first, the same way start() would before backgrounding
    the real run.
    """
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID)  # fresh: nothing left to probe
    _patch_common(monkeypatch, tmp_path, channel, attached, state)
    acquire_lock("probe_only")

    run_pipeline({"settings": {}}, mode="probe_only")

    progress = lock_status()["progress"]
    assert progress["channel_index"] == 1
    assert progress["channels_total"] == 1
    assert progress["channel_name"] == "RAI 1"


def test_run_pipeline_publishes_per_stream_progress_while_probing(
    tmp_path, monkeypatch
):
    """The user-visible "stream N of M" figure, tracked one probe at a time.

    Two never-probed candidates give an exact denominator (2) to check
    against, and the final published progress must reflect the last one
    actually probed - not some earlier per-channel snapshot.
    """
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A"), row(2, "IT: Rai 1 4K", "B")]
    state = _make_state(tmp_path)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)
    _stub_probe_one(monkeypatch)
    acquire_lock("probe_only")

    run_pipeline({"settings": {}}, mode="probe_only")

    progress = lock_status()["progress"]
    assert progress["streams_total"] == 2
    assert progress["stream_index"] == 2
    assert progress["current_stream"] in {"IT: RAI 1 HD", "IT: Rai 1 4K"}


def test_update_progress_noops_when_lock_holder_mismatches():
    """update_progress must silently no-op when it doesn't own the lock.

    This mirrors the same guard in refresh_lock/release_lock - if the lock
    has been stolen by another run, we must not resurrect the old lock with
    stale progress data.
    """
    acquire_lock("run", now=0.0)
    pipeline_module.update_progress("run", channel_name="RAI 1")
    progress = lock_status()["progress"]
    assert progress["channel_name"] == "RAI 1"

    # Simulate another run stealing the lock
    release_lock()
    acquire_lock("reorder_only", now=10.0)

    # Original run's update_progress must not overwrite the new run's lock
    pipeline_module.update_progress("run", channel_name="STALE")
    progress = lock_status()["progress"]
    assert progress.get("channel_name") != "STALE", (
        "update_progress must no-op when holder mismatches"
    )


def test_run_pipeline_counts_newly_found_valid_streams(tmp_path, monkeypatch):
    """"Found" means matched-but-not-yet-attached and confirmed VALID.

    This is independent of dry_run, which controls attaching, not discovery.
    """
    channel = types.SimpleNamespace(name="RAI 1")
    state = _make_state(tmp_path)
    _patch_common(monkeypatch, tmp_path, channel, [], state)  # nothing attached yet
    new_stream = row(9, "IT: RAI 1 HD", "A")
    monkeypatch.setattr(
        pipeline_module, "iter_pool", lambda *_a, **_kw: iter([new_stream])
    )
    monkeypatch.setattr(
        pipeline_module, "find_matches", lambda *_a, **_kw: [new_stream]
    )
    _stub_probe_one(monkeypatch)
    monkeypatch.setattr(
        models_access_module, "apply_channel_plan",
        lambda *_a, **_kw: {"attached": 1, "detached": 0},
    )

    result = run_pipeline({"settings": {}}, mode="run")

    assert result["new_found"] == 1


# --- Django DB connection cleanup (Fix 2) -----------------------------------
#
# No live Django/DB is available offline, so these tests monkeypatch the
# seam functions themselves (following the models_access_save pattern
# already used above) and assert they are actually invoked, rather than
# integration-testing against a real connection.


def test_run_pipeline_closes_old_django_connections_before_touching_the_orm(
    tmp_path, monkeypatch
):
    """run_pipeline closes stale DB connections before any ORM access.

    The test name promises ordering: _close_old_connections must run *before*
    models_access.resolve_models (the first ORM touch). A regression that
    moves _close_old_connections() to the end of run_pipeline would keep the
    old test green (it only asserted the function was called at all).
    """
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)
    monkeypatch.setattr(
        models_access_module, "apply_channel_plan",
        lambda *_a, **_kw: {"attached": 0, "detached": 0},
    )

    call_sequence = []

    def track_close_old():
        call_sequence.append("close_old_connections")

    def track_resolve_models():
        call_sequence.append("resolve_models")
        return object  # the stub used in _patch_common

    monkeypatch.setattr(pipeline_module, "_close_old_connections", track_close_old)
    monkeypatch.setattr(models_access_module, "resolve_models", track_resolve_models)

    run_pipeline({"settings": {}}, mode="reorder_only")

    assert call_sequence == ["close_old_connections", "resolve_models"], (
        "_close_old_connections must be called before resolve_models (first ORM access)"
    )


def test_start_inline_branch_closes_its_django_connection(tmp_path, monkeypatch):
    """Covers the non-backgrounded execution path of start()."""
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)
    monkeypatch.setattr(
        models_access_module, "apply_channel_plan",
        lambda *_a, **_kw: {"attached": 0, "detached": 0},
    )

    calls = []
    monkeypatch.setattr(pipeline_module, "_close_connection",
                        lambda: calls.append(True))

    # channel_count (1) <= INLINE_CHANNEL_LIMIT and mode == "reorder_only":
    # this takes start()'s inline branch, not the backgrounded one.
    result = pipeline_module.start({"settings": {}}, mode="reorder_only")

    assert result["status"] == "ok"
    assert calls, "the inline branch must close its connection when done"


def test_backgrounded_start_closes_its_django_connection(tmp_path, monkeypatch):
    """Covers start()'s spawn() path (the scheduler and larger manual runs)."""
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID)  # fresh: no real probing needed
    _patch_common(monkeypatch, tmp_path, channel, attached, state)

    # Run the "background" work synchronously so the assertion below doesn't
    # race a real thread.
    monkeypatch.setattr(pipeline_module, "spawn", lambda fn, *args: fn(*args))

    calls = []
    monkeypatch.setattr(pipeline_module, "_close_connection",
                        lambda: calls.append(True))

    result = pipeline_module.start({"settings": {}}, mode="probe_only")

    assert result["status"] == "started"
    assert calls, "the backgrounded run must close its connection when done"


def test_backgrounded_run_releases_the_lock_once_it_finishes(tmp_path, monkeypatch):
    """Answers "is the lock released when the plugin is done" directly.

    start()'s success path already releases via a finally: this proves it
    end to end through the real spawn()->run_pipeline()->release_lock chain
    (spawn patched to run synchronously so the test doesn't race a thread).
    """
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID)  # fresh: no real probing needed
    _patch_common(monkeypatch, tmp_path, channel, attached, state)
    monkeypatch.setattr(pipeline_module, "spawn", lambda fn, *args: fn(*args))

    pipeline_module.start({"settings": {}}, mode="probe_only")

    assert lock_status()["holder"] is None


def test_backgrounded_run_releases_the_lock_even_if_run_pipeline_crashes(monkeypatch):
    """start()'s background() finally: must release the lock on a crash too.

    Otherwise one bad run permanently wedges every future run behind a lock
    nothing will ever clear.
    """
    monkeypatch.setattr(pipeline_module, "spawn", lambda fn, *args: fn(*args))
    monkeypatch.setattr(pipeline_module, "select_channels", lambda *_a, **_kw: [])
    monkeypatch.setattr(models_access_module, "resolve_models", object)
    monkeypatch.setattr(pipeline_module, "_close_connection", lambda: None)

    def _boom(*_a, **_kw):
        raise RuntimeError("simulated crash mid-run")

    monkeypatch.setattr(pipeline_module, "run_pipeline", _boom)

    pipeline_module.start({"settings": {}}, mode="run")

    assert lock_status()["holder"] is None


# --- Aborted-provider budget waste (Fix 4) ----------------------------------


def test_probe_candidates_skips_candidates_on_an_already_aborted_provider():
    """A provider that aborted early must not keep burning probe budget.

    prober.probe_one() already no-ops for an aborted provider, but the old
    loop still called budget.spend() for that no-op - letting one bad
    provider with many candidates starve healthy providers of their share.
    """
    settings = load_settings({"settings": {}})
    state = State(path=pathlib.Path("/nonexistent/does-not-matter.json"))
    budget = Budget(max_probes=10, max_minutes=60, now_fn=lambda: 0.0)
    candidate = row(1, "IT: RAI 1 HD", "A")

    class AbortedProber:
        aborted_providers = {"A"}

        def probe_one(self, *_args, **_kwargs):
            raise AssertionError(
                "must not probe a candidate on an already-aborted provider"
            )

    log = types.SimpleNamespace(
        info=lambda *_a, **_kw: None, debug=lambda *_a, **_kw: None
    )

    probed = pipeline_module._probe_candidates(
        [candidate], state, settings, AbortedProber(), budget,
        resolved=None, log=log,
    )

    assert probed == 0
    assert budget.probes == 0, "an aborted provider's candidates must not spend budget"


def test_select_probe_batch_mid_batch_provider_abort_still_probes_selected(
    tmp_path
):
    """Mid-batch provider abort: already-selected candidates still get probed.

    _select_probe_batch selects the entire batch up front, before any probe
    runs. If a provider crosses the abort threshold during the batch, its
    already-selected remaining candidates are still dispatched and charged.
    Only subsequent calls to _select_probe_batch will skip them.
    """
    settings = load_settings({"settings": {}})
    budget = Budget(max_probes=10, max_minutes=60, now_fn=lambda: 0.0)

    # 3 candidates from provider A, none fresh
    candidates = [
        row(1, "IT: RAI 1 HD", 1),
        row(2, "IT: RAI 2 HD", 1),
        row(3, "IT: RAI 3 HD", 1),
    ]

    class ProberMidAbort:
        aborted_providers = set()

        def probe_one(self, provider_id, url):
            if provider_id == 1 and "2" in url:
                # Simulate provider 1 aborting on the 2nd probe
                self.aborted_providers.add(1)
            return ProbeResult(VALID, {}, "ok")

    log = types.SimpleNamespace(
        info=lambda *_a, **_kw: None, debug=lambda *_a, **_kw: None
    )

    # First call: all 3 selected (provider not aborted yet)
    state1 = State(path=tmp_path / "state1.json")
    prober = ProberMidAbort()
    to_probe_1 = pipeline_module._select_probe_batch(
        candidates, state1, settings, prober, budget, log
    )
    assert len(to_probe_1) == 3
    assert budget.probes == 3

    # Second call: provider A now aborted, should skip all
    state2 = State(path=tmp_path / "state2.json")
    prober2 = ProberMidAbort()
    prober2.aborted_providers = {1}  # already aborted (provider_id is int)
    to_probe_2 = pipeline_module._select_probe_batch(
        candidates, state2, settings, prober2, budget, log
    )
    assert len(to_probe_2) == 0
    assert budget.probes == 3  # no additional budget spent


def test_probe_candidates_logs_the_offending_stream_when_a_worker_raises(monkeypatch):
    """Regression: a probe-worker exception logged with no stream identity.

    `futures` was a plain list with no mapping back to its originating
    candidate, so with concurrent probes there was no way to tell which
    stream failed from the log line alone.
    """
    settings = load_settings({"settings": {}})
    state = State(path=pathlib.Path("/nonexistent/does-not-matter.json"))
    budget = Budget(max_probes=10, max_minutes=60, now_fn=lambda: 0.0)
    candidate = row(7, "IT: RAI 1 HD", "Zeta")

    class RaisingProber:
        aborted_providers = set()

        def probe_one(self, *_args, **_kwargs):
            raise RuntimeError("simulated ffprobe crash")

    monkeypatch.setattr(pipeline_module, "_models_access_save", lambda *_a, **_kw: None)
    monkeypatch.setattr(pipeline_module, "_notify", lambda *_a, **_kw: None)

    captured = {}

    class _Log:
        def info(self, *_a, **_kw):
            pass

        def exception(self, msg, *args, **_kw):
            captured["call"] = (msg, args)

    probed = pipeline_module._probe_candidates(
        [candidate], state, settings, RaisingProber(), budget,
        resolved=None, log=_Log(),
    )

    assert probed == 0, "a raised probe must not count toward the total"
    assert "call" in captured, "the worker exception must be logged"
    _msg, args = captured["call"]
    assert 7 in args, "the offending stream_id must be in the log args"
    assert "IT: RAI 1 HD" in args, "the offending stream name must be in the log args"
    assert "Zeta" in args, "the offending provider must be in the log args"


def test_probe_candidates_probes_different_providers_concurrently(monkeypatch):
    """Different providers must be probed in parallel (CLAUDE.md §7).

    Prober.probe_one already enforces the per-account/global caps (see
    test_probing.py); this proves _probe_candidates actually dispatches
    concurrently instead of one candidate at a time.
    """
    settings = load_settings({"settings": {"global_concurrency": 4}})
    state = State(path=pathlib.Path("/nonexistent/does-not-matter.json"))
    budget = Budget(max_probes=10, max_minutes=60, now_fn=lambda: 0.0)

    live = {"n": 0}
    peak = {"n": 0}
    lock = threading.Lock()
    release = threading.Event()
    started = threading.Event()

    class SlowProber:
        aborted_providers = set()

        def probe_one(self, _provider_id, _url):
            with lock:
                live["n"] += 1
                peak["n"] = max(peak["n"], live["n"])
                if peak["n"] >= 2:
                    started.set()
            release.wait(timeout=10)
            with lock:
                live["n"] -= 1
            return ProbeResult(VALID, {}, "ok")

    candidates = [
        row(i, f"IT: RAI {i} HD", provider)
        for i, provider in enumerate(["A", "B", "C", "D"], start=1)
    ]

    monkeypatch.setattr(pipeline_module, "_models_access_save", lambda *_a, **_kw: None)
    log = types.SimpleNamespace(info=lambda *_a, **_kw: None)

    result = {}

    def run():
        result["probed"] = pipeline_module._probe_candidates(
            candidates, state, settings, SlowProber(), budget,
            resolved=None, log=log,
        )

    thread = threading.Thread(target=run)
    thread.start()
    assert started.wait(timeout=10), "the pool must reach concurrent execution"
    release.set()
    thread.join(timeout=10)

    assert result["probed"] == 4
    assert peak["n"] > 1, (
        "candidates on different providers must be probed concurrently"
    )


def test_probe_candidates_stops_launching_new_probes_once_stopped_mid_batch(
    monkeypatch,
):
    """Stop must not wait for a whole channel's batch to finish.

    With global_concurrency=2 and 4 candidates, 2 start immediately and 2
    sit queued behind them. Once Stop is pressed while the first 2 are
    in-flight, those 2 must still be allowed to finish (there is no way to
    kill a subprocess mid-probe), but the still-queued 2 must never launch
    at all - "wait for the current probe, then stop."
    """
    settings = load_settings({"settings": {"global_concurrency": 2}})
    state = State(path=pathlib.Path("/nonexistent/does-not-matter.json"))
    budget = Budget(max_probes=10, max_minutes=60, now_fn=lambda: 0.0)

    started = threading.Event()
    release = threading.Event()
    live = {"n": 0}
    calls = []
    lock = threading.Lock()

    class SlowProber:
        aborted_providers = set()

        def probe_one(self, _provider_id, url):
            with lock:
                calls.append(url)
                live["n"] += 1
                if live["n"] == 2:
                    started.set()
            release.wait(timeout=10)
            return ProbeResult(VALID, {}, "ok")

    candidates = [
        row(i, f"IT: RAI {i} HD", provider)
        for i, provider in enumerate(["A", "B", "C", "D"], start=1)
    ]

    monkeypatch.setattr(pipeline_module, "_models_access_save", lambda *_a, **_kw: None)
    canceled = {"flag": False}
    monkeypatch.setattr(pipeline_module, "cancel_requested", lambda: canceled["flag"])
    log = types.SimpleNamespace(info=lambda *_a, **_kw: None)

    result = {}

    def run():
        result["probed"] = pipeline_module._probe_candidates(
            candidates, state, settings, SlowProber(), budget,
            resolved=None, log=log,
        )

    thread = threading.Thread(target=run)
    thread.start()
    assert started.wait(timeout=10), "the first 2 (global_concurrency=2) must start"
    canceled["flag"] = True  # Stop pressed while 2 are in flight, 2 still queued
    release.set()
    thread.join(timeout=10)

    assert result["probed"] == 2, "only the 2 already in flight should complete"
    assert len(calls) == 2, "the 2 queued-but-not-started candidates must never probe"


def test_probe_candidates_stop_shortcircuit_does_not_mark_stream_fresh(
    tmp_path, monkeypatch
):
    """Stop short-circuit must not mark stream as fresh.

    When Stop is pressed before a probe starts, verdict=None must not be
    recorded in state, and the stream must NOT be considered fresh.

    This tests the guard in _handle_probe_result (pipeline.py:908) that
    returns False for verdict=None, and that state.record is never called
    with verdict=None. If it were, is_fresh would incorrectly return True
    (since it only short-circuits on INCONCLUSIVE, not None), causing the
    next run to skip re-probing that stream entirely.
    """
    settings = load_settings({"settings": {"global_concurrency": 2}})
    state = State(path=tmp_path / "state.json")
    budget = Budget(max_probes=10, max_minutes=60, now_fn=lambda: 0.0)

    started = threading.Event()
    release = threading.Event()
    live = {"n": 0}
    lock = threading.Lock()

    class SlowProber:
        aborted_providers = set()

        def probe_one(self, _provider_id, url):
            with lock:
                live["n"] += 1
                if live["n"] == 2:
                    started.set()
            release.wait(timeout=10)
            return ProbeResult(VALID, {}, "ok")

    candidates = [
        row(i, f"IT: RAI {i} HD", provider)
        for i, provider in enumerate(["A", "B", "C", "D"], start=1)
    ]

    monkeypatch.setattr(pipeline_module, "_models_access_save", lambda *_a, **_kw: None)
    canceled = {"flag": False}
    monkeypatch.setattr(pipeline_module, "cancel_requested", lambda: canceled["flag"])
    log = types.SimpleNamespace(info=lambda *_a, **_kw: None)

    result = {}

    def run():
        result["probed"] = pipeline_module._probe_candidates(
            candidates, state, settings, SlowProber(), budget,
            resolved=None, log=log,
        )

    thread = threading.Thread(target=run)
    thread.start()
    assert started.wait(timeout=10), "the first 2 (global_concurrency=2) must start"
    canceled["flag"] = True  # Stop pressed while 2 are in flight, 2 still queued
    release.set()
    thread.join(timeout=10)

    assert result["probed"] == 2, "only the 2 already in flight should complete"

    # The 2 queued candidates (C and D) had their work() short-circuit
    # with verdict=None. They must NOT be recorded in state.
    assert "3" not in state.streams, "stream 3 (C) must not have a state entry"
    assert "4" not in state.streams, "stream 4 (D) must not have a state entry"

    # And they must NOT be considered fresh - is_fresh must return False
    # so the next run will re-probe them.
    assert state.is_fresh(3, "http://example.com/C", 24) is False
    assert state.is_fresh(4, "http://example.com/D", 24) is False


# --- Lock heartbeat + periodic state.save() cadence (Fixes 1 & 3) ----------


def test_probe_candidates_refreshes_the_lock_and_saves_state_every_25_probes(
    tmp_path, monkeypatch
):
    settings = load_settings({"settings": {}})
    state = State(path=tmp_path / "state.json")
    budget = Budget(max_probes=100, max_minutes=60, now_fn=lambda: 0.0)
    candidates = [row(i, f"IT: RAI {i} HD", "A") for i in range(1, 26)]  # 25

    valid_stats = {
        "video_codec": "hevc", "resolution": "1920x1080", "video_bitrate": 5000,
        "source_fps": 25, "audio_codec": "aac", "audio_channels": 2,
    }

    class StubProber:
        aborted_providers = set()

        def probe_one(self, *_args, **_kwargs):
            return ProbeResult(VALID, valid_stats, "ok")

    saves = []
    refreshes = []
    monkeypatch.setattr(state, "save", lambda: saves.append(True))
    monkeypatch.setattr(
        pipeline_module, "refresh_lock", lambda *_a, **_kw: refreshes.append(True)
    )
    monkeypatch.setattr(pipeline_module, "_models_access_save", lambda *_a, **_kw: None)
    monkeypatch.setattr(pipeline_module, "_notify", lambda *_a, **_kw: None)

    log = types.SimpleNamespace(info=lambda *_a, **_kw: None)

    probed = pipeline_module._probe_candidates(
        candidates, state, settings, StubProber(), budget, resolved=None, log=log,
    )

    assert probed == 25
    assert len(saves) == 1, "state.save() should fire once at the 25th probe"
    assert len(refreshes) == 1, "refresh_lock() should fire alongside it"


def test_probe_candidates_accumulates_the_heartbeat_cadence_across_channels(
    tmp_path, monkeypatch
):
    """The 25-probe cadence must accumulate across the whole run.

    It must not reset every time `_probe_candidates` is called for a new
    channel. run_pipeline calls _probe_candidates once per channel, and
    real deployments have far fewer than 25 candidates per channel
    (CLAUDE.md's own cost model: ~8 candidates/channel;
    max_streams_per_channel defaults to 10) - so a per-call-local counter
    would never reach 25, and the heartbeat/save would never fire during a
    real multi-channel run.
    """
    settings = load_settings({"settings": {}})
    state = State(path=tmp_path / "state.json")
    budget = Budget(max_probes=100, max_minutes=60, now_fn=lambda: 0.0)

    valid_stats = {
        "video_codec": "hevc", "resolution": "1920x1080", "video_bitrate": 5000,
        "source_fps": 25, "audio_codec": "aac", "audio_channels": 2,
    }

    class StubProber:
        aborted_providers = set()

        def probe_one(self, *_args, **_kwargs):
            return ProbeResult(VALID, valid_stats, "ok")

    saves = []
    refreshes = []
    monkeypatch.setattr(state, "save", lambda: saves.append(True))
    monkeypatch.setattr(
        pipeline_module, "refresh_lock", lambda *_a, **_kw: refreshes.append(True)
    )
    monkeypatch.setattr(pipeline_module, "_models_access_save", lambda *_a, **_kw: None)
    monkeypatch.setattr(pipeline_module, "_notify", lambda *_a, **_kw: None)

    log = types.SimpleNamespace(info=lambda *_a, **_kw: None)

    # Channel 1: 20 candidates - under 25, must not trigger the cadence alone.
    channel_1 = [row(i, f"IT: RAI {i} HD", "A") for i in range(1, 21)]
    probed_1 = pipeline_module._probe_candidates(
        channel_1, state, settings, StubProber(), budget,
        resolved=None, log=log, probed_so_far=0,
    )
    assert probed_1 == 20
    assert len(saves) == 0, "20 probes in one channel must not fire the cadence"

    # Channel 2: 10 more candidates. The run-wide total crosses 25 on this
    # channel's 5th probe (20 + 5 = 25), even though this channel alone
    # never reaches 25 candidates.
    channel_2 = [row(i, f"IT: RAI {i} HD", "A") for i in range(21, 31)]
    probed_2 = pipeline_module._probe_candidates(
        channel_2, state, settings, StubProber(), budget,
        resolved=None, log=log, probed_so_far=probed_1,
    )
    assert probed_2 == 10
    assert len(saves) == 1, "the run-wide total crossing 25 must fire the cadence"
    assert len(refreshes) == 1


def test_probe_candidates_records_the_measured_response_time(tmp_path, monkeypatch):
    settings = load_settings({"settings": {}})
    state = State(path=tmp_path / "state.json")
    budget = Budget(max_probes=10, max_minutes=60, now_fn=lambda: 0.0)
    candidates = [row(1, "IT: RAI 1 HD", "A")]

    valid_stats = {
        "video_codec": "hevc", "resolution": "1920x1080", "video_bitrate": 5000,
        "source_fps": 25, "audio_codec": "aac", "audio_channels": 2,
    }

    class StubProber:
        aborted_providers = set()

        def probe_one(self, *_args, **_kwargs):
            return ProbeResult(VALID, valid_stats, "ok", 275)

    monkeypatch.setattr(pipeline_module, "_models_access_save", lambda *_a, **_kw: None)
    log = types.SimpleNamespace(info=lambda *_a, **_kw: None)

    pipeline_module._probe_candidates(
        candidates, state, settings, StubProber(), budget, resolved=None, log=log,
    )

    assert state.response_time_ms(1) == 275


def test_blank_detected_stream_does_not_record_a_response_time(tmp_path, monkeypatch):
    settings = load_settings({"settings": {"blank_detect": True}})
    state = State(path=tmp_path / "state.json")
    budget = Budget(max_probes=10, max_minutes=60, now_fn=lambda: 0.0)
    candidates = [row(1, "IT: RAI 1 HD", "A")]

    valid_stats = {
        "video_codec": "hevc", "resolution": "1920x1080", "video_bitrate": 5000,
        "source_fps": 25, "audio_codec": "aac", "audio_channels": 2,
    }

    class StubProber:
        aborted_providers = set()

        def probe_one(self, *_args, **_kwargs):
            return ProbeResult(VALID, valid_stats, "ok", 275)

    monkeypatch.setattr(pipeline_module, "_models_access_save", lambda *_a, **_kw: None)
    monkeypatch.setattr(probing_module, "is_blank", lambda *_a, **_kw: True)
    log = types.SimpleNamespace(info=lambda *_a, **_kw: None)

    pipeline_module._probe_candidates(
        candidates, state, settings, StubProber(), budget, resolved=None, log=log,
    )

    assert state.response_time_ms(1) is None


def test_handle_probe_result_calls_models_access_save_on_valid(tmp_path, monkeypatch):
    """_handle_probe_result must call _models_access_save for VALID probes.

    The save path could be deleted and every test would pass (all sites stub
    it to no-op). This test ensures the call actually happens.
    """
    state = State(path=tmp_path / "state.json")
    candidate = row(1, "IT: RAI 1 HD", "A")

    valid_stats = {
        "video_codec": "hevc", "resolution": "1920x1080", "video_bitrate": 5000,
        "source_fps": 25, "audio_codec": "aac", "audio_channels": 2,
    }

    save_calls = []

    def fake_save(_resolved, stream_id, stats):
        save_calls.append((stream_id, stats))

    monkeypatch.setattr(pipeline_module, "_models_access_save", fake_save)

    # Call _handle_probe_result directly with a VALID verdict
    result = pipeline_module._handle_probe_result(
        candidate, VALID, valid_stats, 275, "ok",
        resolved=None, state=state,
        log=types.SimpleNamespace(info=lambda *_a, **_kw: None),
        progress_cb=None, probed_before=0, mode="probe_only",
    )

    assert result is True
    assert len(save_calls) == 1
    assert save_calls[0][0] == 1
    assert save_calls[0][1] == valid_stats


def test_handle_probe_result_skips_save_on_invalid(tmp_path, monkeypatch):
    """_handle_probe_result must NOT call _models_access_save for INVALID."""
    state = State(path=tmp_path / "state.json")
    candidate = row(1, "IT: RAI 1 HD", "A")

    save_calls = []
    monkeypatch.setattr(
        pipeline_module, "_models_access_save",
        lambda *a, **_kw: save_calls.append(a)
    )

    result = pipeline_module._handle_probe_result(
        candidate, INVALID, {}, None, "dead",
        resolved=None, state=state,
        log=types.SimpleNamespace(info=lambda *_a, **_kw: None),
        progress_cb=None, probed_before=0, mode="probe_only",
    )

    assert result is True
    assert save_calls == [], "INVALID must not trigger save"


# --- Gevent detection (Task 2) --------------------------------------------------


def test_gevent_patched_is_false_without_gevent_installed():
    """Both spawn() and execution_model() must use one shared gevent check.

    The dev/test environment has no gevent installed (only the live
    Dispatcharr container is guaranteed to, per CLAUDE.md §4), so both
    functions must fall back to the plain-thread path through the shared
    _gevent_patched() helper.
    """
    assert pipeline_module._gevent_patched() is False
    assert pipeline_module.execution_model() == "daemon thread"


def test_notify_leaves_a_trace_when_the_websocket_update_fails(caplog):
    """Regression: a bare `pass` left a notification failure with no trace.

    A recurring best-effort notification failure left zero trace. It must at
    least log at debug so a recurring send_websocket_update failure is
    diagnosable from the log.
    """
    import logging

    with caplog.at_level(logging.DEBUG, logger="failoverr"):
        pipeline_module._notify({"type": "failoverr"})

    assert any(
        "progress notification failed" in r.message for r in caplog.records
    ), caplog.records
