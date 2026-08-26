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
    find_matches,
    load_settings,
    lock_status,
    plan_channel,
    release_lock,
    report_path,
    run_pipeline,
    update_progress,
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
    found = find_matches(normalize("RAI 1"), index)
    assert {r.stream_id for r in found} == {1, 2, 3}


def test_strict_matching_excludes_rai_2_and_rai_sport():
    index = build_index(POOL)
    found = {r.stream_id for r in find_matches(normalize("RAI 1"), index)}
    assert 4 not in found and 5 not in found and 6 not in found


def test_channel_with_no_matches_returns_empty():
    index = build_index(POOL)
    assert find_matches(normalize("BBC One"), index) == []


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
        max_streams=10, codec_priority=("hevc",),
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
    assert settings["removal_failure_threshold"] == 3


def test_load_settings_defaults_the_ranking_toggle_to_true():
    settings = load_settings({"settings": {}})
    assert settings["rank_by_bitrate"] is True


def test_load_settings_coerces_the_ranking_toggle():
    settings = load_settings({"settings": {"rank_by_bitrate": "false"}})
    assert settings["rank_by_bitrate"] is False


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
    release_lock()
    yield
    release_lock()


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
    acquire_lock("run", now=0.0)
    assert lock_status()["holder"] == "run"


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
    than racing - the same active/stale distinction acquire_lock's
    self-heal relies on.
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


def test_channel_candidates_reads_the_link_table_once_per_channel(
    tmp_path, monkeypatch,
):
    """Regression: the attached-link table was queried more than once per channel.

    _channel_candidates used to call both attached_rows() and
    iter_attached_rows() - two queries for data available in one.
    attached_rows is gone now and attached_ids is derived from
    iter_attached_rows' StreamRows, so the link table is read exactly once
    per channel per run.
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

    assert calls["n"] == 2, (
        f"link table read {calls['n']} times for 2 channels; expected 2 "
        "(one per channel)"
    )


def test_update_progress_heartbeat_prevents_a_steal_at_the_original_ttl_deadline():
    """Finding: a long run must keep its own lock fresh.

    Without a heartbeat, a second run could steal the lock once
    LOCK_TTL_SECONDS has passed since the ORIGINAL acquire.
    """
    acquire_lock("run", now=0.0)
    # Well before the original deadline, the run heartbeats its lock.
    update_progress(None, now=1000.0)
    # At the moment the lock would have gone stale relative to the original
    # acquisition (now=0.0), it must still be held - the heartbeat worked.
    assert acquire_lock("preview", now=LOCK_TTL_SECONDS + 1) is False
    # It does go stale relative to the refreshed timestamp, once enough
    # time has passed since THAT.
    assert acquire_lock("preview", now=1000.0 + LOCK_TTL_SECONDS + 1) is True


def test_release_lock_no_ops_when_it_no_longer_owns_the_lock():
    """Regression: release_lock wiped any lock, not just its own.

    A run whose lock was force-cleared (release_lock()) and then
    re-acquired by a second run would still call release_lock() on its way
    out, clearing the SECOND run's lock and letting a third start - the
    two-runs-at-once hazard the lock exists to prevent. release_lock now
    checks it still owns the lock (holder matches) before clearing.
    """
    acquire_lock("run", now=0.0)
    release_lock()  # force-clear, holder=None
    acquire_lock("preview", now=10.0)
    # Run A's release_lock("run") must NOT clear B's lock.
    release_lock("run")
    assert lock_status()["holder"] == "preview"


def test_update_progress_heartbeat_no_ops_when_it_no_longer_owns_the_lock():
    """Regression: refresh_lock bumped any lock's since, not just its own.

    A stale run heartbeating after its lock was cleared and re-acquired
    would bump the NEW run's since, making the new lock look freshly held
    by the wrong run. update_progress checks it still owns the lock.
    """
    acquire_lock("run", now=0.0)
    release_lock()
    acquire_lock("preview", now=10.0)
    # Run A's heartbeat must NOT touch B's lock timestamp.
    update_progress("run", now=9999.0)
    assert lock_status()["holder"] == "preview"
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
    """The lock heartbeat fired only every 25 probes, never when nothing needs probing.

    A run whose candidates are all fresh (nothing to re-probe) never
    exercises the 25-probe cadence, and that cadence never fires between
    channels either, so such a run's lock could go stale and be
    force-cleared mid-run. The lock now refreshes at every channel
    boundary and after indexing.
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
    real_update_progress = pipeline_module.update_progress

    def counting_update_progress(*args, **kwargs):
        # Only the bare heartbeat call (no progress fields) counts here -
        # _start_channel_progress/on_probe also call update_progress, but
        # with fields, and this test is only about the plain heartbeat.
        if not kwargs:
            refreshes.append(True)
        return real_update_progress(*args, **kwargs)

    monkeypatch.setattr(pipeline_module, "update_progress", counting_update_progress)
    monkeypatch.setattr(pipeline_module, "iter_pool", lambda *_a, **_kw: iter([]))
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

    run_pipeline({"settings": {}}, mode="run")

    # 1 after-index heartbeat + 2 per-channel heartbeats (every candidate is
    # fresh, so the 25-probe cadence contributes nothing).
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
    # index-building (build_index(iter_pool(...))) always runs now, so give
    # it a harmless empty pool - these tests only exercise already-attached
    # candidates, never newly matched ones.
    monkeypatch.setattr(pipeline_module, "iter_pool", lambda *_a, **_kw: iter([]))
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
    # apply_channel_plan is always reached now too (no more probe_only early
    # exit); individual tests override this when they need to inspect calls.
    monkeypatch.setattr(
        models_access_module, "apply_channel_plan",
        lambda *_a, **_kw: {"attached": 0, "detached": 0},
    )


def test_run_pipeline_report_includes_the_response_time_column(tmp_path, monkeypatch):
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID, response_time_ms=180)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)

    result = run_pipeline({"settings": {}}, mode="run")

    with pathlib.Path(result["report"]).open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["response_time_ms"] == "180"


def test_run_pipeline_reports_a_valid_candidate_truncated_by_the_streams_cap(
    tmp_path, monkeypatch,
):
    """A valid, matched candidate that loses the max_streams_per_channel cutoff.

    It must still show up in the report - otherwise it vanishes with no
    trace, indistinguishable from a matching bug (the exact confusion a new
    provider's streams caused: probed valid in the log, absent from the CSV).
    """
    channel = types.SimpleNamespace(name="RAI 1")
    winner = row(1, "IT: RAI 1 HD", 1, height=1080)
    loser = row(2, "IT: Rai 1 4K", 2, height=720)
    state = _make_state(tmp_path)
    state.record(1, winner.url, VALID, response_time_ms=100)
    state.record(2, loser.url, VALID, response_time_ms=100)

    monkeypatch.setattr(
        pipeline_module, "select_channels", lambda *_a, **_kw: [channel]
    )
    monkeypatch.setattr(
        pipeline_module, "iter_attached_rows", lambda *_a, **_kw: iter([])
    )
    monkeypatch.setattr(
        pipeline_module, "iter_pool", lambda *_a, **_kw: iter([winner, loser])
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
    monkeypatch.setattr(
        models_access_module, "apply_channel_plan",
        lambda *_a, **_kw: {"attached": 1, "detached": 0},
    )

    result = run_pipeline(
        {"settings": {"max_streams_per_channel": 1}}, mode="run"
    )

    with pathlib.Path(result["report"]).open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    by_stream = {r["stream"]: r for r in rows}
    assert by_stream["IT: RAI 1 HD"]["action"] == "attach"
    assert by_stream["IT: Rai 1 4K"]["action"] == "not attached - outranked"
    assert by_stream["IT: Rai 1 4K"]["verdict"] == "valid"


# --- Broken channel marker ---------------------------------------------


def test_apply_broken_suffix_appends_once():
    from failoverr.pipeline import apply_broken_suffix

    once = apply_broken_suffix("RAI 1", " [BROKEN]", is_broken=True)
    assert once == "RAI 1 [BROKEN]"
    assert apply_broken_suffix(once, " [BROKEN]", is_broken=True) == once


def test_apply_broken_suffix_strips_once_healthy():
    from failoverr.pipeline import apply_broken_suffix

    healed = apply_broken_suffix("RAI 1 [BROKEN]", " [BROKEN]", is_broken=False)
    assert healed == "RAI 1"
    assert apply_broken_suffix(healed, " [BROKEN]", is_broken=False) == healed


def test_apply_broken_suffix_noop_on_blank_suffix():
    from failoverr.pipeline import apply_broken_suffix

    assert apply_broken_suffix("RAI 1", "", is_broken=True) == "RAI 1"


def test_channel_broken_state_prefers_valid_over_invalid(tmp_path):
    from failoverr.pipeline import _channel_broken_state

    state = _make_state(tmp_path)
    state.record(1, "http://a", VALID)
    state.record(2, "http://b", INVALID)
    candidates = [row(1, "A", "p"), row(2, "B", "p")]
    assert _channel_broken_state(candidates, state) is False


def test_channel_broken_state_true_when_only_invalid(tmp_path):
    from failoverr.pipeline import _channel_broken_state

    state = _make_state(tmp_path)
    state.record(1, "http://a", INVALID)
    assert _channel_broken_state([row(1, "A", "p")], state) is True


def test_channel_broken_state_none_when_nothing_conclusive(tmp_path):
    """Never-probed or inconclusive-only candidates leave the marker alone."""
    from failoverr.pipeline import _channel_broken_state

    state = _make_state(tmp_path)
    state.record(1, "http://a", INCONCLUSIVE)
    candidates = [row(1, "A", "p"), row(2, "B", "p")]  # 2 never probed
    assert _channel_broken_state(candidates, state) is None


def test_channel_broken_state_none_when_invalid_mixed_with_unresolved(tmp_path):
    """One confirmed-invalid stream is not enough - every candidate must be.

    Regression: an earlier version returned True as soon as any candidate
    was INVALID, marking a channel broken while another of its candidates
    was still genuinely unresolved (never probed) - exactly the "not yet
    probed" case the marker is supposed to never act on.
    """
    from failoverr.pipeline import _channel_broken_state

    state = _make_state(tmp_path)
    state.record(1, "http://a", INVALID)
    candidates = [row(1, "A", "p"), row(2, "B", "p")]  # 2 never probed
    assert _channel_broken_state(candidates, state) is None


class _FakeQuerySet(list):
    def filter(self, **kwargs):
        if "name__in" in kwargs:
            names = set(kwargs["name__in"])
            return _FakeQuerySet(c for c in self if c.name in names)
        raise NotImplementedError(kwargs)


class _FakeChannelModel:
    def __init__(self, items):
        self.objects = types.SimpleNamespace(all=lambda: _FakeQuerySet(items))


def test_select_channels_matches_a_channel_after_the_marker_renamed_it():
    """Regression: channel_names is an exact match.

    A rename this same feature performs would otherwise drop the channel
    out of its own configured filter forever - defeating the marker's
    self-clearing.
    """
    from failoverr.pipeline import select_channels

    channel = types.SimpleNamespace(id=1, name="RAI 1 [BROKEN]")
    resolved = types.SimpleNamespace(channel_model=_FakeChannelModel([channel]))
    settings = {
        "channel_names": ["RAI 1"], "channel_group": "",
        "mark_broken_channels": True, "broken_channel_suffix": " [BROKEN]",
    }

    assert select_channels(resolved, settings) == [channel]


def test_select_channels_does_not_widen_the_filter_when_marking_is_off():
    from failoverr.pipeline import select_channels

    channel = types.SimpleNamespace(id=1, name="RAI 1 [BROKEN]")
    resolved = types.SimpleNamespace(channel_model=_FakeChannelModel([channel]))
    settings = {
        "channel_names": ["RAI 1"], "channel_group": "",
        "mark_broken_channels": False, "broken_channel_suffix": " [BROKEN]",
    }

    assert select_channels(resolved, settings) == []


def test_run_pipeline_marks_a_channel_broken_when_every_stream_is_invalid(
    tmp_path, monkeypatch,
):
    channel = types.SimpleNamespace(id=1, name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, INVALID)  # fresh: no re-probe needed
    _patch_common(monkeypatch, tmp_path, channel, attached, state)

    renamed = []
    monkeypatch.setattr(
        models_access_module, "rename_channel",
        lambda _resolved, ch, new_name: renamed.append((ch.name, new_name)),
    )

    run_pipeline({"settings": {"dry_run": False}}, mode="run")

    assert renamed == [("RAI 1", "RAI 1 [BROKEN]")]


def test_run_pipeline_clears_the_marker_once_a_stream_is_valid_again(
    tmp_path, monkeypatch,
):
    channel = types.SimpleNamespace(id=1, name="RAI 1 [BROKEN]")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)

    renamed = []
    monkeypatch.setattr(
        models_access_module, "rename_channel",
        lambda _resolved, ch, new_name: renamed.append((ch.name, new_name)),
    )

    run_pipeline({"settings": {"dry_run": False}}, mode="run")

    assert renamed == [("RAI 1 [BROKEN]", "RAI 1")]


def test_run_pipeline_does_not_mark_broken_when_the_setting_is_off(
    tmp_path, monkeypatch,
):
    channel = types.SimpleNamespace(id=1, name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, INVALID)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)

    renamed = []
    monkeypatch.setattr(
        models_access_module, "rename_channel",
        lambda _resolved, ch, new_name: renamed.append((ch.name, new_name)),
    )

    run_pipeline(
        {"settings": {"dry_run": False, "mark_broken_channels": False}}, mode="run"
    )

    assert renamed == []


def test_run_pipeline_computes_but_does_not_save_the_marker_under_dry_run(
    tmp_path, monkeypatch,
):
    channel = types.SimpleNamespace(id=1, name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, INVALID)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)

    renamed = []
    monkeypatch.setattr(
        models_access_module, "rename_channel",
        lambda _resolved, ch, new_name: renamed.append((ch.name, new_name)),
    )

    run_pipeline({"settings": {}}, mode="run")  # dry_run defaults to True

    assert renamed == [], "dry_run must never write the rename"


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

    result = run_pipeline({"settings": {}}, mode="run")

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
    monkeypatch.setattr(pipeline_module, "iter_pool", lambda *_a, **_kw: iter([]))
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

    result = run_pipeline({"settings": {}}, mode="run")

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

    result = run_pipeline({"settings": {}}, mode="run")

    assert result["status"] == "interrupted"
    assert result["probed"] == 1


def test_run_pipeline_reports_ok_on_a_normal_finish(tmp_path, monkeypatch):
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID)  # fresh: nothing left to probe
    _patch_common(monkeypatch, tmp_path, channel, attached, state)

    result = run_pipeline({"settings": {}}, mode="run")

    assert result["status"] == "ok"


def test_run_pipeline_finally_populates_state_meta(tmp_path, monkeypatch):
    """run_pipeline's finally block must populate state.meta for Show Status.

    The finally block updates last_run, last_mode, and budget_stop. Show
    Status depends on this metadata being present even when the run is
    canceled or interrupted.
    """
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A")]
    state = _make_state(tmp_path)
    state.record(1, attached[0].url, VALID)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)
    _stub_probe_one(monkeypatch)
    acquire_lock("run")

    # Normal finish
    run_pipeline({"settings": {}}, mode="run")
    reloaded = State.load(tmp_path / "state.json")
    assert reloaded.meta["last_mode"] == "run"
    assert reloaded.meta["last_run"] > 0
    assert reloaded.meta["budget_stop"] is None

    # Canceled run
    state2 = _make_state(tmp_path / "state2.json")
    state2.record(1, attached[0].url, VALID)
    acquire_lock("run")
    monkeypatch.setattr(
        pipeline_module.State, "load", staticmethod(lambda *_a, **_kw: state2)
    )
    monkeypatch.setattr(pipeline_module, "cancel_requested", lambda: True)

    run_pipeline({"settings": {}}, mode="run")
    reloaded2 = State.load(tmp_path / "state2.json")
    assert reloaded2.meta["last_mode"] == "run"
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
    acquire_lock("run")

    run_pipeline({"settings": {}}, mode="run")

    progress = lock_status()["progress"]
    assert progress["channel_index"] == 1
    assert progress["channels_total"] == 1
    assert progress["channel_name"] == "RAI 1"


def test_run_pipeline_publishes_current_stream_while_probing(
    tmp_path, monkeypatch
):
    """The last-probed stream name, tracked one probe at a time.

    The final published progress must reflect the last stream actually
    probed - not some earlier per-channel snapshot.
    """
    channel = types.SimpleNamespace(name="RAI 1")
    attached = [row(1, "IT: RAI 1 HD", "A"), row(2, "IT: Rai 1 4K", "B")]
    state = _make_state(tmp_path)
    _patch_common(monkeypatch, tmp_path, channel, attached, state)
    _stub_probe_one(monkeypatch)
    acquire_lock("run")

    run_pipeline({"settings": {}}, mode="run")

    progress = lock_status()["progress"]
    assert progress["current_stream"] in {"IT: RAI 1 HD", "IT: Rai 1 4K"}
    assert progress["channel_name"] == "RAI 1"


def test_update_progress_noops_when_lock_holder_mismatches():
    """update_progress must silently no-op when it doesn't own the lock.

    This mirrors the same guard in release_lock - if the lock has been
    stolen by another run, we must not resurrect the old lock with stale
    progress data.
    """
    acquire_lock("run", now=0.0)
    pipeline_module.update_progress("run", channel_name="RAI 1")
    progress = lock_status()["progress"]
    assert progress["channel_name"] == "RAI 1"

    # Simulate another run stealing the lock
    release_lock()
    acquire_lock("preview", now=10.0)

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

    call_sequence = []

    def track_close_old():
        call_sequence.append("close_old_connections")

    def track_resolve_models():
        call_sequence.append("resolve_models")
        return object  # the stub used in _patch_common

    monkeypatch.setattr(pipeline_module, "_close_old_connections", track_close_old)
    monkeypatch.setattr(models_access_module, "resolve_models", track_resolve_models)

    run_pipeline({"settings": {}}, mode="run")

    assert call_sequence == ["close_old_connections", "resolve_models"], (
        "_close_old_connections must be called before resolve_models (first ORM access)"
    )


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

    result = pipeline_module.start({"settings": {}}, mode="run")

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

    pipeline_module.start({"settings": {}}, mode="run")

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

    monkeypatch.setattr(
        models_access_module, "save_stream_stats", lambda *_a, **_kw: None
    )

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
    """Different providers must be probed in parallel.

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

    monkeypatch.setattr(
        models_access_module, "save_stream_stats", lambda *_a, **_kw: None
    )
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

    monkeypatch.setattr(
        models_access_module, "save_stream_stats", lambda *_a, **_kw: None
    )
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

    monkeypatch.setattr(
        models_access_module, "save_stream_stats", lambda *_a, **_kw: None
    )
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
        pipeline_module, "update_progress", lambda *_a, **_kw: refreshes.append(True)
    )
    monkeypatch.setattr(
        models_access_module, "save_stream_stats", lambda *_a, **_kw: None
    )

    log = types.SimpleNamespace(info=lambda *_a, **_kw: None)

    probed = pipeline_module._probe_candidates(
        candidates, state, settings, StubProber(), budget, resolved=None, log=log,
    )

    assert probed == 25
    assert len(saves) == 1, "state.save() should fire once at the 25th probe"
    assert len(refreshes) == 1, "update_progress() heartbeat should fire alongside it"


def test_probe_candidates_accumulates_the_heartbeat_cadence_across_channels(
    tmp_path, monkeypatch
):
    """The 25-probe cadence must accumulate across the whole run.

    It must not reset every time `_probe_candidates` is called for a new
    channel. run_pipeline calls _probe_candidates once per channel, and
    real deployments have far fewer than 25 candidates per channel (a rough
    cost model puts it around 8 candidates/channel; max_streams_per_channel
    defaults to 10) - so a per-call-local counter would never reach 25, and
    the heartbeat/save would never fire during a real multi-channel run.
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
        pipeline_module, "update_progress", lambda *_a, **_kw: refreshes.append(True)
    )
    monkeypatch.setattr(
        models_access_module, "save_stream_stats", lambda *_a, **_kw: None
    )

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

    monkeypatch.setattr(
        models_access_module, "save_stream_stats", lambda *_a, **_kw: None
    )
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

    monkeypatch.setattr(
        models_access_module, "save_stream_stats", lambda *_a, **_kw: None
    )
    monkeypatch.setattr(probing_module, "is_blank", lambda *_a, **_kw: True)
    log = types.SimpleNamespace(info=lambda *_a, **_kw: None)

    pipeline_module._probe_candidates(
        candidates, state, settings, StubProber(), budget, resolved=None, log=log,
    )

    assert state.response_time_ms(1) is None


def test_handle_probe_result_calls_save_stream_stats_on_valid(tmp_path, monkeypatch):
    """_handle_probe_result must save stream stats for VALID probes.

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

    monkeypatch.setattr(models_access_module, "save_stream_stats", fake_save)

    # Call _handle_probe_result directly with a VALID verdict
    result = pipeline_module._handle_probe_result(
        candidate, VALID, valid_stats, 275, "ok",
        resolved=None, state=state,
        log=types.SimpleNamespace(info=lambda *_a, **_kw: None),
        progress_cb=None, probed_before=0, mode="run",
    )

    assert result is True
    assert len(save_calls) == 1
    assert save_calls[0][0] == 1
    assert save_calls[0][1] == valid_stats


def test_handle_probe_result_skips_save_on_invalid(tmp_path, monkeypatch):
    """_handle_probe_result must NOT save stream stats for INVALID."""
    state = State(path=tmp_path / "state.json")
    candidate = row(1, "IT: RAI 1 HD", "A")

    save_calls = []
    monkeypatch.setattr(
        models_access_module, "save_stream_stats",
        lambda *a, **_kw: save_calls.append(a)
    )

    result = pipeline_module._handle_probe_result(
        candidate, INVALID, {}, None, "dead",
        resolved=None, state=state,
        log=types.SimpleNamespace(info=lambda *_a, **_kw: None),
        progress_cb=None, probed_before=0, mode="run",
    )

    assert result is True
    assert save_calls == [], "INVALID must not trigger save"


# --- Gevent detection (Task 2) --------------------------------------------------


def test_gevent_patched_is_false_without_gevent_installed():
    """Both spawn() and execution_model() must use one shared gevent check.

    The dev/test environment has no gevent installed (only the live
    Dispatcharr container is expected to), so both functions must fall
    back to the plain-thread path through the shared _gevent_patched()
    helper.
    """
    assert pipeline_module._gevent_patched() is False
    assert pipeline_module.execution_model() == "daemon thread"


