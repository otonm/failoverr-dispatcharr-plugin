"""Regression tests for the fix round on Task 18's review findings.

Finding 1: a malformed cron_expression must never escape _ensure_scheduler
and break _diagnose/_start's return-value guarantee.
Finding 2: _ensure_scheduler must hold a lock for its whole
read-stop-construct-start sequence so two near-simultaneous calls can't
each start a Scheduler and leak one's thread.
"""

import logging

import pytest

from failoverr import plugin as plugin_module
from failoverr.plugin import Plugin, _ensure_scheduler


@pytest.fixture(autouse=True)
def _reset_scheduler():
    """Every test starts and ends with no live scheduler thread."""
    yield
    if plugin_module._scheduler is not None:
        plugin_module._scheduler.stop()
        plugin_module._scheduler = None


def test_malformed_cron_expression_does_not_raise(caplog):
    context = {
        "settings": {
            "schedule_enabled": True,
            "cron_expression": "not a cron expression",
            "timezone": "UTC",
        }
    }
    with caplog.at_level(logging.ERROR, logger="failoverr"):
        result = _ensure_scheduler(context)

    assert result is None
    assert plugin_module._scheduler is None
    assert any("not armed" in r.message for r in caplog.records), caplog.records


def test_start_still_returns_pipelines_result_with_a_bad_cron_expression(monkeypatch):
    """Regression for the review finding.

    _ensure_scheduler raising must not replace _start's real return value
    with a generic error dict.
    """
    real_result = {"status": "ok", "marker": "real pipeline.start() result"}
    monkeypatch.setattr("failoverr.pipeline.start", lambda *_a, **_k: real_result)
    context = {
        "settings": {
            "schedule_enabled": True,
            "cron_expression": "garbage",
            "timezone": "UTC",
        }
    }

    result = Plugin().run("run", {}, context)

    assert result == real_result


def test_ensure_scheduler_holds_the_lock_for_the_whole_construct_sequence(monkeypatch):
    """Construction-level check for the locking finding.

    The guard must be held while a new Scheduler is built and started, not
    released early.
    """
    observed = {}
    guard = plugin_module._scheduler_guard

    class FakeScheduler:
        def __init__(self, *_args, **_kwargs):
            observed["locked_during_construct"] = guard.locked()

        def start(self):
            pass

        def stop(self):
            pass

    monkeypatch.setattr("failoverr.scheduling.Scheduler", FakeScheduler)
    context = {
        "settings": {
            "schedule_enabled": True,
            "cron_expression": "0 4 * * *",
            "timezone": "UTC",
        }
    }

    _ensure_scheduler(context)

    assert observed["locked_during_construct"] is True
