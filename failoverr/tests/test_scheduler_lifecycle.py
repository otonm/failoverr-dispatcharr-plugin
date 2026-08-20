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


# --- django-celery-beat preferred when available ----------------------------


def test_celery_beat_is_used_instead_of_the_thread_when_available(monkeypatch):
    """§10: prefer celery beat over the thread whenever it's importable."""
    monkeypatch.setattr("failoverr.scheduling.celery_beat_available", lambda: True)
    calls = []
    monkeypatch.setattr(
        "failoverr.scheduling.sync_celery_beat",
        lambda cron, enabled: calls.append((cron, enabled)),
    )
    context = {
        "settings": {
            "schedule_enabled": True,
            "cron_expression": "0 4 * * *",
            "timezone": "UTC",
        }
    }

    result = _ensure_scheduler(context)

    assert calls == [("0 4 * * *", True)]
    assert result is None
    assert plugin_module._scheduler is None


def test_celery_beat_path_stops_any_live_thread_scheduler(monkeypatch):
    """Switching backends (or just re-arming) must not leave the old thread running."""
    settings = {
        "schedule_enabled": True, "cron_expression": "0 4 * * *", "timezone": "UTC",
    }
    monkeypatch.setattr("failoverr.scheduling.celery_beat_available", lambda: False)
    _ensure_scheduler({"settings": settings})
    assert plugin_module._scheduler is not None
    live_thread = plugin_module._scheduler._thread

    monkeypatch.setattr("failoverr.scheduling.celery_beat_available", lambda: True)
    monkeypatch.setattr("failoverr.scheduling.sync_celery_beat", lambda *_a, **_k: None)
    _ensure_scheduler({"settings": settings})

    assert plugin_module._scheduler is None
    assert live_thread.is_alive() is False


def test_malformed_cron_does_not_raise_in_the_celery_beat_path(monkeypatch, caplog):
    monkeypatch.setattr("failoverr.scheduling.celery_beat_available", lambda: True)
    context = {
        "settings": {
            "schedule_enabled": True,
            "cron_expression": "garbage",
            "timezone": "UTC",
        }
    }

    with caplog.at_level(logging.ERROR, logger="failoverr"):
        result = _ensure_scheduler(context)

    assert result is None
    assert any("not armed" in r.message for r in caplog.records), caplog.records


def test_stop_disables_celery_beat(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "failoverr.scheduling.disable_celery_beat", lambda: calls.append(True)
    )

    Plugin().stop()

    assert calls == [True]
