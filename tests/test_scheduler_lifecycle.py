"""_ensure_scheduler's celery-beat-only arming path, and Plugin.stop()'s lifecycle hook.

A malformed cron_expression, or django-celery-beat simply being unavailable,
must never escape _ensure_scheduler and break _diagnose/_start's return-value
guarantee.
"""

import logging

from failoverr.plugin import Plugin, _ensure_scheduler
from failoverr.scheduling import disable_celery_beat


def test_celery_beat_unavailable_warns_and_does_not_raise(monkeypatch, caplog):
    monkeypatch.setattr("failoverr.scheduling.celery_beat_available", lambda: False)
    context = {"settings": {"schedule_enabled": True, "cron_expression": "0 4 * * *"}}

    with caplog.at_level(logging.WARNING, logger="failoverr"):
        result = _ensure_scheduler(context)

    assert result is None
    assert any("unavailable" in r.message for r in caplog.records), caplog.records


def test_malformed_cron_expression_does_not_raise(monkeypatch, caplog):
    monkeypatch.setattr("failoverr.scheduling.celery_beat_available", lambda: True)
    context = {"settings": {"schedule_enabled": True, "cron_expression": "garbage"}}

    with caplog.at_level(logging.ERROR, logger="failoverr"):
        result = _ensure_scheduler(context)

    assert result is None
    assert any("not armed" in r.message for r in caplog.records), caplog.records


def test_start_still_returns_pipelines_result_with_a_bad_cron_expression(monkeypatch):
    """Regression for the review finding.

    _ensure_scheduler raising must not replace _start's real return value
    with a generic error dict.
    """
    real_result = {"status": "ok", "marker": "real pipeline.start() result"}
    monkeypatch.setattr("failoverr.pipeline.start", lambda *_a, **_k: real_result)
    monkeypatch.setattr("failoverr.scheduling.celery_beat_available", lambda: True)
    context = {"settings": {"schedule_enabled": True, "cron_expression": "garbage"}}

    result = Plugin().run("run", {}, context)

    assert result == real_result


def test_celery_beat_path_logs_when_the_schedule_is_armed(monkeypatch, caplog):
    monkeypatch.setattr("failoverr.scheduling.celery_beat_available", lambda: True)
    monkeypatch.setattr("failoverr.scheduling.sync_celery_beat", lambda *_a, **_k: None)
    context = {"settings": {"schedule_enabled": True, "cron_expression": "0 4 * * *"}}

    with caplog.at_level(logging.INFO, logger="failoverr"):
        result = _ensure_scheduler(context)

    assert result is None
    assert any(
        "celery-beat schedule armed" in r.message and "0 4 * * *" in r.message
        for r in caplog.records
    ), caplog.records


def test_stop_disables_celery_beat(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "failoverr.scheduling.disable_celery_beat", lambda: calls.append(True)
    )

    Plugin().stop()

    assert calls == [True]


def test_stop_logs_and_swallows_when_disable_celery_beat_raises(monkeypatch, caplog):
    """An exception in the lifecycle hook must not crash it with no trace."""
    def boom():
        raise RuntimeError("celery-beat delete failed")

    monkeypatch.setattr("failoverr.scheduling.disable_celery_beat", boom)

    with caplog.at_level(logging.ERROR, logger="failoverr"):
        Plugin().stop()  # must not raise

    assert any("stop FAILED" in r.message for r in caplog.records), caplog.records


def test_stop_logs_completion_on_success(monkeypatch, caplog):
    monkeypatch.setattr("failoverr.scheduling.disable_celery_beat", lambda: None)

    with caplog.at_level(logging.INFO, logger="failoverr"):
        Plugin().stop()

    assert any("stop COMPLETED" in r.message for r in caplog.records), caplog.records


def test_disable_celery_beat_logs_the_noop_branch(caplog):
    """The ImportError early-return must leave a trace, not vanish silently."""
    with caplog.at_level(logging.DEBUG, logger="failoverr"):
        disable_celery_beat()

    assert any("nothing to remove" in r.message for r in caplog.records), caplog.records
