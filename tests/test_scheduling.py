import datetime
import logging
import sys
import types

import pytest

from failoverr.scheduling import (
    Scheduler,
    celery_beat_available,
    disable_celery_beat,
    matches_cron,
    resolve_timezone,
    sync_celery_beat,
)


def at(year, month, day, hour, minute):
    return datetime.datetime(year, month, day, hour, minute)  # noqa: DTZ001 - matches_cron takes naive datetimes


def test_nightly_default_fires_only_at_the_exact_minute():
    assert matches_cron("0 4 * * *", at(2026, 8, 19, 4, 0)) is True
    assert matches_cron("0 4 * * *", at(2026, 8, 19, 4, 1)) is False
    assert matches_cron("0 4 * * *", at(2026, 8, 19, 5, 0)) is False


def test_wildcard_matches_every_minute():
    assert matches_cron("* * * * *", at(2026, 8, 19, 13, 37)) is True


@pytest.mark.parametrize("minute,expected", [(0, True), (15, True), (30, True),
                                             (45, True), (7, False), (16, False)])
def test_step_values(minute, expected):
    assert matches_cron("*/15 * * * *", at(2026, 8, 19, 4, minute)) is expected


def test_lists_and_ranges():
    assert matches_cron("0 2,4 * * *", at(2026, 8, 19, 4, 0)) is True
    assert matches_cron("0 2,4 * * *", at(2026, 8, 19, 3, 0)) is False
    assert matches_cron("0 2-5 * * *", at(2026, 8, 19, 3, 0)) is True
    assert matches_cron("0 2-5 * * *", at(2026, 8, 19, 6, 0)) is False


def test_day_of_week_uses_cron_numbering():
    """2026-08-19 is a Wednesday: cron day-of-week 3."""
    assert matches_cron("0 4 * * 3", at(2026, 8, 19, 4, 0)) is True
    assert matches_cron("0 4 * * 1", at(2026, 8, 19, 4, 0)) is False


def test_sunday_accepts_both_0_and_7():
    sunday = at(2026, 8, 23, 4, 0)
    assert matches_cron("0 4 * * 0", sunday) is True
    assert matches_cron("0 4 * * 7", sunday) is True


def test_day_of_month_and_month():
    assert matches_cron("0 4 19 8 *", at(2026, 8, 19, 4, 0)) is True
    assert matches_cron("0 4 20 8 *", at(2026, 8, 19, 4, 0)) is False


@pytest.mark.parametrize("expression", ["", "0 4 * *", "0 4 * * * *", "x 4 * * *"])
def test_malformed_expressions_raise_value_error(expression):
    with pytest.raises(ValueError):  # noqa: PT011 - every malformed shape in the matrix is a ValueError
        matches_cron(expression, at(2026, 8, 19, 4, 0))


@pytest.mark.parametrize("minute,expected", [(0, True), (15, True), (30, True),
                                             (45, True), (7, False), (16, False)])
def test_step_values_with_an_explicit_start_still_step(minute, expected):
    """"0/15" must mean every 15th minute from 0, not only minute 0 itself."""
    assert matches_cron("0/15 * * * *", at(2026, 8, 19, 4, minute)) is expected


@pytest.mark.parametrize("expression", [
    "60 * * * *",   # minute out of range
    "0 24 * * *",   # hour out of range
    "0 4 32 * *",   # day out of range
    "0 4 * 13 *",   # month out of range
    "0 4 10-5 * *",  # reversed range
])
def test_out_of_range_field_raises_instead_of_silently_never_matching(expression):
    with pytest.raises(ValueError):  # noqa: PT011 - every case here is a bad cron field
        matches_cron(expression, at(2026, 8, 19, 4, 0))


def test_unknown_timezone_falls_back_to_utc_without_raising():
    """A missing timezone database must not break the rest of the plugin."""
    assert resolve_timezone("Not/AZone") is not None


def test_known_timezone_resolves():
    assert resolve_timezone("UTC") is not None


def test_stop_joins_the_thread_before_returning():
    """The plan's verification checklist requires stop() to join in bounded time.

    A bare `_stop.set()` doesn't guarantee the thread has actually exited by
    the time stop() returns - only that it will notice soon. Assert on the
    thread's real liveness, not just the event.
    """
    scheduler = Scheduler("* * * * *", "UTC", callback=lambda: None)
    scheduler.start()
    assert scheduler._thread.is_alive()

    scheduler.stop()

    assert scheduler._thread.is_alive() is False


def test_stop_warns_instead_of_claiming_success_when_the_join_times_out(caplog):
    """A stuck callback must not be logged as a clean "scheduler stopped"."""
    scheduler = Scheduler("* * * * *", "UTC", callback=lambda: None)

    class _StuckThread:
        def join(self, timeout=None):
            pass

        def is_alive(self):
            return True

    scheduler._thread = _StuckThread()

    with caplog.at_level(logging.INFO, logger="failoverr"):
        scheduler.stop()

    assert "timed out" in caplog.text
    assert "scheduler stopped" not in caplog.text


# --- django-celery-beat integration -----------------------------------------


def test_celery_beat_unavailable_when_the_modules_are_missing():
    """Confirm: this repo's own bare pytest run has neither Django nor celery."""
    assert celery_beat_available() is False


@pytest.fixture
def fake_celery_beat(monkeypatch):
    """Inject stand-ins for the two Dispatcharr-only modules this needs."""
    calls = {"created": [], "deleted": []}

    fake_core_scheduling = types.ModuleType("core.scheduling")

    def create_or_update_periodic_task(task_name, celery_task_path,
                                        cron_expression, enabled, **_kwargs):
        calls["created"].append((task_name, celery_task_path, cron_expression, enabled))

    def delete_periodic_task(task_name):
        calls["deleted"].append(task_name)

    fake_core_scheduling.create_or_update_periodic_task = create_or_update_periodic_task
    fake_core_scheduling.delete_periodic_task = delete_periodic_task

    fake_core = types.ModuleType("core")
    fake_djcb = types.ModuleType("django_celery_beat")
    fake_djcb_models = types.ModuleType("django_celery_beat.models")

    monkeypatch.setitem(sys.modules, "core", fake_core)
    monkeypatch.setitem(sys.modules, "core.scheduling", fake_core_scheduling)
    monkeypatch.setitem(sys.modules, "django_celery_beat", fake_djcb)
    monkeypatch.setitem(sys.modules, "django_celery_beat.models", fake_djcb_models)
    return calls


def test_celery_beat_available_when_both_modules_are_importable(fake_celery_beat):  # noqa: ARG001
    assert celery_beat_available() is True


def test_sync_celery_beat_creates_a_periodic_task_with_our_task_name(fake_celery_beat):
    sync_celery_beat("0 4 * * *", enabled=True)

    name, task_path, cron, enabled = fake_celery_beat["created"][0]
    assert name == "failoverr-scheduled-run"
    assert task_path == "failoverr.scheduled_run"
    assert cron == "0 4 * * *"
    assert enabled is True


def test_disable_celery_beat_deletes_the_periodic_task(fake_celery_beat):
    disable_celery_beat()

    assert fake_celery_beat["deleted"] == ["failoverr-scheduled-run"]


def test_disable_celery_beat_is_a_noop_without_celery_beat_installed():
    """Nothing to remove if it was never available - must not raise."""
    disable_celery_beat()
