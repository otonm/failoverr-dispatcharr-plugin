import datetime

import pytest

from failoverr.scheduling import Scheduler, matches_cron, resolve_timezone


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
