"""Cron parsing and the scheduler thread.

matches_cron is pure. The thread checks the current minute every 20
seconds rather than computing the next fire time - simpler, and free at
this frequency.
"""

import datetime
import logging
import threading

logger = logging.getLogger("failoverr")

CHECK_INTERVAL_SECONDS = 20

# django-celery-beat's PeriodicTask.name - not the celery task name (see
# tasks.TASK_NAME), just this row's identity in the PeriodicTask table.
PERIODIC_TASK_NAME = "failoverr-scheduled-run"


def celery_beat_available():
    """Whether django-celery-beat's DB scheduler can drive the schedule.

    §10: prefer this over the thread when available. Both imports are
    Dispatcharr-only - this repo's own bare pytest run has neither, so it
    correctly reports False there and the thread scheduler is used.
    """
    try:
        import core.scheduling  # noqa: F401
        import django_celery_beat.models  # noqa: F401
    except ImportError:
        return False
    return True


def sync_celery_beat(cron_expression, enabled):
    """(Re)point celery beat's schedule at our task. Raises on a bad cron_expression."""
    from core.scheduling import create_or_update_periodic_task

    from .tasks import TASK_NAME

    create_or_update_periodic_task(
        task_name=PERIODIC_TASK_NAME,
        celery_task_path=TASK_NAME,
        cron_expression=cron_expression,
        enabled=enabled,
    )


def disable_celery_beat():
    """Silence a previously-registered schedule on stop()/disable/delete.

    Best-effort: if django-celery-beat was never available there is
    nothing to remove.
    """
    try:
        from core.scheduling import delete_periodic_task
    except ImportError:
        return
    delete_periodic_task(PERIODIC_TASK_NAME)


def _field_matches(spec, value, minimum, maximum):
    for part in str(spec).split(","):
        part = part.strip()  # noqa: PLW2901 - reduced in place, this is the cron field parser
        if not part:
            raise ValueError(f"empty cron field component in {spec!r}")
        step = 1
        if "/" in part:
            part, _, raw_step = part.partition("/")  # noqa: PLW2901
            step = int(raw_step)
            part = part or "*"  # noqa: PLW2901
        if part == "*":
            low, high = minimum, maximum
        elif "-" in part:
            raw_low, _, raw_high = part.partition("-")
            low, high = int(raw_low), int(raw_high)
        else:
            low = high = int(part)
        if low <= value <= high and (value - low) % step == 0:
            return True
    return False


def matches_cron(expression, when):
    """Does `when` fall on this five-field cron expression?"""  # noqa: D400, D401
    fields = str(expression or "").split()
    if len(fields) != 5:  # noqa: PLR2004 - a cron expression has 5 fields, not a tunable
        raise ValueError(
            f"cron expression must have 5 fields, got {len(fields)}: {expression!r}"
        )
    minute, hour, day, month, weekday = fields
    try:
        # cron day-of-week: 0 and 7 are both Sunday.
        cron_weekday = (when.weekday() + 1) % 7
        return (
            _field_matches(minute, when.minute, 0, 59)
            and _field_matches(hour, when.hour, 0, 23)
            and _field_matches(day, when.day, 1, 31)
            and _field_matches(month, when.month, 1, 12)
            and (
                _field_matches(weekday, cron_weekday, 0, 7)
                or (cron_weekday == 0 and _field_matches(weekday, 7, 0, 7))
            )
        )
    except ValueError as exc:
        raise ValueError(f"invalid cron expression {expression!r}: {exc}") from exc


def resolve_timezone(name):
    """Falls back to UTC with a warning rather than breaking the plugin."""
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(str(name or "UTC"))
    except Exception:  # noqa: S110, BLE001 - falling through to the pytz/UTC fallback below
        pass
    try:
        import pytz

        return pytz.timezone(str(name or "UTC"))
    except Exception:  # noqa: BLE001 - any failure here must fall back to UTC, never raise
        logger.warning(
            "FAILOVERR timezone %r is unavailable, falling back to UTC", name
        )
        return datetime.timezone.utc  # noqa: UP017 - py311+ alias not worth diverging from stdlib name here


class Scheduler:
    """Daemon thread that fires `callback` when the cron expression matches.

    Started on plugin enable and stopped in Plugin.stop(), so it survives
    container restarts.
    """

    def __init__(self, expression, timezone_name, callback):
        matches_cron(expression, datetime.datetime.now())  # noqa: DTZ005 - fail fast, no tz needed for shape validation
        self.expression = expression
        self.tzinfo = resolve_timezone(timezone_name)
        self.callback = callback
        self._stop = threading.Event()
        self._thread = None
        self._last_fired_minute = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info("FAILOVERR scheduler started: %r (%s)",
                    self.expression, self.tzinfo)

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=CHECK_INTERVAL_SECONDS + 5)
        logger.info("FAILOVERR scheduler stopped")

    def _loop(self):
        while not self._stop.wait(CHECK_INTERVAL_SECONDS):
            now = datetime.datetime.now(self.tzinfo)
            marker = now.replace(second=0, microsecond=0)
            if marker == self._last_fired_minute:
                continue
            try:
                if matches_cron(self.expression, now):
                    self._last_fired_minute = marker
                    self.callback()
            except Exception as exc:
                logger.exception("FAILOVERR scheduled run FAILED: %s", exc)  # noqa: TRY401
