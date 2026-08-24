"""django-celery-beat integration for the scheduled run.

Scheduling is celery-beat only: it survives a celery worker restart without
needing anything here to be called again, and its schedule fires from a
separate process rather than a thread inside whichever uwsgi worker happened
to handle the enabling request.
"""

import logging

from .tasks import TASK_NAME

logger = logging.getLogger("failoverr")

# django-celery-beat's PeriodicTask.name - not the celery task name (see
# tasks.TASK_NAME), just this row's identity in the PeriodicTask table.
_PERIODIC_TASK_NAME = "failoverr-scheduled-run"


def celery_beat_available():
    """Whether django-celery-beat's DB scheduler can drive the schedule.

    Both imports are Dispatcharr-only - this repo's own bare pytest run has
    neither, so it correctly reports False there.
    """
    try:
        import core.scheduling  # noqa: F401
        import django_celery_beat.models  # noqa: F401
    except ImportError:
        return False
    return True


def sync_celery_beat(cron_expression, enabled):
    """(Re)point celery beat's schedule at our task. Raises on a bad cron_expression."""
    # core.scheduling is Dispatcharr-only, unlike .tasks (guarded at its own
    # module level - see tasks.py's docstring) - so only this import has to
    # stay lazy for the module to load in a bare pytest run.
    from core.scheduling import create_or_update_periodic_task

    create_or_update_periodic_task(
        task_name=_PERIODIC_TASK_NAME,
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
        # Dispatcharr-only; the ImportError itself is the "never available"
        # case above, not just a laziness formality.
        from core.scheduling import delete_periodic_task
    except ImportError:
        logger.debug("FAILOVERR celery-beat not available, nothing to remove")
        return
    delete_periodic_task(_PERIODIC_TASK_NAME)
    logger.info("FAILOVERR celery-beat periodic task removed")
