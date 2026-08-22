"""Celery task fired by django-celery-beat's schedule (see scheduling.py).

Dispatcharr's own celery worker startup hook (`worker_process_init` in
dispatcharr/celery.py) imports every enabled plugin's plugin.py at each
worker (re)start - that's what makes the @shared_task below register in
that worker's task registry. celery beat itself never imports this file;
it only needs the dotted TASK_NAME string stored in a PeriodicTask row
(see scheduling.sync_celery_beat).

celery may not be installed at all - this repo's own bare pytest run has
neither Django nor celery - so the import is guarded the same way
models_access.py guards its optional imports, keeping plugin.py (which
imports this module for the registration side effect) importable either
way.
"""

import logging

logger = logging.getLogger("failoverr")

PLUGIN_KEY = "failoverr"
TASK_NAME = "failoverr.scheduled_run"

try:
    from celery import shared_task
except ImportError:

    def shared_task(*_args, **_kwargs):
        return lambda fn: fn


@shared_task(name=TASK_NAME)
def scheduled_run():
    """Run the same code path a manual Run button press takes - no duplicated logic."""
    from apps.plugins.loader import PluginManager

    result = PluginManager.get().run_action(PLUGIN_KEY, "run", {})
    status = result.get("status") if isinstance(result, dict) else None
    verb = "COMPLETED" if status == "ok" else "FAILED"
    logger.info("FAILOVERR scheduled_run (celery beat) %s: %s", verb, result)
    return result
