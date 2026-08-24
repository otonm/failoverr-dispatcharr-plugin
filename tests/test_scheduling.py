import sys
import types

import pytest

from failoverr.scheduling import (
    celery_beat_available,
    disable_celery_beat,
    sync_celery_beat,
)


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
