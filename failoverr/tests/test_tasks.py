"""Tests for scheduled_run.

It must reuse the exact code path a manual Run button takes - Dispatcharr's
own PluginManager.run_action - rather than re-deriving settings or
re-implementing the lock/dry_run handling in a second place.
"""

import sys
import types

import pytest

from failoverr.tasks import PLUGIN_KEY, scheduled_run


@pytest.fixture
def fake_plugin_manager(monkeypatch):
    calls = []

    class FakeManager:
        def run_action(self, key, action_id, params):
            calls.append((key, action_id, params))
            return {"status": "ok"}

    fake_loader = types.ModuleType("apps.plugins.loader")
    fake_loader.PluginManager = types.SimpleNamespace(get=FakeManager)
    fake_apps = types.ModuleType("apps")
    fake_apps_plugins = types.ModuleType("apps.plugins")
    monkeypatch.setitem(sys.modules, "apps", fake_apps)
    monkeypatch.setitem(sys.modules, "apps.plugins", fake_apps_plugins)
    monkeypatch.setitem(sys.modules, "apps.plugins.loader", fake_loader)
    return calls


def test_scheduled_run_calls_run_action_with_the_run_action_id(fake_plugin_manager):
    scheduled_run()

    assert fake_plugin_manager == [(PLUGIN_KEY, "run", {})]


def test_scheduled_run_returns_run_actions_result(fake_plugin_manager):  # noqa: ARG001
    assert scheduled_run() == {"status": "ok"}
