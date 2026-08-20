import pytest

from failoverr.models_access import FieldResolutionError
from failoverr.plugin import Plugin, _flatten


def test_unknown_action_returns_error_not_exception():
    result = Plugin().run("no_such_action", {}, {})
    assert result["status"] == "error"
    assert "no_such_action" in result["message"]


def test_field_resolution_failure_surfaces_as_readable_error(monkeypatch):
    def boom():
        raise FieldResolutionError(
            "Failoverr could not find a field for stream ordering. "
            "Available fields: channel, id, stream."
        )

    monkeypatch.setattr("failoverr.models_access.resolve_models", boom)
    result = Plugin().run("diagnose", {}, {})
    assert result["status"] == "error"
    assert "Available fields" in result["message"], (
        "the user must see what fields DO exist so they can report them"
    )


class FakeLogger:
    """Captures rendered lines the way a log handler would see them."""

    def __init__(self):
        self.lines = []

    def _record(self, msg, *args):
        self.lines.append(msg % args if args else msg)

    info = error = exception = _record


def _stub(result):
    """Build a _diagnose replacement that returns a fixed report."""
    return lambda *_args, **_kwargs: result


def test_flatten_gives_every_leaf_its_own_dotted_path():
    payload = {
        "channels": {
            "total": 2,
            "normalization_examples": [
                {"name": "RAI 1", "tokens": ["rai", "1"]},
            ],
        },
    }
    assert dict(_flatten(payload)) == {
        "channels.total": 2,
        "channels.normalization_examples[0].name": "RAI 1",
        "channels.normalization_examples[0].tokens": ["rai", "1"],
    }


def test_flatten_treats_empty_containers_as_leaves():
    assert list(_flatten({"a": {}, "b": []})) == [("a", {}), ("b", [])]


def test_every_logged_line_is_greppable(monkeypatch):
    """Check the grep contract.

    Dispatcharr shows nothing in the UI and grep is line-based, so a
    line without the prefix is a line the user can never find.
    """
    log = FakeLogger()
    monkeypatch.setattr(Plugin, "_diagnose", _stub({"status": "ok", "a": {"b": 1}}))
    Plugin().run("diagnose", {}, {"logger": log, "settings": {"match_mode": "strict"}})

    assert all(line.startswith("FAILOVERR ") for line in log.lines), log.lines
    assert "FAILOVERR diagnose settings.match_mode = strict" in log.lines
    assert "FAILOVERR diagnose result.a.b = 1" in log.lines
    assert "FAILOVERR diagnose COMPLETED" in log.lines


def test_error_result_logs_failed_not_completed(monkeypatch):
    log = FakeLogger()
    monkeypatch.setattr(Plugin, "_diagnose", _stub({"status": "error"}))
    Plugin().run("diagnose", {}, {"logger": log})
    assert "FAILOVERR diagnose FAILED" in log.lines
    assert "FAILOVERR diagnose COMPLETED" not in log.lines


def test_secret_settings_are_not_logged():
    log = FakeLogger()
    Plugin().run(
        "no_such_action", {}, {"logger": log, "settings": {"password": "hunter2"}}
    )
    assert "hunter2" not in "\n".join(log.lines)
    assert "FAILOVERR no_such_action settings.password = ***" in log.lines


@pytest.mark.parametrize("action_id", [
    a["id"] for a in Plugin.actions
])
def test_every_declared_action_has_a_handler(action_id, monkeypatch):
    """A declared action with no handler is a button that reports an error."""
    def fail(*_, **__):
        raise RuntimeError("reached the handler")

    monkeypatch.setattr("failoverr.models_access.resolve_models", fail)
    monkeypatch.setattr("failoverr.pipeline.start", fail)
    monkeypatch.setattr("failoverr.pipeline.run_preview", fail)

    result = Plugin().run(action_id, {}, {})
    assert "Unknown action" not in result.get("message", "")
