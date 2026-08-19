from failoverr.models_access import FieldResolutionError
from failoverr.plugin import Plugin


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
