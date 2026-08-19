import json
import pathlib

PLUGIN_DIR = pathlib.Path(__file__).resolve().parent.parent


def test_plugin_json_parses_and_has_required_metadata():
    manifest = json.loads((PLUGIN_DIR / "plugin.json").read_text())
    for key in ("name", "version", "description", "author"):
        assert key in manifest, f"plugin.json missing {key}"


def test_plugin_json_carries_no_fields_or_actions():
    """Settings live only in plugin.py. See the plan's File Structure note."""
    manifest = json.loads((PLUGIN_DIR / "plugin.json").read_text())
    assert "fields" not in manifest
    assert "actions" not in manifest


def test_plugin_exposes_actions_with_unique_ids():
    from failoverr.plugin import Plugin

    ids = [a["id"] for a in Plugin.actions]
    assert len(ids) == len(set(ids)), f"duplicate action ids: {ids}"
    assert "diagnose" in ids


def test_every_field_has_help_text():
    from failoverr.plugin import Plugin

    for field in Plugin.fields:
        if field["type"] == "info":
            continue
        assert field.get("help_text"), f"{field['id']} has no help_text"


def test_mutating_actions_require_confirmation():
    from failoverr.plugin import Plugin

    mutating = {"run", "reorder_only", "probe_only"}
    for action in Plugin.actions:
        if action["id"] in mutating:
            assert action.get("confirm", {}).get("required") is True, (
                f"{action['id']} must have a confirm modal"
            )


def test_dry_run_defaults_to_on():
    from failoverr.plugin import Plugin

    field = next(f for f in Plugin.fields if f["id"] == "dry_run")
    assert field["default"] is True
