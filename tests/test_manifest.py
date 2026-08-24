import json
import pathlib

PLUGIN_DIR = pathlib.Path(__file__).resolve().parent.parent / "failoverr"


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

    mutating = {"run", "reorder_only", "probe_only", "clear_state"}
    for action in Plugin.actions:
        if action["id"] in mutating:
            assert action.get("confirm", {}).get("required") is True, (
                f"{action['id']} must have a confirm modal"
            )


def test_plugin_json_metadata_matches_plugin_class():
    """plugin.json metadata must match Plugin class attributes."""
    import json
    import pathlib

    from failoverr.plugin import Plugin

    plugin_dir = pathlib.Path(__file__).resolve().parent.parent / "failoverr"
    manifest = json.loads((plugin_dir / "plugin.json").read_text())

    assert manifest["name"] == Plugin.name
    assert manifest["version"] == Plugin.version
    assert manifest["description"] == Plugin.description
    assert manifest["author"] == Plugin.author


def test_dry_run_defaults_to_on():
    from failoverr.plugin import Plugin

    field = next(f for f in Plugin.fields if f["id"] == "dry_run")
    assert field["default"] is True


def test_rank_by_bitrate_defaults_to_true():
    from failoverr.plugin import Plugin

    field = next(f for f in Plugin.fields if f["id"] == "rank_by_bitrate")
    assert field["type"] == "boolean"
    assert field["default"] is True


def test_response_time_bucket_ms_field_exists_with_a_sane_default():
    from failoverr.ordering import DEFAULT_RESPONSE_TIME_BUCKET_MS
    from failoverr.plugin import Plugin

    field = next(f for f in Plugin.fields if f["id"] == "response_time_bucket_ms")
    assert field["type"] == "number"
    # the constant, not a literal: the UI default and the ranking fallback
    # are the same number and must stay that way.
    assert field["default"] == DEFAULT_RESPONSE_TIME_BUCKET_MS


def test_every_settings_field_id_is_consumed_by_load_settings():
    from failoverr.pipeline import load_settings
    from failoverr.plugin import Plugin

    field_ids = {f["id"] for f in Plugin.fields if f["type"] != "info"}
    setting_keys = set(load_settings({"settings": {}}))
    assert field_ids == setting_keys


def test_every_settings_default_matches_the_field_default():
    """Every field default survives load_settings unchanged.

    _DEFAULTS is derived from Plugin.fields, so the equality check is
    tautological for plain fields - the load-bearing parts are the type
    assertions (a string default on a number field, or a "false" string on a
    boolean, would slip through _INT_KEYS/_BOOL_KEYS coercion unnoticed) and
    the three text-list keys, whose defaults really are parsed independently.
    """
    from failoverr.naming import DEFAULT_STRIP_TOKENS
    from failoverr.ordering import DEFAULT_CODEC_PRIORITY
    from failoverr.pipeline import load_settings
    from failoverr.plugin import Plugin

    settings = load_settings({"settings": {}})

    # These three are parsed by load_settings itself (comma/newline split),
    # not passed through as their field's raw string default - see the
    # _TEXT_LIST_KEYS comment in pipeline.py.
    text_list_expected = {
        "strip_tokens": DEFAULT_STRIP_TOKENS,
        "codec_priority": DEFAULT_CODEC_PRIORITY,
        "channel_names": [],
    }

    for field in Plugin.fields:
        if field["id"] in text_list_expected:
            assert settings[field["id"]] == text_list_expected[field["id"]]
            continue
        assert settings[field["id"]] == field["default"]
        if field["type"] == "boolean":
            assert isinstance(settings[field["id"]], bool)
        elif field["type"] == "number":
            assert isinstance(settings[field["id"]], int)


def test_no_module_level_django_or_dispatcharr_imports():
    """CLAUDE.md: Django/Dispatcharr imports stay inside functions.

    The pure modules must import with neither Django nor celery installed,
    which is what lets `uv run --with pytest pytest tests/` work with no
    extra `--with` flags. A single top-level `from django.db.models import
    UniqueConstraint` in models_access.py once broke collection for three
    test modules at once, and it broke silently for anyone who happened to
    have Django on their path.

    Parsed rather than imported: this stays honest even when the test run
    DOES have Django available.
    """
    import ast

    banned = ("django", "celery", "apps", "core", "dispatcharr")
    offenders = []
    for path in sorted(PLUGIN_DIR.glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in tree.body:  # module level only - nested imports are the point
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            else:
                continue
            offenders.extend(
                f"{path.name}:{node.lineno} imports {name}"
                for name in names
                if name.split(".")[0] in banned
            )

    assert not offenders, (
        "move these inside the function that uses them:\n  "
        + "\n  ".join(offenders)
    )
