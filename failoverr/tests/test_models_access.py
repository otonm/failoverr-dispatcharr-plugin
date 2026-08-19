import pytest

from failoverr.models_access import FieldResolutionError, resolve_field


class FakeField:
    def __init__(self, name):
        self.name = name


class FakeMeta:
    def __init__(self, names):
        self._names = names

    def get_fields(self):
        return [FakeField(n) for n in self._names]


class FakeModel:
    def __init__(self, names):
        self._meta = FakeMeta(names)


def test_returns_first_candidate_present():
    model = FakeModel(["id", "position", "channel"])
    assert resolve_field(model, ["order", "position", "priority"], "ordering") == "position"


def test_prefers_earlier_candidate_when_several_present():
    model = FakeModel(["id", "order", "position"])
    assert resolve_field(model, ["order", "position"], "ordering") == "order"


def test_raises_listing_available_fields_when_none_match():
    model = FakeModel(["id", "channel", "stream"])
    with pytest.raises(FieldResolutionError) as exc:
        resolve_field(model, ["order", "position"], "ordering")
    message = str(exc.value)
    assert "ordering" in message
    assert "order" in message and "position" in message
    assert "channel" in message and "stream" in message, (
        "the error must list what IS available, not just what is missing"
    )
