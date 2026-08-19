import pytest

from failoverr.models_access import FieldResolutionError, plan_writes, resolve_field


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
    assert (
        resolve_field(model, ["order", "position", "priority"], "ordering")
        == "position"
    )


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


def test_empty_ordered_list_produces_no_writes():
    """Spec §12: a channel that matched nothing is never cleared."""
    assert plan_writes({1: 0, 2: 1}, [], [1, 2], use_offset=False) == {
        "attach": [], "detach": [], "orders": []
    }


def test_new_streams_are_attached_and_missing_ones_detached():
    result = plan_writes({1: 0, 2: 1}, [1, 3], [2], use_offset=False)
    assert result["attach"] == [3]
    assert result["detach"] == [2]
    assert result["orders"] == [(1, 0), (3, 1)]


def test_offset_mode_bumps_existing_rows_first():
    result = plan_writes({1: 0, 2: 1}, [2, 1], [], use_offset=True)
    bumps = [o for o in result["orders"] if o[1] >= 100000]
    assert len(bumps) == 2
    assert result["orders"][-2:] == [(2, 0), (1, 1)]


def test_detach_list_never_includes_a_stream_being_kept():
    result = plan_writes({1: 0, 2: 1}, [1, 2], [1], use_offset=False)
    assert result["detach"] == []
