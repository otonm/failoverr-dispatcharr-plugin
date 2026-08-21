import sys
import types

import pytest

from failoverr.models_access import (
    FieldResolutionError,
    apply_channel_plan,
    placeholder_orders,
    plan_writes,
    resolve_field,
)


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


def test_duplicate_ordered_ids_do_not_produce_a_duplicate_attach_entry():
    """Regression: duplicates used to create two rows with contradictory orders."""
    result = plan_writes({1: 0}, [2, 2, 1], [], use_offset=True)
    assert result["attach"] == [2]
    ordered_stream_ids = [sid for sid, _order in result["orders"]]
    assert ordered_stream_ids.count(2) == 1


# --- placeholder_orders ------------------------------------------------------


def test_placeholder_orders_are_disjoint_from_the_bump_range():
    """The invariant that broke twice during task review.

    New-row placeholder orders must never collide with rewrite_plan's bump
    range.
    """
    current = {10: 0, 11: 1, 12: 2}
    bump_targets = {v + 100000 for v in current.values()}
    placeholders = placeholder_orders(current, attach=[20, 21])
    assert not (set(placeholders) & bump_targets)


def test_placeholder_orders_are_mutually_distinct():
    placeholders = placeholder_orders({1: 0}, attach=[2, 3, 4])
    assert len(placeholders) == len(set(placeholders))


def test_placeholder_orders_with_no_existing_rows():
    assert placeholder_orders({}, attach=[1, 2]) == [100000, 100001]


# --- apply_channel_plan --------------------------------------------------


class _FakeLinkRow:
    def __init__(self, channel, stream_id, order):
        self.channel = channel
        self.stream_id = stream_id
        self.order = order


class _FakeQuerySet:
    def __init__(self, model, rows):
        self._model = model
        self._rows = rows

    def select_for_update(self):
        self._model.select_for_update_calls += 1
        return self

    def filter(self, **kwargs):
        def matches(row):
            for key, value in kwargs.items():
                if key.endswith("__in"):
                    if getattr(row, key[: -len("__in")]) not in value:
                        return False
                elif getattr(row, key) != value:
                    return False
            return True

        return _FakeQuerySet(self._model, [row for row in self._rows if matches(row)])

    def __iter__(self):
        return iter(self._rows)

    def create(self, **kwargs):
        row = _FakeLinkRow(kwargs["channel"], kwargs["stream_id"], kwargs["order"])
        self._model.rows.append(row)
        return row

    def update(self, **kwargs):
        for row in self._rows:
            for key, value in kwargs.items():
                setattr(row, key, value)
        return len(self._rows)

    def delete(self):
        for row in self._rows:
            self._model.rows.remove(row)


class FakeChannelStreamModel:
    """Stands in for the Django through-model: objects IS the manager."""

    def __init__(self, rows):
        self.rows = rows
        self.select_for_update_calls = 0
        self.objects = self

    def select_for_update(self):
        return _FakeQuerySet(self, self.rows).select_for_update()

    def filter(self, **kwargs):
        return _FakeQuerySet(self, self.rows).filter(**kwargs)

    def create(self, **kwargs):
        return _FakeQuerySet(self, self.rows).create(**kwargs)


@pytest.fixture(autouse=True)
def _fake_django_db(monkeypatch):
    """Fake out django.db.transaction, lazily imported by apply_channel_plan."""
    class _FakeAtomic:
        def __enter__(self):
            return self

        def __exit__(self, *_exc_info):
            return False

    fake_transaction = types.SimpleNamespace(atomic=_FakeAtomic)
    fake_db = types.SimpleNamespace(transaction=fake_transaction)
    monkeypatch.setitem(sys.modules, "django.db", fake_db)
    monkeypatch.setitem(sys.modules, "django", types.SimpleNamespace(db=fake_db))


def _resolved(link_model, order_field="order", use_offset=False):
    return types.SimpleNamespace(
        channel_stream_model=link_model,
        order_field=order_field,
        has_unique_order_constraint=use_offset,
    )


def test_apply_channel_plan_locks_current_rows_before_reading_them():
    """A concurrent edit between the read and the write must not be clobbered."""
    channel = object()
    link_model = FakeChannelStreamModel(
        [_FakeLinkRow(channel, 1, 0), _FakeLinkRow(channel, 2, 1)]
    )

    apply_channel_plan(_resolved(link_model), channel, [1, 2], [], dry_run=False)

    assert link_model.select_for_update_calls == 1


def test_apply_channel_plan_writes_attach_reorder_and_detach():
    channel = object()
    link_model = FakeChannelStreamModel(
        [_FakeLinkRow(channel, 1, 0), _FakeLinkRow(channel, 2, 1)]
    )

    summary = apply_channel_plan(
        _resolved(link_model), channel,
        ordered_ids=[2, 3], detach_ids=[1], dry_run=False,
    )

    remaining = {row.stream_id: row.order for row in link_model.rows}
    assert 1 not in remaining, "stream dropped from ordered_ids must be detached"
    assert remaining[2] == 0
    assert 3 in remaining, "a new stream_id must be attached"
    assert summary == {"attached": 1, "detached": 1, "reordered": 2}


def test_apply_channel_plan_dry_run_never_writes():
    channel = object()
    link_model = FakeChannelStreamModel([_FakeLinkRow(channel, 1, 0)])

    apply_channel_plan(_resolved(link_model), channel, [1, 2], [], dry_run=True)

    assert [row.stream_id for row in link_model.rows] == [1], (
        "dry_run must never attach, reorder, or detach"
    )
