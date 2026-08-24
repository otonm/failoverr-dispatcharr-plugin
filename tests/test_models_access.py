import sys
import types

import pytest

from failoverr.models_access import (
    FieldResolutionError,
    apply_channel_plan,
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
    assert plan_writes({1: 0, 2: 1}, [], [1, 2]) == {
        "attach": [], "detach": [], "orders": []
    }


def test_new_streams_are_attached_and_missing_ones_detached():
    result = plan_writes({1: 0, 2: 1}, [1, 3], [2])
    assert result["attach"] == [3]
    assert result["detach"] == [2]
    assert result["orders"] == [(1, 0), (3, 1)]


def test_detach_list_never_includes_a_stream_being_kept():
    result = plan_writes({1: 0, 2: 1}, [1, 2], [1])
    assert result["detach"] == []


def test_an_omitted_attached_stream_is_left_untouched():
    """A stream in current but omitted from both ordered_ids and detach_ids.

    plan_writes' docstring documents this as a caller contract hazard: the
    stream is neither detached nor given a final order, so its order is
    simply never written by this pass.
    """
    current = {1: 0, 2: 1}
    result = plan_writes(current, [1], [])
    stream_2_orders = [order for sid, order in result["orders"] if sid == 2]
    assert stream_2_orders == [], "stream 2's order must not be touched"
    assert 2 not in result["detach"], "stream 2 is not detached either"


def test_duplicate_ordered_ids_do_not_produce_a_duplicate_attach_entry():
    """Regression: duplicates used to create two rows with contradictory orders."""
    result = plan_writes({1: 0}, [2, 2, 1], [])
    assert result["attach"] == [2]
    ordered_stream_ids = [sid for sid, _order in result["orders"]]
    assert ordered_stream_ids.count(2) == 1


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
        # Iterate over a copy to avoid mutating the list while iterating.
        for row in list(self._rows):
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


def _resolved(link_model, order_field="order"):
    return types.SimpleNamespace(
        channel_stream_model=link_model,
        order_field=order_field,
    )


def test_apply_channel_plan_locks_current_rows_before_reading_them():
    """A concurrent edit between the read and the write must not be clobbered."""
    channel = types.SimpleNamespace(name="Test Channel")
    link_model = FakeChannelStreamModel(
        [_FakeLinkRow(channel, 1, 0), _FakeLinkRow(channel, 2, 1)]
    )

    apply_channel_plan(_resolved(link_model), channel, [1, 2], [], dry_run=False)

    assert link_model.select_for_update_calls == 1


def test_apply_channel_plan_writes_attach_reorder_and_detach():
    channel = types.SimpleNamespace(name="Test Channel")
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
    assert summary == {"attached": 1, "detached": 1}


def test_apply_channel_plan_dry_run_never_writes():
    channel = types.SimpleNamespace(name="Test Channel")
    link_model = FakeChannelStreamModel([_FakeLinkRow(channel, 1, 0)])

    apply_channel_plan(_resolved(link_model), channel, [1, 2], [], dry_run=True)

    assert [row.stream_id for row in link_model.rows] == [1], (
        "dry_run must never attach, reorder, or detach"
    )


# --- save_stream_stats -----------------------------------------------------


def test_save_stream_stats_logs_when_the_stream_was_deleted_mid_run(
    monkeypatch, caplog,
):
    """Regression: a deleted Stream row dropped its probe stats with no trace.

    A probe whose Stream row was deleted during the run used to drop its
    freshly-measured stats with zero trace - indistinguishable from a
    successful write, and the only path that silently wastes a probe.
    """
    import logging

    # save_stream_stats lazily imports django.utils.timezone; Django isn't
    # installed in this env, so stub it. The deleted-stream path returns
    # before touching timezone, but the import still runs.
    monkeypatch.setitem(
        sys.modules, "django.utils",
        types.SimpleNamespace(timezone=types.SimpleNamespace(now=lambda: 0)),
    )

    class _DeletedQuerySet:
        def select_for_update(self):
            return self

        def filter(self, **_kwargs):
            return self

        def first(self):
            return None

        def update(self, **_kwargs):
            raise AssertionError("must not write stats for a deleted stream")

    class _StreamModel:
        objects = _DeletedQuerySet()

    resolved = types.SimpleNamespace(stream_model=_StreamModel())

    with caplog.at_level(logging.INFO, logger="failoverr"):
        from failoverr.models_access import save_stream_stats

        save_stream_stats(resolved, 42, {"video_codec": "hevc"})

    matched = [
        r for r in caplog.records
        if "deleted mid-run" in r.message and "42" in r.message
    ]
    assert matched, caplog.records
