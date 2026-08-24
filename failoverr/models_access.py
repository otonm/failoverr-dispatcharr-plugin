"""All ORM access and runtime field-name resolution.

Dispatcharr's model field names vary between versions. The order and
provider-link fields are resolved at runtime via resolve_field(), and
failure names what IS available so a mismatch is diagnosable from the
error alone. The through-model's own channel/stream FK names are NOT
resolved this way - they're assumed to be `channel`/`stream_id`.
"""

import dataclasses
import logging
import shutil
import subprocess

from .ordering import ORDER_OFFSET, rewrite_plan

logger = logging.getLogger("failoverr")

_ORDER_FIELD_CANDIDATES = ["order", "position", "priority", "sort_order"]
_PROVIDER_FIELD_CANDIDATES = ["m3u_account", "m3u_source", "account", "source"]


class FieldResolutionError(Exception):
    """Raised when no candidate field name exists on a model."""


def resolve_field(model, candidates, purpose):
    available = sorted(f.name for f in model._meta.get_fields())
    for candidate in candidates:
        if candidate in available:
            return candidate
    raise FieldResolutionError(
        f"Failoverr could not find a field for {purpose} on "
        f"{getattr(model, '__name__', model)}. Tried: {', '.join(candidates)}. "
        f"Available fields: {', '.join(available)}."
    )


@dataclasses.dataclass
class ResolvedModels:
    channel_model: object
    stream_model: object
    channel_stream_model: object
    order_field: str
    provider_field: str
    has_unique_order_constraint: bool


def _import_channel_stream(channel_model):
    """CLAUDE.md §4: importable directly, or reachable through the M2M."""
    try:
        from apps.channels.models import ChannelStream
    except (ImportError, AttributeError):
        return channel_model.streams.through
    return ChannelStream


def _detect_unique_order_constraint(model, order_field):
    """Report a unique (channel, order) constraint: it forces the offset trick."""
    # Kept in-function on purpose (CLAUDE.md): this is the module's only
    # Django dependency, and hoisting it to the top makes models_access -
    # and everything importing it - unimportable without Django installed.
    # test_no_module_level_django_or_dispatcharr_imports enforces this.
    from django.db.models import UniqueConstraint

    for unique_together in getattr(model._meta, "unique_together", ()) or ():
        if order_field in unique_together:
            return True
    for constraint in getattr(model._meta, "constraints", ()) or ():
        fields = getattr(constraint, "fields", ()) or ()
        if (
            order_field in fields
            and isinstance(constraint, UniqueConstraint)
        ):
            return True
    return False


def resolve_models():
    from apps.channels.models import Channel, Stream

    channel_stream = _import_channel_stream(Channel)
    order_field = resolve_field(
        channel_stream, _ORDER_FIELD_CANDIDATES, "stream ordering"
    )
    provider_field = resolve_field(
        Stream, _PROVIDER_FIELD_CANDIDATES, "the M3U provider link"
    )
    return ResolvedModels(
        channel_model=Channel,
        stream_model=Stream,
        channel_stream_model=channel_stream,
        order_field=order_field,
        provider_field=provider_field,
        has_unique_order_constraint=_detect_unique_order_constraint(
            channel_stream, order_field
        ),
    )


def _binary_version(path):
    resolved = shutil.which(path) or path
    try:
        proc = subprocess.run(
            [resolved, "-version"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"path": resolved, "present": False, "version": "", "error": str(exc)}
    first_line = (proc.stdout or proc.stderr).splitlines()[:1]
    return {
        "path": resolved,
        "present": proc.returncode == 0,
        "version": first_line[0] if first_line else "",
    }


def _module_available(name):
    import importlib

    try:
        module = importlib.import_module(name)
    except Exception as exc:  # noqa: BLE001 - importing a module can raise anything
        return {"available": False, "error": str(exc)}
    return {"available": True, "version": getattr(module, "__version__", "unknown")}


def _gevent_status():
    """Decides the execution model. See spec §6."""
    try:
        from gevent import monkey
    except ImportError:
        return {"gevent": False, "patched": {}}
    return {
        "gevent": True,
        "patched": {
            module: monkey.is_module_patched(module)
            for module in ("socket", "subprocess", "threading")
        },
    }


def environment_report(ffprobe_path, ffmpeg_path):
    return {
        "ffprobe": _binary_version(ffprobe_path),
        "ffmpeg": _binary_version(ffmpeg_path),
        "gevent": _gevent_status(),
        "celery_beat": _module_available("django_celery_beat"),
        # both halves of scheduling._timezone's chain: zoneinfo first, pytz
        # as the fallback. Reporting only one makes a timezone failure
        # undiagnosable from the report alone.
        "zoneinfo": _module_available("zoneinfo"),
        "pytz": _module_available("pytz"),
    }


def plan_writes(current, ordered_ids, detach_ids, use_offset):
    """Turn a channel plan into concrete write operations.

    `current` maps attached stream_id -> current order. Returns attach /
    detach / order operations. An empty `ordered_ids` produces nothing at
    all: a channel that matched nothing is never cleared (spec §12).

    Caller contract: `ordered_ids` must include every stream_id the caller
    wants to remain attached, not just newly matched ones — a currently
    attached stream that is kept but merely omitted from `ordered_ids` is
    neither detached nor given a final order; its order keeps escalating by
    ORDER_OFFSET on every run that omits it, since it is never assigned a
    position by the pass below. The Phase 5 candidate-assembly step
    ("newly matched streams + streams already attached", CLAUDE.md §6) is
    what is meant to guarantee this in practice.
    """
    ordered_ids = list(dict.fromkeys(ordered_ids))
    if not ordered_ids:
        return {"attach": [], "detach": [], "orders": []}
    keep = set(ordered_ids)
    return {
        "attach": [sid for sid in ordered_ids if sid not in current],
        "detach": [sid for sid in detach_ids if sid not in keep],
        "orders": rewrite_plan(current, ordered_ids, use_offset),
    }


def placeholder_orders(current, attach):
    """Distinct create-time orders for newly attached rows.

    Provably disjoint from rewrite_plan's bump range (v + ORDER_OFFSET for
    v in current.values()) regardless of how many rows exist or their
    values, since it starts past the highest possible bump target. Both
    sides read the one ORDER_OFFSET defined in ordering.py.
    """
    high_water = max(current.values(), default=-1)
    return [ORDER_OFFSET + high_water + 1 + index for index in range(len(attach))]


def apply_channel_plan(resolved, channel, ordered_ids, detach_ids, dry_run):
    """Write one channel's plan. All order changes in a single transaction."""
    from django.db import transaction

    link_model = resolved.channel_stream_model
    order_field = resolved.order_field

    # Reading current state and writing the plan must be one atomic,
    # row-locked operation - otherwise a concurrent manual Dispatcharr edit
    # between the read and the write gets silently overwritten by a plan
    # computed from a stale snapshot.
    with transaction.atomic():
        current = {
            link.stream_id: getattr(link, order_field)
            for link in link_model.objects.select_for_update().filter(channel=channel)
        }
        plan = plan_writes(
            current, ordered_ids, detach_ids, resolved.has_unique_order_constraint
        )
        logger.debug(
            "FAILOVERR apply_channel_plan: channel=%s ordered=%s "
            "attach=%s detach=%s dry_run=%s",
            channel.name, ordered_ids, plan["attach"], plan["detach"], dry_run,
        )
        summary = {
            "attached": len(plan["attach"]),
            "detached": len(plan["detach"]),
        }
        if dry_run or not plan["orders"]:
            return summary

        # Placeholder orders for new rows must be disjoint from every value
        # the order pass below might use, including rewrite_plan's bump
        # range — see placeholder_orders(). The order pass right after
        # overwrites all of these unconditionally.
        for stream_id, order in zip(
            plan["attach"], placeholder_orders(current, plan["attach"]), strict=True
        ):
            link_model.objects.create(
                channel=channel, stream_id=stream_id, **{order_field: order}
            )
        for stream_id, new_order in plan["orders"]:
            link_model.objects.filter(
                channel=channel, stream_id=stream_id
            ).update(**{order_field: new_order})
        if plan["detach"]:
            link_model.objects.filter(
                channel=channel, stream_id__in=plan["detach"]
            ).delete()
    return summary


def save_stream_stats(resolved, stream_id, stats):
    """Write probe results using the key names Dispatcharr already uses.

    Merges onto the existing stream_stats dict rather than replacing it, so
    keys Dispatcharr or another plugin populated (width, height, ...) survive
    a Failoverr probe. Plugin-private bookkeeping never goes here — it lives
    in the sidecar.
    """
    from django.db import transaction
    from django.utils import timezone

    with transaction.atomic():
        stream = (
            resolved.stream_model.objects.select_for_update()
            .filter(id=stream_id).first()
        )
        if stream is None:
            logger.info(
                "FAILOVERR stream %s deleted mid-run; dropped probe stats", stream_id,
            )
            return
        existing = stream.stream_stats if isinstance(stream.stream_stats, dict) else {}
        updated = resolved.stream_model.objects.filter(id=stream_id).update(
            stream_stats={**existing, **stats}, stream_stats_updated_at=timezone.now()
        )
        logger.debug(
            "FAILOVERR save_stream_stats: stream=%s updated=%s keys=%s",
            stream_id, updated, list(stats.keys()),
        )
