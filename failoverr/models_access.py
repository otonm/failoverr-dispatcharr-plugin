"""All ORM access and runtime field-name resolution.

Dispatcharr's model field names vary between versions. Nothing here
hardcodes one; every name is resolved at runtime and failure names what
IS available so a mismatch is diagnosable from the error alone.
"""

import dataclasses
import shutil
import subprocess

from .ordering import rewrite_plan

ORDER_FIELD_CANDIDATES = ["order", "position", "priority", "sort_order"]
PROVIDER_FIELD_CANDIDATES = ["m3u_account", "m3u_source", "account", "source"]


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
    for unique_together in getattr(model._meta, "unique_together", ()) or ():
        if order_field in unique_together:
            return True
    for constraint in getattr(model._meta, "constraints", ()) or ():
        fields = getattr(constraint, "fields", ()) or ()
        if (
            order_field in fields
            and constraint.__class__.__name__ == "UniqueConstraint"
        ):
            return True
    for field in model._meta.get_fields():
        if getattr(field, "name", None) == order_field and getattr(
            field, "unique", False
        ):
            return True
    return False


def resolve_models():
    from apps.channels.models import Channel, Stream

    channel_stream = _import_channel_stream(Channel)
    order_field = resolve_field(
        channel_stream, ORDER_FIELD_CANDIDATES, "stream ordering"
    )
    provider_field = resolve_field(
        Stream, PROVIDER_FIELD_CANDIDATES, "the M3U provider link"
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
        return {"path": resolved, "present": False, "error": str(exc)}
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
        "rapidfuzz": _module_available("rapidfuzz"),
        "celery_beat": _module_available("django_celery_beat"),
        "zoneinfo": _module_available("zoneinfo"),
        "pytz": _module_available("pytz"),
    }


ORDER_OFFSET = 100000


def plan_writes(current, ordered_ids, detach_ids, use_offset, offset=ORDER_OFFSET):
    """Turn a channel plan into concrete write operations.

    `current` maps attached stream_id -> current order. Returns attach /
    detach / order operations. An empty `ordered_ids` produces nothing at
    all: a channel that matched nothing is never cleared (spec §12).
    """
    if not ordered_ids:
        return {"attach": [], "detach": [], "orders": []}
    keep = set(ordered_ids)
    return {
        "attach": [sid for sid in ordered_ids if sid not in current],
        "detach": [sid for sid in detach_ids if sid not in keep],
        "orders": rewrite_plan(current, ordered_ids, use_offset, offset),
    }


def apply_channel_plan(resolved, channel, ordered_ids, detach_ids, dry_run):
    """Write one channel's plan. All order changes in a single transaction."""
    from django.db import transaction

    link_model = resolved.channel_stream_model
    order_field = resolved.order_field

    current = {
        link.stream_id: getattr(link, order_field)
        for link in link_model.objects.filter(channel=channel)
    }
    plan = plan_writes(
        current, ordered_ids, detach_ids, resolved.has_unique_order_constraint
    )
    summary = {
        "attached": len(plan["attach"]),
        "detached": len(plan["detach"]),
        "reordered": len(ordered_ids),
    }
    if dry_run or not plan["orders"]:
        return summary

    with transaction.atomic():
        # Distinct placeholder orders (offset + index) so 2+ creates in this
        # loop can never collide under a unique (channel, order) constraint.
        # The order pass right after overwrites all of these unconditionally.
        for index, stream_id in enumerate(plan["attach"]):
            link_model.objects.create(
                channel=channel,
                stream_id=stream_id,
                **{order_field: ORDER_OFFSET + index},
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

    Plugin-private bookkeeping never goes here — it lives in the sidecar.
    """
    from django.utils import timezone

    resolved.stream_model.objects.filter(id=stream_id).update(
        stream_stats=stats, stream_stats_updated_at=timezone.now()
    )
