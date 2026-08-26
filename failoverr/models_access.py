"""All ORM access and runtime field-name resolution.

Dispatcharr's model field names vary between versions. The order and
provider-link fields are resolved at runtime via resolve_field(), and
failure names what IS available so a mismatch is diagnosable from the
error alone. The through-model's own channel/stream FK names are NOT
resolved this way - they're assumed to be `channel`/`stream_id`.

Every django.*/apps.* import below lives inside the function that uses
it, never at module level - that's what lets this module (and everything
that imports it) load with neither Django nor Dispatcharr installed, the
same contract test_no_module_level_django_or_dispatcharr_imports enforces
for every file in this package.
"""

import dataclasses
import importlib
import logging
import shutil
import subprocess

from .ordering import rewrite_plan

logger = logging.getLogger("failoverr")

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


def resolve_models():
    # Lazy per the module docstring: apps.channels.models only exists inside
    # a running Dispatcharr instance.
    from apps.channels.models import Channel, Stream

    # Channel.streams.through is authoritative regardless of whether the
    # M2M's through-model has its own importable name: when one is set via
    # `through=`, Django's .through returns exactly that class either way.
    channel_stream = Channel.streams.through
    order_field = resolve_field(channel_stream, ["order"], "stream ordering")
    provider_field = resolve_field(Stream, ["m3u_account"], "the M3U provider link")
    return ResolvedModels(
        channel_model=Channel,
        stream_model=Stream,
        channel_stream_model=channel_stream,
        order_field=order_field,
        provider_field=provider_field,
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
    try:
        module = importlib.import_module(name)
    except Exception as exc:  # noqa: BLE001 - importing a module can raise anything
        return {"available": False, "error": str(exc)}
    return {"available": True, "version": getattr(module, "__version__", "unknown")}


def gevent_monkey():
    """gevent.monkey if gevent is installed and importable, else None.

    Shared by pipeline._gevent_patched (execution model) and _gevent_status
    below (Diagnose report) - gevent is an optional third-party dependency,
    not stdlib, so both call sites need the same ImportError guard.
    """
    try:
        from gevent import monkey
    except ImportError:
        return None
    return monkey


def _gevent_status():
    """Decides the execution model. See spec §6."""
    monkey = gevent_monkey()
    if monkey is None:
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
    }


def plan_writes(current, ordered_ids, detach_ids):
    """Turn a channel plan into concrete write operations.

    `current` maps attached stream_id -> current order. Returns attach /
    detach / order operations. An empty `ordered_ids` produces nothing at
    all: a channel that matched nothing is never cleared (spec §12).

    Caller contract: `ordered_ids` must include every stream_id the caller
    wants to remain attached, not just newly matched ones — a currently
    attached stream that is kept but merely omitted from `ordered_ids` is
    neither detached nor given a final order. The candidate-assembly step
    ("newly matched streams + streams already attached") is what is meant
    to guarantee this in practice.
    """
    ordered_ids = list(dict.fromkeys(ordered_ids))
    if not ordered_ids:
        return {"attach": [], "detach": [], "orders": []}
    keep = set(ordered_ids)
    return {
        "attach": [sid for sid in ordered_ids if sid not in current],
        "detach": [sid for sid in detach_ids if sid not in keep],
        "orders": rewrite_plan(ordered_ids),
    }


def apply_channel_plan(resolved, channel, ordered_ids, detach_ids, dry_run):
    """Write one channel's plan. All order changes in a single transaction."""
    # Lazy per the module docstring.
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
        plan = plan_writes(current, ordered_ids, detach_ids)
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

        # Dispatcharr's ChannelStream carries no unique constraint on order
        # (only on channel+stream), so a new row's create-time order value
        # is never checked for collisions. The order pass right after
        # overwrites it unconditionally regardless.
        for stream_id in plan["attach"]:
            link_model.objects.create(
                channel=channel, stream_id=stream_id, **{order_field: 0}
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


def rename_channel(resolved, channel, new_name):
    """Write the broken-marker rename. Single field, so no transaction needed.

    new_name is computed from channel.name as read earlier in this run's
    loop - a concurrent manual rename in the Dispatcharr UI mid-run would
    be overwritten here. Accepted: low odds, and it self-heals next run,
    since channel.name is always re-read fresh from the DB at the top of
    the next select_channels() call.
    """
    resolved.channel_model.objects.filter(id=channel.id).update(name=new_name)


def save_stream_stats(resolved, stream_id, stats):
    """Write probe results using the key names Dispatcharr already uses.

    Merges onto the existing stream_stats dict rather than replacing it, so
    keys Dispatcharr or another plugin populated (width, height, ...) survive
    a Failoverr probe. Plugin-private bookkeeping never goes here — it lives
    in the sidecar.
    """
    # Lazy per the module docstring.
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
