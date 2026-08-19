"""Orchestration: indexing, matching, per-channel planning, reporting.

Django imports are lazy (inside functions). The functions in this first
section touch nothing but plain data so they can be tested offline.
"""

import csv
import datetime
import logging
import pathlib
from collections import defaultdict
from typing import Any, NamedTuple

from .naming import DEFAULT_STRIP_TOKENS, normalize
from .naming import matches as name_matches
from .ordering import DEFAULT_CODEC_PRIORITY, Candidate, order_candidates
from .state import DEFAULT_PATH as STATE_PATH
from .state import INCONCLUSIVE, INVALID, VALID, State

logger = logging.getLogger("failoverr")

EXPORT_DIR = "/data/exports"
REPORT_COLUMNS = [
    "channel", "position", "stream", "provider", "verdict",
    "resolution", "codec", "action",
]

_DEFAULTS = {
    "dry_run": True,
    "ffprobe_path": "/usr/local/bin/ffprobe",
    "ffmpeg_path": "/usr/local/bin/ffmpeg",
    "probe_timeout_seconds": 15,
    "channel_group": "",
    "match_mode": "strict",
    "fuzzy_threshold": 85,
    "map_number_words": True,
    "order_strategy": "quality_first",
    "max_streams_per_channel": 10,
    "probe_ttl_hours": 24,
    "per_account_concurrency": 1,
    "account_cooldown_seconds": 2,
    "global_concurrency": 4,
    "removal_failure_threshold": 3,
    "blank_detect": False,
    "blank_detect_seconds": 5,
    "max_probes_per_run": 400,
    "max_run_minutes": 60,
    "schedule_enabled": False,
    "cron_expression": "0 4 * * *",
    "timezone": "UTC",
}

_INT_KEYS = (
    "probe_timeout_seconds", "fuzzy_threshold", "max_streams_per_channel",
    "probe_ttl_hours", "per_account_concurrency", "account_cooldown_seconds",
    "global_concurrency", "removal_failure_threshold", "blank_detect_seconds",
    "max_probes_per_run", "max_run_minutes",
)


class StreamRow(NamedTuple):
    stream_id: int
    name: str
    provider_id: Any
    url: str
    stats: dict
    tokens: tuple


def build_index(rows):
    """Group streams by normalized token set, in one pass over the pool."""
    index = defaultdict(list)
    for row in rows:
        if row.tokens:
            index[frozenset(row.tokens)].append(row)
    return dict(index)


def find_matches(channel_tokens, index, mode="strict", threshold=85):
    """Streams belonging to this channel.

    Strict mode is an O(1) lookup. Fuzzy scans distinct token sets rather
    than individual streams, which keeps it tractable on a large pool.
    """
    if not channel_tokens:
        return []
    if mode == "strict":
        return list(index.get(frozenset(channel_tokens), []))

    found = []
    for rows in index.values():
        if rows and name_matches(
            channel_tokens, rows[0].tokens, mode="fuzzy", threshold=threshold
        ):
            found.extend(rows)
    return found


def plan_channel(  # noqa: PLR0913, PLR0917 - interface fixed by the task spec
    attached_ids, candidates, state, threshold, max_streams,
    strategy, codec_priority,
):
    """Decide this channel's final stream list.

    Returns (ordered_stream_ids, detach_ids).

    Rules, all from spec §12:
      - only confirmed-valid streams are ever newly attached;
      - an attached stream whose probe was inconclusive keeps its place;
      - an attached stream that was never probed at all also keeps its
        place, but only once something else about this channel is known —
        if literally nothing has ever been probed, the channel is left
        completely alone rather than being rewritten into its own order;
      - an attached stream that failed, but not `threshold` times in a row,
        is demoted to the bottom rather than removed;
      - a channel whose plan comes out empty is left completely alone.
    """
    attached_ids = set(attached_ids)
    detach = []
    promotable = []
    demoted = []
    never_probed = []

    for candidate in candidates:
        verdict = state.last_verdict(candidate.stream_id)
        is_attached = candidate.stream_id in attached_ids

        if is_attached and verdict == INVALID and state.should_remove(
            candidate.stream_id, threshold
        ):
            detach.append(candidate.stream_id)
        elif verdict == VALID or (is_attached and verdict == INCONCLUSIVE):
            promotable.append(candidate)
        elif is_attached and verdict == INVALID:
            demoted.append(candidate)
        elif is_attached and verdict is None:
            never_probed.append(candidate)
        # Unattached and not confirmed valid: never attach it.

    def ranked(items):
        return order_candidates(
            [
                Candidate(c.stream_id, c.name, c.provider_id, c.stats)
                for c in items
            ],
            strategy=strategy,
            codec_priority=codec_priority,
        )

    ordered = [c.stream_id for c in ranked(promotable)]
    ordered += [c.stream_id for c in ranked(demoted)]

    if not ordered:
        # Nothing has ever been learned about this channel: leave it
        # completely alone, including any never-probed attached streams —
        # never clear a channel on an empty result.
        return [], []

    ordered += [c.stream_id for c in ranked(never_probed)]

    kept = ordered[: max(1, int(max_streams))]
    truncated = [sid for sid in ordered[len(kept):] if sid in attached_ids]
    detach.extend(truncated)
    detach.extend(
        sid for sid in attached_ids if sid not in ordered and sid not in detach
    )
    return kept, detach


def _csv_tuple(raw, fallback):
    parts = tuple(p.strip().lower() for p in str(raw or "").split(",") if p.strip())
    return parts or fallback


def load_settings(context):
    """Typed settings with defaults. Dispatcharr may hand back strings."""
    raw = dict(_DEFAULTS)
    raw.update({k: v for k, v in (context.get("settings") or {}).items()
                if v is not None})

    settings = dict(raw)
    for key in _INT_KEYS:
        try:
            settings[key] = int(float(raw[key]))
        except (TypeError, ValueError):
            settings[key] = _DEFAULTS[key]
    for key in ("dry_run", "map_number_words", "blank_detect", "schedule_enabled"):
        settings[key] = bool(raw[key])

    settings["strip_tokens"] = _csv_tuple(
        raw.get("strip_tokens"), DEFAULT_STRIP_TOKENS
    )
    settings["codec_priority"] = _csv_tuple(
        raw.get("codec_priority"), DEFAULT_CODEC_PRIORITY
    )
    settings["channel_names"] = [
        line.strip()
        for line in str(raw.get("channel_names") or "").splitlines()
        if line.strip()
    ]
    return settings


def write_report(rows, path):
    path = pathlib.Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in REPORT_COLUMNS})
    return str(path)


def report_path(action):
    stamp = datetime.datetime.now(tz=datetime.UTC).strftime("%Y%m%d-%H%M%S")
    return pathlib.Path(EXPORT_DIR) / f"failoverr-{action}-{stamp}.csv"


def iter_pool(resolved, settings):
    """One streaming pass over the whole Stream pool.

    select_related on the provider FK avoids an N+1; .iterator() keeps a
    100k-row pool out of memory.
    """
    queryset = (
        resolved.stream_model.objects.select_related(resolved.provider_field)
        .only("id", "name", "url", "stream_stats", resolved.provider_field)
        .iterator(chunk_size=2000)
    )
    for stream in queryset:
        provider = getattr(stream, f"{resolved.provider_field}_id", None)
        yield StreamRow(
            stream_id=stream.id,
            name=stream.name or "",
            provider_id=provider,
            url=stream.url or "",
            stats=stream.stream_stats if isinstance(stream.stream_stats, dict) else {},
            tokens=normalize(
                stream.name or "",
                strip_tokens=settings["strip_tokens"],
                map_number_words=settings["map_number_words"],
            ),
        )


def select_channels(resolved, settings):
    queryset = resolved.channel_model.objects.all()
    if settings["channel_names"]:
        queryset = queryset.filter(name__in=settings["channel_names"])
    elif settings["channel_group"]:
        queryset = queryset.filter(channel_group__name=settings["channel_group"])
    return list(queryset)


def attached_rows(resolved, channel):
    """Return the currently attached streams, in their present failover order."""
    links = (
        resolved.channel_stream_model.objects.filter(channel=channel)
        .select_related("stream")
        .order_by(resolved.order_field)
    )
    return [(link.stream_id, getattr(link, resolved.order_field)) for link in links]


def iter_attached_rows(resolved, channel, settings):
    """StreamRows for the streams already attached to this channel.

    An attached stream that no longer matches its channel must still reach
    the candidate set, or it would silently escape evaluation.
    """
    links = (
        resolved.channel_stream_model.objects.filter(channel=channel)
        .select_related("stream")
        .order_by(resolved.order_field)
    )
    for link in links:
        stream = link.stream
        yield StreamRow(
            stream_id=stream.id,
            name=stream.name or "",
            provider_id=getattr(stream, f"{resolved.provider_field}_id", None),
            url=stream.url or "",
            stats=stream.stream_stats if isinstance(stream.stream_stats, dict) else {},
            tokens=normalize(stream.name or "",
                             strip_tokens=settings["strip_tokens"],
                             map_number_words=settings["map_number_words"]),
        )


def run_preview(context):
    """Read-only. Match and order from cached probe data, write a CSV."""
    from . import models_access

    log = context.get("logger", logger)
    settings = load_settings(context)
    resolved = models_access.resolve_models()
    state = State.load(STATE_PATH)

    index = build_index(iter_pool(resolved, settings))
    channels = select_channels(resolved, settings)

    rows = []
    for channel in channels:
        tokens = normalize(
            channel.name or "",
            strip_tokens=settings["strip_tokens"],
            map_number_words=settings["map_number_words"],
        )
        matched = find_matches(
            tokens, index,
            mode=settings["match_mode"],
            threshold=settings["fuzzy_threshold"],
        )
        current = attached_rows(resolved, channel)
        attached_ids = {stream_id for stream_id, _ in current}
        by_id = {row.stream_id: row for row in matched}
        for row in iter_attached_rows(resolved, channel, settings):
            by_id.setdefault(row.stream_id, row)
        candidates = list(by_id.values())

        ordered, detach = plan_channel(
            attached_ids, candidates, state,
            settings["removal_failure_threshold"],
            settings["max_streams_per_channel"],
            settings["order_strategy"],
            settings["codec_priority"],
        )

        lookup = {row.stream_id: row for row in candidates}
        for position, stream_id in enumerate(ordered):
            row = lookup.get(stream_id)
            rows.append({
                "channel": channel.name,
                "position": position,
                "stream": row.name if row else stream_id,
                "provider": row.provider_id if row else "",
                "verdict": state.last_verdict(stream_id) or "unprobed",
                "resolution": (row.stats or {}).get("resolution", "") if row else "",
                "codec": (row.stats or {}).get("video_codec", "") if row else "",
                "action": "keep" if stream_id in attached_ids else "attach",
            })
        for stream_id in detach:
            row = lookup.get(stream_id)
            rows.append({
                "channel": channel.name,
                "position": "",
                "stream": row.name if row else stream_id,
                "provider": row.provider_id if row else "",
                "verdict": state.last_verdict(stream_id) or "unprobed",
                "resolution": "", "codec": "", "action": "detach",
            })

        # Everything matched but not acted on. Until probing exists, this is
        # the whole report: it is what lets the user check that matching
        # found the right streams before an hour of probing is spent on them.
        planned = set(ordered) | set(detach)
        for row in candidates:
            if row.stream_id in planned:
                continue
            rows.append({
                "channel": channel.name,
                "position": "",
                "stream": row.name,
                "provider": row.provider_id,
                "verdict": state.last_verdict(row.stream_id) or "unprobed",
                "resolution": (row.stats or {}).get("resolution", ""),
                "codec": (row.stats or {}).get("video_codec", ""),
                "action": "matched - would be probed",
            })

        if not candidates:
            log.info("FAILOVERR preview: %s matched nothing", channel.name)

    path = write_report(rows, report_path("preview"))
    return {
        "status": "ok",
        "channels": len(channels),
        "rows": len(rows),
        "report": path,
        "message": (
            f"Previewed {len(channels)} channels. Nothing was changed. "
            f"Report: {path}"
        ),
    }
