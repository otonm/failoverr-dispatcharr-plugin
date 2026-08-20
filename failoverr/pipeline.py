"""Orchestration: indexing, matching, per-channel planning, reporting.

Django imports are lazy (inside functions). The functions in this first
section touch nothing but plain data so they can be tested offline.
"""

import csv
import datetime
import logging
import pathlib
import threading
import time
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

_BOOL_KEYS = ("dry_run", "map_number_words", "blank_detect", "schedule_enabled")
_FALSE_STRINGS = ("false", "0", "no", "off")


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

    ranked_ids = [c.stream_id for c in ranked(promotable)]
    ranked_ids += [c.stream_id for c in ranked(demoted)]

    if not ranked_ids:
        # Nothing has ever been learned about this channel: leave it
        # completely alone, including any never-probed attached streams —
        # never clear a channel on an empty result.
        return [], []

    never_probed_ids = [c.stream_id for c in ranked(never_probed)]

    # Truncation applies only to streams with a recorded verdict. Never-probed
    # attached streams don't count against the limit and are never detached
    # for it — they just haven't had a chance to earn or lose their place yet.
    kept = ranked_ids[: max(1, int(max_streams))]
    truncated = [sid for sid in ranked_ids[len(kept):] if sid in attached_ids]
    detach.extend(truncated)
    detach.extend(
        sid for sid in attached_ids
        if sid not in ranked_ids and sid not in never_probed_ids and sid not in detach
    )
    return kept + never_probed_ids, detach


def _csv_tuple(raw, fallback):
    parts = tuple(p.strip().lower() for p in str(raw or "").split(",") if p.strip())
    return parts or fallback


def _to_bool(raw):
    """Coerce a boolean setting. Dispatcharr may hand back a string.

    bool("false") is True in plain Python - any non-empty string is
    truthy - so a real Python bool passes through, and a string is judged
    by its content instead.
    """
    if isinstance(raw, str):
        return raw.strip().lower() not in _FALSE_STRINGS
    return bool(raw)


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
    for key in _BOOL_KEYS:
        settings[key] = _to_bool(raw[key])

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
    # datetime.timezone.utc, not datetime.UTC (py3.11+) - the container's
    # Python version is unconfirmed (CLAUDE.md §4).
    stamp = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")  # noqa: UP017
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
        matched_ids = {row.stream_id for row in matched}
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
                "action": "matched - would be probed"
                if row.stream_id in matched_ids else "attached - not matched",
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


# --- Run lock, budgets, background execution --------------------------------

# A stale lock from a killed run must not block the plugin forever.
LOCK_TTL_SECONDS = 1800

_lock_guard = threading.Lock()
_lock_holder = {"holder": None, "since": 0.0}


def acquire_lock(name, now=None):
    now = time.time() if now is None else now
    with _lock_guard:
        holder = _lock_holder["holder"]
        if holder is not None and (now - _lock_holder["since"]) < LOCK_TTL_SECONDS:
            return False
        _lock_holder.update({"holder": name, "since": now})
        return True


def refresh_lock(now=None):
    """Keep a long-running run's lock from going stale mid-run.

    Without this, a run past LOCK_TTL_SECONDS looks abandoned to
    acquire_lock even though it is still working - a second run can then
    steal the lock, and the first run's eventual release_lock() would clear
    the SECOND run's lock too.
    """
    now = time.time() if now is None else now
    with _lock_guard:
        _lock_holder["since"] = now


def release_lock():
    with _lock_guard:
        _lock_holder.update({"holder": None, "since": 0.0})


def clear_lock():
    release_lock()
    return {"status": "ok", "message": "Lock cleared."}


def lock_status():
    with _lock_guard:
        return dict(_lock_holder)


class Budget:
    """Runaway guard, not a normal limit.

    A typical run uses about 200 probes and 15 minutes against defaults
    of 400 and 60.
    """

    def __init__(self, max_probes, max_minutes, now_fn=time.time):
        self.max_probes = max(1, int(max_probes))
        self.max_seconds = max(1, int(max_minutes)) * 60
        self.now_fn = now_fn
        self.started = now_fn()
        self.probes = 0
        self.reason = None

    def allow(self):
        if self.probes >= self.max_probes:
            self.reason = f"probe budget exhausted ({self.max_probes})"
            return False
        if (self.now_fn() - self.started) >= self.max_seconds:
            self.reason = f"time budget exhausted ({self.max_seconds // 60} min)"
            return False
        return True

    def spend(self):
        self.probes += 1


def _gevent_patched():
    """Return whether gevent has monkey-patched subprocess.

    That is the condition under which a blocking subprocess wait in a
    plain thread would stall the whole Dispatcharr worker process, so it
    decides both how work is spawned and what Show Status reports.
    """
    try:
        from gevent import monkey

        return monkey.is_module_patched("subprocess")
    except ImportError:
        return False


def spawn(fn, *args):
    """Run in the background without freezing the Dispatcharr worker."""
    if _gevent_patched():
        from gevent import spawn as gevent_spawn

        return gevent_spawn(fn, *args)
    thread = threading.Thread(target=fn, args=args, daemon=True)
    thread.start()
    return thread


def execution_model():
    return "gevent greenlet" if _gevent_patched() else "daemon thread"


INLINE_CHANNEL_LIMIT = 15


def _notify(payload):
    try:
        from core.utils import send_websocket_update

        send_websocket_update("updates", "update", payload)
    except Exception:  # noqa: BLE001, S110 - notifications must never break a run
        pass


def _probe_candidates(  # noqa: PLR0913, PLR0917 - interface fixed by the task spec
    candidates, state, settings, prober, budget, resolved, log, probed_so_far=0,
):
    """Probe what is stale, record verdicts, write stats. Returns count.

    probed_so_far is the run-wide probe count before this call, so the
    25-probe heartbeat/save cadence accumulates across the whole run rather
    than resetting every time run_pipeline calls this once per channel -
    real channels rarely have 25 candidates each, so a per-call-local
    counter would almost never fire.
    """
    from .probing import is_blank
    from .state import INVALID, VALID

    probed = 0
    for row in candidates:
        if state.is_fresh(row.stream_id, row.url, settings["probe_ttl_hours"]):
            continue
        if row.provider_id in prober.aborted_providers:
            # probe_one already no-ops for an aborted provider; skip the
            # call entirely so its candidates don't burn budget that
            # healthy providers could otherwise use.
            continue
        if not budget.allow():
            log.info("FAILOVERR run stopping early: %s", budget.reason)
            break

        result = prober.probe_one(row.provider_id, row.url)
        budget.spend()
        probed += 1

        verdict, stats = result.verdict, result.stats
        if verdict == VALID and settings["blank_detect"] and is_blank(
            row.url, settings["ffmpeg_path"], settings["blank_detect_seconds"]
        ):
            verdict, stats = INVALID, {}

        state.record(row.stream_id, row.url, verdict)
        if verdict == VALID and stats:
            models_access_save(resolved, row.stream_id, stats)
        if (probed_so_far + probed) % 25 == 0:
            refresh_lock()
            state.save()
            _notify({"type": "failoverr", "probed": probed_so_far + probed})
    return probed


def models_access_save(resolved, stream_id, stats):
    from . import models_access

    models_access.save_stream_stats(resolved, stream_id, stats)


def _close_old_connections():
    """Drop Django DB connections past CONN_MAX_AGE.

    run_pipeline runs outside Django's request/response cycle (a background
    thread/greenlet, or the scheduler thread), so the usual per-request
    cleanup signal never fires here - do it explicitly instead.
    """
    from django.db import close_old_connections

    close_old_connections()


def _close_connection():
    """Release this thread's Django DB connection once a run is done.

    Same reasoning as _close_old_connections: nothing else closes it for a
    thread/greenlet that never went through a Django request.
    """
    from django.db import connection

    connection.close()


def run_pipeline(context, mode="run"):
    """Single entry point for Run, Reorder Only, and Probe Only."""
    from . import models_access
    from .probing import Prober

    log = context.get("logger", logger)
    settings = load_settings(context)
    _close_old_connections()
    resolved = models_access.resolve_models()
    state = State.load(STATE_PATH)
    budget = Budget(settings["max_probes_per_run"], settings["max_run_minutes"])

    channels = select_channels(resolved, settings)
    index = (
        {} if mode != "run"
        else build_index(iter_pool(resolved, settings))
    )
    prober = Prober(
        settings["ffprobe_path"], settings["probe_timeout_seconds"],
        settings["per_account_concurrency"], settings["global_concurrency"],
        settings["account_cooldown_seconds"],
    )

    rows = []
    totals = {"attached": 0, "detached": 0, "probed": 0, "channels": 0}

    for channel in channels:
        tokens = normalize(
            channel.name or "",
            strip_tokens=settings["strip_tokens"],
            map_number_words=settings["map_number_words"],
        )
        matched = (
            [] if mode != "run"
            else find_matches(tokens, index, settings["match_mode"],
                              settings["fuzzy_threshold"])
        )
        current = attached_rows(resolved, channel)
        attached_ids = {stream_id for stream_id, _ in current}

        by_id = {row.stream_id: row for row in matched}
        for row in iter_attached_rows(resolved, channel, settings):
            by_id.setdefault(row.stream_id, row)
        candidates = list(by_id.values())

        if mode != "reorder_only":
            totals["probed"] += _probe_candidates(
                candidates, state, settings, prober, budget, resolved, log,
                probed_so_far=totals["probed"],
            )

        if mode == "probe_only":
            totals["channels"] += 1
            continue

        ordered, detach = plan_channel(
            attached_ids, candidates, state,
            settings["removal_failure_threshold"],
            settings["max_streams_per_channel"],
            settings["order_strategy"], settings["codec_priority"],
        )
        if mode == "reorder_only":
            # Reorder Only never detaches. plan_channel's removal branch
            # reads a cross-run failure counter that may have accumulated
            # over prior Probe Only runs; a truncated/failed attached
            # stream just keeps its old order and stays attached instead.
            detach = []
        if not ordered:
            log.info("FAILOVERR %s: %s matched nothing, left alone",
                     mode, channel.name)
            continue

        summary = models_access.apply_channel_plan(
            resolved, channel, ordered, detach, settings["dry_run"]
        )
        totals["attached"] += summary["attached"]
        totals["detached"] += summary["detached"]
        totals["channels"] += 1

        lookup = {row.stream_id: row for row in candidates}
        for position, stream_id in enumerate(ordered):
            row = lookup.get(stream_id)
            rows.append({
                "channel": channel.name, "position": position,
                "stream": row.name if row else stream_id,
                "provider": row.provider_id if row else "",
                "verdict": state.last_verdict(stream_id) or "unprobed",
                "resolution": (row.stats or {}).get("resolution", "") if row else "",
                "codec": (row.stats or {}).get("video_codec", "") if row else "",
                "action": "keep" if stream_id in attached_ids else "attach",
            })
        rows.extend({
            "channel": channel.name, "position": "",
            "stream": stream_id, "provider": "",
            "verdict": state.last_verdict(stream_id) or "unprobed",
            "resolution": "", "codec": "", "action": "detach",
        } for stream_id in detach)

    state.meta.update({
        "last_run": time.time(),
        "last_mode": mode,
        "degraded_providers": sorted(str(p) for p in prober.aborted_providers),
        "budget_stop": budget.reason,
    })
    state.save()

    path = write_report(rows, report_path(mode))
    degraded = " DEGRADED" if prober.aborted_providers else ""
    log.info(
        "FAILOVERR %s COMPLETED%s: %s channels, %s probed, %s attached, "
        "%s detached, dry_run=%s, report %s",
        mode, degraded, totals["channels"], totals["probed"],
        totals["attached"], totals["detached"], settings["dry_run"], path,
    )
    _notify({"type": "failoverr", "status": "completed", **totals})
    return {"status": "ok", "report": path, "degraded_providers":
            sorted(str(p) for p in prober.aborted_providers), **totals}


def start(context, mode="run"):
    """Acquire the lock and run, inline for small jobs, backgrounded otherwise."""
    log = context.get("logger", logger)
    if not acquire_lock(mode):
        held = lock_status()["holder"]
        return {
            "status": "error",
            "message": (
                f"A {held} operation is already running. Wait for it to "
                f"finish, or use Clear Lock if it is stuck."
            ),
        }

    settings = load_settings(context)
    try:
        from . import models_access

        resolved = models_access.resolve_models()
        channel_count = len(select_channels(resolved, settings))
    except Exception as exc:
        release_lock()
        log.exception("FAILOVERR %s FAILED", mode)
        return {"status": "error", "message": str(exc)}

    if channel_count <= INLINE_CHANNEL_LIMIT and mode == "reorder_only":
        try:
            return run_pipeline(context, mode)
        finally:
            release_lock()
            _close_connection()

    def background():
        try:
            run_pipeline(context, mode)
        except Exception:  # a background run must never crash silently
            log.exception("FAILOVERR %s FAILED", mode)
        finally:
            release_lock()
            _close_connection()

    spawn(background)
    return {
        "status": "started",
        "channels": channel_count,
        "dry_run": settings["dry_run"],
        "message": (
            f"Started {mode} over {channel_count} channels in the background "
            f"({execution_model()}). dry_run is "
            f"{'ON - nothing will be changed' if settings['dry_run'] else 'OFF'}. "
            f"Use Show Status to follow along."
        ),
    }
