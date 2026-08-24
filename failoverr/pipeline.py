"""Orchestration: indexing, matching, per-channel planning, reporting.

Actual django.*/apps.* calls are lazy (inside the functions that make
them) so a bare pytest run never needs Django installed. models_access and
probing are imported at module level below despite touching Django/ffprobe
at call time: neither module itself has a top-level Django import (each is
lazy the same way, one level down), so importing the module here costs
nothing a bare pytest run can't afford.

`probing` is imported as a module and called as `probing.is_blank(...)`,
not `from .probing import is_blank`, so a test's
`monkeypatch.setattr(probing, "is_blank", ...)` takes effect here - a
direct name import would bind the original function once at import time
and never see the replacement. `Prober` doesn't need that: tests patch
its `probe_one` method on the class itself, and a class is the same
object no matter which name it's imported under.
"""

import concurrent.futures
import contextlib
import csv
import datetime
import fcntl
import json
import logging
import os
import pathlib
import threading
import time
from collections import defaultdict
from typing import NamedTuple

from . import models_access, probing
from .naming import DEFAULT_STRIP_TOKENS, normalize
from .naming import matches as name_matches
from .ordering import (
    DEFAULT_CODEC_PRIORITY,
    DEFAULT_RESPONSE_TIME_BUCKET_MS,
    Candidate,
    order_candidates,
)
from .plugin import Plugin
from .probing import Prober
from .state import DEFAULT_PATH as STATE_PATH
from .state import INCONCLUSIVE, INVALID, VALID, State

logger = logging.getLogger("failoverr")

EXPORT_DIR = "/data/exports"
_REPORT_COLUMNS = [
    "channel", "position", "stream", "provider", "verdict",
    "resolution", "codec", "response_time_ms", "action",
]

# Most recent reports to keep in EXPORT_DIR; older ones are pruned after each
# write so an unattended cron deployment (one run/day by default) can't grow
# the exports directory without bound.
_MAX_REPORTS = 100

# strip_tokens/codec_priority/channel_names are excluded: load_settings
# parses them itself (_csv_tuple / splitlines) and must fall through to
# naming.py's/ordering.py's own constants, not a re-parsed copy of the
# field's string default - see load_settings below.
# ponytail: pipeline reads Plugin.fields directly; extract a settings.py
# if this dependency inversion (entry point -> core) ever bites.
_TEXT_LIST_KEYS = frozenset({"strip_tokens", "codec_priority", "channel_names"})

_DEFAULTS = {
    f["id"]: f["default"]
    for f in Plugin.fields
    if f["type"] != "info" and f["id"] not in _TEXT_LIST_KEYS
}
_INT_KEYS = tuple(f["id"] for f in Plugin.fields if f["type"] == "number")
_BOOL_KEYS = tuple(f["id"] for f in Plugin.fields if f["type"] == "boolean")
_FALSE_STRINGS = ("false", "0", "no", "off")


class StreamRow(NamedTuple):
    stream_id: int
    name: str
    provider_id: int | None
    url: str
    stats: dict
    tokens: tuple


def _stream_to_row(stream, resolved, settings):
    """Convert a Django Stream model instance to a StreamRow.

    Shared by iter_pool and iter_attached_rows so the construction logic
    (isinstance guard + normalize call) exists in one place and cannot drift.
    """
    provider = getattr(stream, f"{resolved.provider_field}_id", None)
    return StreamRow(
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
    strategy, codec_priority, response_time_bucket_ms=DEFAULT_RESPONSE_TIME_BUCKET_MS,
    rank_by_bitrate=True,
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
                Candidate(c.stream_id, c.name, c.provider_id, c.stats,
                          state.response_time_ms(c.stream_id))
                for c in items
            ],
            strategy=strategy,
            codec_priority=codec_priority,
            response_time_bucket_ms=response_time_bucket_ms,
            rank_by_bitrate=rank_by_bitrate,
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
        except (TypeError, ValueError, OverflowError):
            logger.warning(
                "FAILOVERR setting %r=%r is not a valid number; "
                "using default %r",
                key, raw[key], _DEFAULTS[key],
            )
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
        writer = csv.DictWriter(handle, fieldnames=_REPORT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in _REPORT_COLUMNS})
    _rotate_reports(path.parent)
    return str(path)


def _rotate_reports(directory, pattern="failoverr-*.csv", keep=_MAX_REPORTS):
    """Prune old exports so EXPORT_DIR can't grow without bound.

    Best-effort: a stat/unlink failure (permissions, a concurrent cleaner)
    must never break a run - the report was already written, so the worst
    case is a few stale files lingering, not a crashed run. Only files
    matching the report pattern are touched, so unrelated files in the
    directory (or test fixtures under a monkeypatched path) survive.
    """
    directory = pathlib.Path(directory)
    if not directory.is_dir():
        return
    try:
        reports = sorted(
            directory.glob(pattern),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except OSError as exc:
        logger.debug(
            "FAILOVERR report rotation: failed to list directory %s: %s",
            directory, exc,
        )
        return
    for stale in reports[keep:]:
        try:
            stale.unlink(missing_ok=True)
            logger.debug("FAILOVERR report rotation: pruned %s", stale)
        except OSError as exc:
            logger.debug(
                "FAILOVERR report rotation: failed to prune %s: %s", stale, exc
            )


def report_path(action):
    # datetime.timezone.utc, not datetime.UTC (py3.11+) - the container's
    # Python version is unconfirmed (CLAUDE.md §4).
    stamp = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")  # noqa: UP017
    return pathlib.Path(EXPORT_DIR) / f"failoverr-{action}-{stamp}.csv"


def _report_row(channel, position, stream_id, row, state, action):  # noqa: PLR0913, PLR0917
    """One CSV report row. Resolution/codec/response_time blank on a detach.

    The single source of the report-row shape: run_preview and run_pipeline
    both build their rows here, so a column rendering fix (e.g. the 0ms
    ``is not None`` check) applies once instead of drifting across sites.
    """
    detach = action == "detach"
    stats = (row.stats or {}) if row and not detach else {}
    return {
        "channel": channel.name,
        "position": position,
        "stream": row.name if row else stream_id,
        "provider": row.provider_id if row else "",
        "verdict": state.last_verdict(stream_id) or "unprobed",
        "resolution": stats.get("resolution", ""),
        "codec": stats.get("video_codec", ""),
        "response_time_ms": (
            "" if detach
            else rt if (rt := state.response_time_ms(stream_id)) is not None else ""
        ),
        "action": action,
    }


def _build_channel_report_rows(  # noqa: PLR0913, PLR0917
    channel, ordered, detach, candidates, attached_ids, state
):
    """Build report rows for a single channel from its plan.

    Centralizes the report-row logic shared by run_preview and run_pipeline
    so the two never drift apart (e.g. Preview passing the StreamRow for
    detach while Run passed None).
    """
    rows = []
    lookup = {row.stream_id: row for row in candidates}
    for position, stream_id in enumerate(ordered):
        row = lookup.get(stream_id)
        rows.append(_report_row(
            channel, position, stream_id, row, state,
            "keep" if stream_id in attached_ids else "attach",
        ))
    for stream_id in detach:
        row = lookup.get(stream_id)
        rows.append(_report_row(channel, "", stream_id, row, state, "detach"))
    return rows


def _plan_channel_args(settings):
    """Extract the plan_channel arguments from settings.

    Shared by run_preview and run_pipeline so the call cannot drift between
    the two sites.
    """
    return (
        settings["removal_failure_threshold"],
        settings["max_streams_per_channel"],
        settings["order_strategy"], settings["codec_priority"],
        settings["response_time_bucket_ms"],
        settings["rank_by_bitrate"],
    )


def iter_pool(resolved, settings):
    """One streaming pass over the whole Stream pool.

    We only need the provider_id FK column from the stream row, not the
    joined provider object. Using only() without select_related avoids the
    unnecessary JOIN on a 100k-row scan. .iterator() keeps memory bounded.
    """
    queryset = (
        resolved.stream_model.objects
        .only("id", "name", "url", "stream_stats", f"{resolved.provider_field}_id")
        .iterator(chunk_size=2000)
    )
    for stream in queryset:
        yield _stream_to_row(stream, resolved, settings)


def select_channels(resolved, settings):
    queryset = resolved.channel_model.objects.all()
    if settings["channel_names"]:
        queryset = queryset.filter(name__in=settings["channel_names"])
    elif settings["channel_group"]:
        queryset = queryset.filter(channel_group__name=settings["channel_group"])
    return list(queryset)


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
        yield _stream_to_row(link.stream, resolved, settings)


def run_preview(context):
    """Read-only. Match and order from cached probe data, write a CSV."""
    log = context.get("logger") or logger
    settings = load_settings(context)
    resolved = models_access.resolve_models()
    state = State.load(STATE_PATH)

    index = build_index(iter_pool(resolved, settings))
    channels = select_channels(resolved, settings)

    rows = []
    for channel in channels:
        matched, attached_ids, candidates = _channel_candidates(
            "run", channel, index, resolved, settings,
        )
        matched_ids = {row.stream_id for row in matched}

        ordered, detach = plan_channel(
            attached_ids, candidates, state,
            *_plan_channel_args(settings),
        )

        rows.extend(_build_channel_report_rows(
            channel, ordered, detach, candidates, attached_ids, state
        ))

        # Everything matched but not acted on. Until probing exists, this is
        # the whole report: it is what lets the user check that matching
        # found the right streams before an hour of probing is spent on them.
        planned = set(ordered) | set(detach)
        for row in candidates:
            if row.stream_id in planned:
                continue
            rows.append(_report_row(
                channel, "", row.stream_id, row, state,
                "matched - would be probed"
                if row.stream_id in matched_ids else "attached - not matched",
            ))

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
_LOCK_TTL_SECONDS = 1800

# A file, not an in-memory dict: a scheduled run fires inside the celery
# worker process while a manual Run fires inside the uwsgi process - two
# separate OS processes that share no Python memory. flock() around every
# read-modify-write below is what actually keeps them from stepping on
# each other; the file's content is just {"holder": ..., "since": ...}.
_LOCK_PATH = "/data/failoverr/run.lock"

# Presence-only flag, checked by the running job itself at its next
# checkpoint (Budget.allow(), the same spot the probe/time budgets are
# checked). A separate process (Clear Lock, pressed from the UI) can only
# ask the run to stop cooperatively - it has no way to kill the thread or
# greenlet actually doing the work.
_CANCEL_PATH = "/data/failoverr/cancel.flag"


def _open_lock_file():
    path = pathlib.Path(_LOCK_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.touch()
    return path.open("r+")


def _read_locked(fh):
    fh.seek(0)
    raw = fh.read()
    try:
        return json.loads(raw) if raw else {}
    except ValueError as exc:
        logger.warning("FAILOVERR lock file corrupt (%s); treating as empty", exc)
        return {}


def _write_locked(fh, data):
    # Write first, then truncate: if a crash occurs mid-write, the file
    # contains the new data (possibly with old tail) rather than being
    # left empty. json.loads will fail on the old tail and _read_locked
    # will log the corruption and treat it as empty, which is safe.
    payload = json.dumps(data)
    fh.seek(0)
    fh.write(payload)
    fh.truncate()
    fh.flush()
    os.fsync(fh.fileno())


@contextlib.contextmanager
def _lock_file(exclusive=True):
    """Context manager for lock file access.

    Args:
        exclusive: If True, acquire LOCK_EX (write); else LOCK_SH (read).

    """
    fh = _open_lock_file()
    try:
        fcntl.flock(fh, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield fh
    finally:
        fcntl.flock(fh, fcntl.LOCK_UN)
        fh.close()


def acquire_lock(name, now=None, ttl=_LOCK_TTL_SECONDS):
    now = time.time() if now is None else now
    with _lock_file() as fh:
        data = _read_locked(fh)
        holder, since = data.get("holder"), float(data.get("since", 0.0))
        if holder is not None and (now - since) < ttl:
            logger.debug(
                "FAILOVERR acquire_lock: refused for %r "
                "(held by %r, age %ss < ttl %ss)",
                name, holder, int(now - since), ttl,
            )
            return False
        if holder is not None:
            logger.warning(
                "FAILOVERR lock %r stale (age %ss > ttl %ss); taken by %r",
                holder, int(now - since), ttl, name,
            )
        else:
            logger.debug("FAILOVERR acquire_lock: acquired by %r (was free)", name)
        _write_locked(fh, {"holder": name, "since": now})
        return True


def _bump_lock(holder, now=None, progress=None):
    """Shared read-check-write body for update_progress().

    Bumps "since" (and "progress" when given) if this process still owns the
    lock - i.e. holder is None (unconditional) or matches the current
    holder. Returns (wrote, actual_holder) so the caller can log its own
    message.
    """
    now = time.time() if now is None else now
    with _lock_file() as fh:
        data = _read_locked(fh)
        actual_holder = data.get("holder")
        if holder is not None and actual_holder != holder:
            return False, actual_holder
        data["since"] = now
        if progress is not None:
            data["progress"] = progress
        _write_locked(fh, data)
        return True, actual_holder


def release_lock(holder=None):
    """Release the run lock.

    ``holder`` (the mode that acquired it) makes the release conditional:
    if the lock has since been force-cleared and re-acquired by a different
    run, this no-ops rather than wiping the new run's lock - the same
    two-runs-writing-state-at-once hazard update_progress guards against.
    ``holder=None`` (the default, used by clear_lock and tests) is the
    unconditional force-release path.
    """
    with _lock_file() as fh:
        data = _read_locked(fh)
        if holder is not None and data.get("holder") != holder:
            logger.debug(
                "FAILOVERR release_lock: no-op for %r (lock held by %r)",
                holder, data.get("holder"),
            )
            return
        _write_locked(fh, {"holder": None, "since": 0.0})
        logger.debug("FAILOVERR release_lock: released by %r", holder or "any")


def update_progress(holder, now=None, **fields):
    """Publish live progress into the lock file for Show Status to read.

    Cheap, best-effort, and self-correcting: if the lock has since been
    released or stolen by someone else, this silently does nothing rather
    than resurrecting a finished/replaced lock with stale progress. Also
    doubles as a plain heartbeat (bumps "since") when called with no
    fields, e.g. ``update_progress(mode)`` - that call leaves "progress"
    untouched rather than clobbering it with an empty dict, which is what
    lets this same function cover both jobs.

    ``holder=None`` (used by tests and the force/clear path) is
    unconditional - it bumps whichever lock is present.
    """
    wrote, actual_holder = _bump_lock(holder, now=now, progress=fields or None)
    if wrote:
        logger.debug("FAILOVERR update_progress: updated for %r", holder)
    else:
        logger.debug(
            "FAILOVERR update_progress: no-op for %r (lock held by %r)",
            holder, actual_holder,
        )


def request_cancel():
    path = pathlib.Path(_CANCEL_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


def cancel_requested():
    return pathlib.Path(_CANCEL_PATH).exists()


def _clear_cancel():
    pathlib.Path(_CANCEL_PATH).unlink(missing_ok=True)


def stop_run():
    """Ask whichever job holds the lock to stop at its next checkpoint.

    Cooperative only - there is no way to kill the thread/greenlet actually
    doing the work, so a probe already in flight still finishes. The run
    exits through its own CANCELED path (Budget.allow(), pipeline.py) and
    releases its own lock once it next checks in.
    """
    status = lock_status()
    if not status["holder"]:
        return {"status": "ok", "message": "Nothing is running."}
    request_cancel()
    return {
        "status": "ok",
        "message": (
            f"Stop requested for the running {status['holder']} job. It "
            "will finish its current probe, then stop and release its "
            "lock - check Show Status to watch it wind down."
        ),
    }


def _lock_is_active(now=None):
    """Check if the lock is held by an active (non-stale) run.

    Returns the holder name if active, None if free or stale.
    """
    now = time.time() if now is None else now
    with _lock_file() as fh:
        data = _read_locked(fh)
        holder, since = data.get("holder"), float(data.get("since", 0.0))
        if holder is not None and (now - since) < _LOCK_TTL_SECONDS:
            return holder
    return None


def clear_lock(now=None):
    """Force-release a lock left behind by a crashed run.

    Only ever acts on a lock that is stale (past LOCK_TTL_SECONDS since its
    last heartbeat) - a lock more recent than that is presumed to belong to
    a run that is genuinely still working, and force-releasing it would let
    a second run start while the first is still probing: two runs writing
    state.json/attaching streams at once. Use Stop to interrupt a run that
    is still active; this is only the escape hatch for one that is not.
    """
    now = time.time() if now is None else now
    active_holder = _lock_is_active(now)
    if active_holder:
        return {
            "status": "error",
            "message": (
                f"The {active_holder} lock is still recent and looks "
                "like a run that's genuinely active - Clear Lock only "
                "force-releases a stale lock, to avoid letting a second "
                "run start while the first is still working. Use Stop to "
                "interrupt it instead."
            ),
        }
    with _lock_file() as fh:
        data = _read_locked(fh)
        holder = data.get("holder")
        if holder is None:
            return {"status": "ok", "message": "Lock already clear."}
        _write_locked(fh, {"holder": None, "since": 0.0})
        _clear_cancel()
        return {"status": "ok", "message": "Lock cleared."}


def lock_status():
    with _lock_file(exclusive=False) as fh:
        data = _read_locked(fh)
    result = {
        "holder": data.get("holder"),
        "since": float(data.get("since", 0.0)),
        "progress": data.get("progress") or {},
    }
    logger.debug(
        "FAILOVERR lock_status: holder=%r since=%.0f",
        result["holder"], result["since"],
    )
    return result


def clear_state():
    """Reset the probe cache: cached verdicts, URL hashes, failure counters.

    Refused while a run genuinely holds the lock (not merely a stale one) -
    that run has its own State instance in memory and would overwrite this
    reset with its own data on its next periodic save(), silently undoing
    the clear.
    """
    active_holder = _lock_is_active()
    if active_holder:
        return {
            "status": "error",
            "message": (
                f"A {active_holder} operation is in progress and holds "
                "the probe cache in memory - clearing it now would be "
                "silently undone by that run's next save. Wait for it to "
                "finish, or use Stop to interrupt it first."
            ),
        }
    State(STATE_PATH).save()
    return {
        "status": "ok",
        "message": (
            "Probe cache cleared. Every stream - matched and already "
            "attached - will be treated as unprobed and re-checked from "
            "scratch on the next run."
        ),
    }


class Budget:
    """Runaway guard, not a normal limit.

    A typical run uses about 200 probes and 15 minutes against defaults
    of 400 and 60.
    """

    def __init__(self, max_probes, max_minutes, now_fn=time.time, cancel_fn=None):
        self.max_probes = max(1, int(max_probes))
        self.max_seconds = max(1, int(max_minutes)) * 60
        self.now_fn = now_fn
        self.cancel_fn = cancel_fn or (lambda: False)
        self.started = now_fn()
        self.probes = 0
        self.reason = None
        self.canceled = False

    def allow(self):
        if self.cancel_fn():
            self.canceled = True
            self.reason = "canceled by user"
            return False
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
        # gevent is an optional third-party dependency, not stdlib - it may
        # not be installed at all outside the live Dispatcharr container.
        from gevent import monkey

        return monkey.is_module_patched("subprocess")
    except ImportError:
        return False


def spawn(fn, *args):
    """Run in the background without freezing the Dispatcharr worker."""
    if _gevent_patched():
        # Only reachable once _gevent_patched() has already confirmed
        # gevent is importable, so no ImportError guard is needed here.
        from gevent import spawn as gevent_spawn

        return gevent_spawn(fn, *args)
    thread = threading.Thread(target=fn, args=args, daemon=True)
    thread.start()
    return thread


def execution_model():
    return "gevent greenlet" if _gevent_patched() else "daemon thread"


def _select_probe_batch(  # noqa: PLR0913, PLR0917 - mirrors _probe_candidates' interface
    candidates, state, settings, prober, budget, log,
):
    """Which candidates get probed this call, reserving their budget.

    Sequential and side-effecting (spends budget) by design: this is the
    dispatching thread's decision, made in full before anything is
    submitted to the probe pool, so Budget needs no lock.
    """
    to_probe = []
    skipped_fresh = 0
    skipped_aborted = 0
    for row in candidates:
        if state.is_fresh(row.stream_id, row.url, settings["probe_ttl_hours"]):
            skipped_fresh += 1
            continue
        if row.provider_id in prober.aborted_providers:
            # probe_one already no-ops for an aborted provider; skip the
            # call entirely so its candidates don't burn budget that
            # healthy providers could otherwise use - across calls. A
            # provider that crosses the abort threshold partway through
            # THIS batch still has its already-selected remaining
            # candidates dispatched and charged, since the whole batch is
            # selected up front, before any probe runs.
            skipped_aborted += 1
            continue
        if not budget.allow():
            log.info("FAILOVERR run stopping early: %s", budget.reason)
            break
        budget.spend()
        to_probe.append(row)
    if skipped_fresh or skipped_aborted:
        log.debug(
            "FAILOVERR _select_probe_batch: candidates=%d fresh=%d "
            "aborted_provider=%d to_probe=%d",
            len(candidates), skipped_fresh, skipped_aborted, len(to_probe)
        )
    return to_probe


def _probe_candidates(  # noqa: PLR0913, PLR0917 - interface fixed by the task spec
    candidates, state, settings, prober, budget, resolved, log, probed_so_far=0,
    progress_cb=None, mode=None,
):
    """Probe what is stale, record verdicts, write stats. Returns count.

    Probes run in parallel across a ThreadPoolExecutor bounded by
    global_concurrency (CLAUDE.md §7: different providers may be probed in
    parallel while staying serialized within each - Prober.probe_one
    already enforces the per-account and global caps internally, so this
    only has to run several of its calls concurrently).

    Which candidates to probe and the budget-spend decision are both made
    by _select_probe_batch, sequentially, in this dispatching thread,
    before any future is submitted - so neither Budget nor State needs a
    lock. Only prober.probe_one() (already thread-safe) and is_blank()
    (stateless) run inside worker threads; state.record(),
    models_access.save_stream_stats(), and the heartbeat/save cadence all
    stay on this thread, processed in completion order.

    probed_so_far is the run-wide probe count before this call, so the
    25-probe heartbeat/save cadence accumulates across the whole run
    rather than resetting every time run_pipeline calls this once per
    channel - real channels rarely have 25 candidates each, so a
    per-call-local counter would almost never fire.

    Stop (cancel_requested()) can only take effect between batches at the
    _select_probe_batch level - a whole channel's candidates are submitted
    to the pool in one go, up to global_concurrency of them run truly
    concurrently, and there is no way to kill a subprocess already
    mid-probe. So work() re-checks cancel_requested() right before it would
    start a *new* probe: anything still queued behind a busy worker thread
    when Stop was pressed short-circuits instead of launching, while
    whatever already started keeps running to completion - "wait for the
    current probe, then stop," not "wait for this whole channel."
    """
    to_probe = _select_probe_batch(candidates, state, settings, prober, budget, log)
    if not to_probe:
        return 0

    def work(candidate_row):
        if cancel_requested():
            return candidate_row, None, None, None, None
        result = prober.probe_one(candidate_row.provider_id, candidate_row.url)
        verdict, stats = result.verdict, result.stats
        response_time_ms = result.response_time_ms
        reason = result.reason
        if verdict == VALID and settings["blank_detect"] and probing.is_blank(
            candidate_row.url, settings["ffmpeg_path"], settings["blank_detect_seconds"]
        ):
            verdict, stats, response_time_ms = INVALID, {}, None
            reason = "blank stream detected"
        return candidate_row, verdict, stats, response_time_ms, reason

    probed = 0
    workers = max(1, int(settings["global_concurrency"]))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_row = {
            pool.submit(work, candidate_row): candidate_row
            for candidate_row in to_probe
        }
        for future in concurrent.futures.as_completed(future_to_row):
            try:
                result = future.result()
                candidate_row, verdict = result[0], result[1]
                stats, response_time_ms = result[2], result[3]
                reason = result[4]
            except Exception:
                # One bad probe (a bug in work(), not just a network error -
                # those are already turned into an INCONCLUSIVE ProbeResult
                # by probe()) must not crash the whole batch or lose the
                # other futures already completed or in flight.
                candidate_row = future_to_row[future]
                log.exception(
                    "FAILOVERR probe worker raised unexpectedly "
                    "stream=%s name=%r provider=%s",
                    candidate_row.stream_id, candidate_row.name,
                    candidate_row.provider_id,
                )
                continue
            if _handle_probe_result(
                candidate_row, verdict, stats, response_time_ms, reason,
                resolved, state, log, progress_cb,
                probed_so_far + probed, mode,
            ):
                probed += 1
    return probed


def _handle_probe_result(  # noqa: PLR0913, PLR0917 - one call site, _probe_candidates
    candidate_row, verdict, stats, response_time_ms, reason, resolved, state, log,
    progress_cb, probed_before, mode=None,
):
    """Record one probe's verdict, save stats, and report progress.

    Returns whether it actually counted as a probe. A None verdict means
    work() short-circuited because Stop was pressed before this candidate's
    probe started - nothing to record, same as it never having been picked.
    """
    if verdict is None:
        return False
    state.record(
        candidate_row.stream_id, candidate_row.url, verdict,
        response_time_ms=response_time_ms,
    )
    log.info(
        "FAILOVERR probe stream=%s name=%r provider=%s verdict=%s reason=%s",
        candidate_row.stream_id, candidate_row.name,
        candidate_row.provider_id, verdict, reason,
    )
    try:
        if verdict == VALID and stats:
            models_access.save_stream_stats(resolved, candidate_row.stream_id, stats)
        probed_total = probed_before + 1
        if progress_cb is not None:
            progress_cb(candidate_row, verdict, probed_total)
        if probed_total % 25 == 0:
            update_progress(mode)
            state.save()
    except Exception:
        # state.record() above already succeeded, so the probe verdict is
        # safe regardless of what happens here - everything in this try is
        # best-effort bookkeeping (saved stats, progress callback, the
        # heartbeat/save cadence) that must not un-record a real probe
        # result just because one of those side effects broke.
        log.exception(
            "FAILOVERR post-probe ops failed for stream=%s; "
            "verdict recorded, continuing",
            candidate_row.stream_id,
        )
    return True


def _run_outcome(budget):
    """Decide the run's final verdict: CANCELED beats INTERRUPTED beats COMPLETED."""
    if budget.canceled:
        return "canceled", "CANCELED"
    if budget.reason:
        return "interrupted", "INTERRUPTED"
    return "ok", "COMPLETED"


def _channel_candidates(mode, channel, index, resolved, settings):
    """Build the matched + already-attached candidate set for one channel.

    The candidate set requirements 3 and 5 share (CLAUDE.md §6 step 5) -
    shared by run_pipeline's main loop and run_preview, so the two can
    never drift apart on what counts as a candidate (preview must show
    what a run would actually produce).

    One attached-link query (iter_attached_rows), not two: attached_ids is
    derived from the same StreamRows the candidate set is built from, so the
    link table is read once per channel per pass, not twice.
    """
    tokens = normalize(
        channel.name or "", strip_tokens=settings["strip_tokens"],
        map_number_words=settings["map_number_words"],
    )
    matched = (
        [] if mode != "run"
        else find_matches(tokens, index, settings["match_mode"],
                          settings["fuzzy_threshold"])
    )
    by_id = {row.stream_id: row for row in matched}
    attached_ids = set()
    for row in iter_attached_rows(resolved, channel, settings):
        by_id.setdefault(row.stream_id, row)
        attached_ids.add(row.stream_id)
    return matched, attached_ids, list(by_id.values())


def _start_channel_progress(  # noqa: PLR0913, PLR0917 - one call site, run_pipeline's loop
    mode, totals, channel_index, channels_total, channel, matched, attached_ids,
):
    """Ping progress at channel start; return this channel's per-probe callback.

    Bundles two responsibilities into one call so run_pipeline's channel
    loop only pays one statement for both (ruff's statement-count limit).
    "Found" means matched-but-not-yet-attached and confirmed VALID this
    run, independent of dry_run (which controls attaching, not discovery) -
    tracked live here, one probe at a time, rather than recomputed after
    the fact.
    """
    new_not_attached = {row.stream_id for row in matched} - attached_ids
    update_progress(
        mode, current_stream=None, channel_name=channel.name,
        channel_index=channel_index, channels_total=channels_total,
        new_found=totals["new_found"], attached=totals["attached"],
        detached=totals["detached"],
    )

    def on_probe(candidate_row, verdict, _stream_index):
        if candidate_row.stream_id in new_not_attached and verdict == VALID:
            totals["new_found"] += 1
        update_progress(
            mode, current_stream=candidate_row.name, channel_name=channel.name,
            channel_index=channel_index, channels_total=channels_total,
            new_found=totals["new_found"], attached=totals["attached"],
            detached=totals["detached"],
        )

    return on_probe


def _close_old_connections():
    """Drop Django DB connections past CONN_MAX_AGE.

    run_pipeline runs outside Django's request/response cycle (a background
    thread/greenlet, or the scheduler thread), so the usual per-request
    cleanup signal never fires here - do it explicitly instead.

    Kept as its own function, not inlined at the one call site: this is
    also the seam tests monkeypatch to skip a real `django.db` import in
    the bare pytest run that has no Django installed - a name the import
    itself couldn't provide.
    """
    from django.db import close_old_connections

    close_old_connections()


def _close_connection():
    """Release this thread's Django DB connection once a run is done.

    Same reasoning as _close_old_connections, including the test-seam one:
    nothing else closes it for a thread/greenlet that never went through a
    Django request.
    """
    from django.db import connection

    connection.close()


def run_pipeline(context, mode="run"):
    """Single entry point for Run, Reorder Only, and Probe Only."""
    log = context.get("logger") or logger
    settings = load_settings(context)
    _close_old_connections()
    resolved = models_access.resolve_models()
    state = State.load(STATE_PATH)
    budget = Budget(
        settings["max_probes_per_run"], settings["max_run_minutes"],
        cancel_fn=cancel_requested,
    )

    channels = select_channels(resolved, settings)
    index = (
        {} if mode != "run"
        else build_index(iter_pool(resolved, settings))
    )
    # Heartbeat the lock right after the indexing phase (which scans the whole
    # pool and otherwise runs without a refresh), so a long indexing pass
    # can't let the lock go stale before the channel loop ever starts. No-op
    # when this process doesn't hold the lock (holder mismatch).
    update_progress(mode)
    prober = Prober(
        settings["ffprobe_path"], settings["probe_timeout_seconds"],
        settings["per_account_concurrency"], settings["global_concurrency"],
        settings["account_cooldown_seconds"],
    )

    channels_total = len(channels)

    rows = []
    totals = {
        "attached": 0, "detached": 0, "probed": 0, "channels": 0, "new_found": 0,
    }

    try:
        for channel_index, channel in enumerate(channels, start=1):
            # Heartbeat at every channel boundary, not just every 25 probes: the
            # 25-probe cadence never fires during a reorder_only run (no probing)
            # or between channels, so without this the lock could go stale during
            # a non-probing phase and be force-cleared mid-run.
            update_progress(mode)
            if cancel_requested():
                # Catches Stop pressed between channels - including for
                # reorder_only, which never calls _probe_candidates and so
                # never touches budget.allow() on its own.
                budget.canceled = True
                budget.reason = budget.reason or "canceled by user"
                log.info(
                    "FAILOVERR %s stopping early before channel %r: %s",
                    mode, channel.name, budget.reason,
                )
                break

            matched, attached_ids, candidates = _channel_candidates(
                mode, channel, index, resolved, settings
            )
            on_probe = _start_channel_progress(
                mode, totals, channel_index, channels_total, channel,
                matched, attached_ids,
            )

            if mode != "reorder_only":
                totals["probed"] += _probe_candidates(
                    candidates, state, settings, prober, budget, resolved, log,
                    probed_so_far=totals["probed"], progress_cb=on_probe, mode=mode,
                )

            if mode == "probe_only":
                totals["channels"] += 1
                continue

            if budget.reason:
                # Stop pressed (or the probe/time budget running out) partway
                # through this channel's own probe batch - a truncated probe
                # pass must never get committed via apply_channel_plan.
                log.info(
                    "FAILOVERR %s stopping early before writing channel %r: %s",
                    mode, channel.name, budget.reason,
                )
                break

            ordered, detach = plan_channel(
                attached_ids, candidates, state,
                *_plan_channel_args(settings),
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

            rows.extend(_build_channel_report_rows(
                channel, ordered, detach, candidates, attached_ids, state
            ))
    finally:
        state.meta.update({
            "last_run": time.time(),
            "last_mode": mode,
            "degraded_providers": sorted(str(p) for p in prober.aborted_providers),
            "budget_stop": budget.reason,
        })
        state.save()

    path = write_report(rows, report_path(mode))
    status, verb = _run_outcome(budget)
    degraded = " DEGRADED" if prober.aborted_providers else ""
    log.info(
        "FAILOVERR %s %s%s: %s channels, %s probed, %s attached, "
        "%s detached, dry_run=%s, report %s",
        mode, verb, degraded, totals["channels"], totals["probed"],
        totals["attached"], totals["detached"], settings["dry_run"], path,
    )
    return {"status": status, "report": path, "degraded_providers":
            sorted(str(p) for p in prober.aborted_providers), **totals}


def start(context, mode="run"):
    """Acquire the lock and run in the background."""
    log = context.get("logger") or logger
    settings = load_settings(context)
    # The lock's TTL must outlive a legitimate run, not just the default 30
    # minutes: max_run_minutes defaults to 60, and a run within its budget
    # must not have its lock auto-stolen by a second acquire_lock partway
    # through. The per-channel + after-indexing heartbeat keeps the lock
    # fresh well inside this TTL, so it only governs the auto-steal window.
    lock_ttl = max(_LOCK_TTL_SECONDS, settings["max_run_minutes"] * 60 + 300)
    if not acquire_lock(mode, ttl=lock_ttl):
        held = lock_status()["holder"]
        return {
            "status": "error",
            "message": (
                f"A {held} operation is already running. Wait for it to "
                f"finish, or use Clear Lock if it is stuck."
            ),
        }

    _clear_cancel()  # clear cancel flag, now that we hold the lock

    try:
        resolved = models_access.resolve_models()
        channel_count = len(select_channels(resolved, settings))
    except Exception as exc:
        # Anything from a bad field resolution to a DB connectivity error
        # can land here, and every one of them must still release the lock
        # this function just acquired - a stuck lock from a failed startup
        # would block every future run, not just this one.
        release_lock(mode)
        log.exception("FAILOVERR %s FAILED", mode)
        return {"status": "error", "message": str(exc)}

    def background():
        try:
            run_pipeline(context, mode)
        except Exception:  # a background run must never crash silently
            log.exception("FAILOVERR %s FAILED", mode)
        finally:
            release_lock(mode)
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
