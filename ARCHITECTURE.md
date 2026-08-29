# Architecture

Developer-facing notes on how Failoverr is built. For what the plugin does
and how to configure it, see [README.md](README.md).

## Module map

```
failoverr/
  plugin.py         Entry point: Plugin class, fields/actions, action handlers
  pipeline.py        Orchestration: settings, indexing, matching, planning, run loop
  models_access.py   All ORM access + runtime field-name resolution
  naming.py           Pure: channel/stream name normalization
  ordering.py         Pure: quality ranking, provider interleaving, order-rewrite
  probing.py          ffprobe/ffmpeg invocation, verdict classification, concurrency
  state.py            Pure: sidecar probe cache (JSON), TTL, removal hysteresis
  scheduling.py       django-celery-beat integration
  tasks.py            The @shared_task the scheduler fires
```

`naming.py`, `ordering.py`, and `state.py` are pure — no Django, no
Dispatcharr, no filesystem beyond `state.py`'s own JSON file. `probing.py`
touches only `subprocess`. Everything that talks to Django/Dispatcharr ORM
lives in `models_access.py`; every one of those imports is inside the
function that uses it, not at module level, so the whole package (including
`pipeline.py`) imports cleanly under a bare `pytest` run with neither Django
nor Dispatcharr installed. `tests/test_manifest.py` (or similar) enforces
this via a no-module-level-Django/Dispatcharr-imports contract.

`pipeline.py` imports `plugin.py` at module level (it reads `Plugin.fields`
to build settings defaults), so `plugin.py` imports `pipeline` lazily inside
each handler method instead — a real circular import otherwise.

## Terminology mapping

Dispatcharr's models don't match how users talk about this feature; keep
this straight when reading the code.

| User language | Code / Dispatcharr model |
|---|---|
| "stream from a playlist" | `Stream` (tens of thousands, the raw pool) |
| "channel" (the custom lineup entry) | `Channel` |
| "channel's stream order" | `Channel.streams.through` (`ChannelStream`), ordered by a resolved `order` field |
| "which provider a stream is from" | `Stream`'s resolved `m3u_account` field |

`models_access.resolve_models()` resolves the through-model's order field
and the stream's provider-link field by trying a list of candidate names
against the live model — Dispatcharr's field names have varied across
versions — and raises `FieldResolutionError` naming what candidates it tried
and what fields actually exist, so a mismatch is diagnosable from the error
alone without a debugger.

## Request flow

Dispatcharr calls `Plugin.run(action, params, context)`. `context["settings"]`
carries the raw (possibly all-string) setting values from the UI;
`context["logger"]` is Dispatcharr's logger for this plugin.

`run()` is a thin dispatch table over six handlers (`diagnose`, `preview`,
`run`→`_start`, `stop`, `clear_state`, `show_status`). It wraps every handler
in `try/except Exception` — an uncaught exception must never surface as a
raw traceback in Dispatcharr's UI — and logs a `FAILOVERR <action> START` /
`COMPLETED`/`FAILED`/`STARTED`/`CANCELED`/`INTERRUPTED` line pair around each
call, which is the whole observability story here: Dispatcharr's UI shows
nothing, so `docker logs -f dispatcharr | grep FAILOVERR` is the intended
debugging tool.

### Settings

`pipeline.load_settings(context)` is the single place raw settings become
typed values: numbers are coerced via `int(float(x))` (falling back to the
field default and logging a warning on a bad value), booleans handle the
`"false"`/`"0"`/`"no"`/`"off"` strings Dispatcharr may hand back (plain
`bool("false")` is `True` in Python — the trap this exists to avoid),
and `strip_tokens`/`codec_priority`/`channel_names` get their own
comma/line parsing. `Plugin.fields` is the source of truth for defaults —
`pipeline.py` reads it directly (`_DEFAULTS`, `_INT_KEYS`, `_BOOL_KEYS`),
a deliberate dependency inversion (entry point → core) rather than a second
place defaults could drift from `plugin.json`.

### Matching

`iter_pool()` streams the whole `Stream` table with `.only(...).iterator()`
to keep memory bounded on a large pool, converting each row via
`_stream_to_row()` into a `StreamRow` — which includes `naming.normalize()`'s
output, a token tuple. `build_index()` groups those by `frozenset(tokens)`
in one pass. `find_matches()` (and `_channel_candidates()`, used by both
`run_preview` and `run_pipeline` so the two can never disagree on what
counts as a candidate) is then an O(1) dict lookup per channel — no per-pair
comparison, matching only exact token-set equality.

`naming.normalize()` is pure text processing: NFKD-fold accents, lowercase,
strip a leading country prefix (`IT:`), strip bracketed/parenthesized
segments, strip configured quality tokens (longest-first, with a `(?<=\d)`
lookbehind so `RAI1HD` still strips `HD`), split on the letter/digit
boundary, and optionally map spelled-out numbers (English/Italian/
German/French) to digits. It never sees provider or probe data — matching is
name-only.

### Probing and verdicts

`probing.classify()` is the single point that turns an `ffprobe` exit code
and stderr into `VALID`/`INVALID`/`INCONCLUSIVE`. Order of checks matters:
the inconclusive-pattern check (timeouts, DNS failures, 401/403/429, 5xx,
connection-limit errors) runs **before** the invalid-pattern check, because
a provider's rate-limit response can produce the same "Invalid data found"
ffprobe emits for a genuinely dead stream — misreading a rate limit as a
dead stream is called out in the module docstring as the project's most
destructive failure mode, since it would eventually detach a working
stream. Only affirmatively-recognized defects (404/410, invalid data,
missing moov atom, ...) count as `INVALID`; anything unrecognized stays
`INCONCLUSIVE` so it can never contribute to removal on its own. A single
`INVALID` reading gets one retry after a short delay before it's accepted,
to distinguish a genuinely dead stream from a live stream's transient
glitch.

`_validate_url()` allow-lists URL schemes (`http`, `https`, `rtmp(s)`,
`rtsp(s)`, `udp`, `rtp`, `srt`) and rejects a leading `-` before a URL ever
reaches a subprocess argv — stream URLs come from third-party M3U playlists,
so this is a real trust boundary, not defensive dead code.

`Prober` enforces two concurrency caps: a per-provider `threading.Semaphore`
(created lazily per `provider_id`) and a global one shared across all
providers, so different providers probe in parallel while each stays
serialized (or capped) internally — most of the available speedup without
tripping any one provider's own connection limit.

Bitrate is measured, not declared: live MPEG-TS/HLS almost never reports
`bit_rate` in its container metadata, so `_packet_video_bitrate_kbps()`
samples ~5 seconds of real packets via `-read_intervals` and averages;
below `_MIN_PACKETS_FOR_BITRATE_CALC` (30) it's left at 0 rather than
persisting a noise-dominated estimate.

### Planning a channel

`pipeline.plan_channel()` is the core decision function — pure, given a
channel's candidate set and cached state — implementing these rules:

- only a confirmed-`VALID` stream is ever newly attached;
- an attached stream that's `INCONCLUSIVE` or never-yet-probed keeps its
  current place rather than being dropped or reordered on no information;
- an attached stream that's `INVALID` but hasn't failed `threshold`
  consecutive times is demoted to the bottom, not removed;
- an attached stream is detached only once `state.should_remove()` says the
  failure streak has crossed the threshold;
- truncation by `max_streams_per_channel` only ever removes streams that
  have a recorded verdict — a never-probed attached stream doesn't count
  against the cap;
- if literally nothing is known about a channel yet (`ranked_ids` comes back
  empty), the channel is left completely alone — never rewritten to an
  empty plan.

Ranking itself is `ordering.order_candidates()`: candidates are bucketed by
`quality_key()` (resolution tier → response time bucket → codec priority →
fps → optionally bitrate → raw height as a final tiebreaker), buckets sorted
descending, and within each bucket, candidates are grouped by provider and
round-robin interleaved (`zip_longest`). Response time and resolution are
deliberately bucketed (250ms, and tier boundaries at 2160/1440/1080/720p)
rather than compared at raw precision — near-continuous raw values would
almost never tie exactly, which would silently disable interleaving. This is
also why "rank by bitrate" is a setting: bitrate is close to always-unique
per probe, so leaving it on tends to break every tie before interleaving
gets a chance to run.

`models_access.plan_writes()` turns a channel's `(ordered_ids, detach_ids)`
into concrete attach/detach/reorder operations against the currently
attached set, and `apply_channel_plan()` executes them inside one
`transaction.atomic()` with `select_for_update()` on the current links — a
concurrent manual edit in the Dispatcharr UI between the read and the write
must not get silently clobbered by a plan computed from a stale snapshot.

### Run loop, budgets, and cancellation

`pipeline.run_pipeline()` is the single entry point behind the Run action
(and the scheduled task — `tasks.scheduled_run()` calls the same plugin
action, no duplicated logic). Per channel: build candidates, probe what's
stale (`Budget`-gated), plan, write (unless `dry_run`), append report rows.

`Budget` is a runaway guard, not a normal limit — defaults (400 probes / 60
minutes) are well above a typical run (~200 probes / 10–20 minutes).
`Budget.allow()` is checked before every probe is dispatched; once it
returns `False` the run stops cleanly, having already saved whatever state
it learned. `cancel_requested()` (backed by a presence-only flag file,
`_CANCEL_PATH`) is the same mechanism the Stop action uses — cooperative
only, checked at the next checkpoint, since there's no way to kill a
subprocess already mid-probe from a separate process.

Probes within a channel run concurrently via `ThreadPoolExecutor`, bounded
by `global_concurrency`; which candidates get probed and the budget spend
are decided sequentially, single-threaded, before anything is submitted to
the pool — so `Budget` and `State` never need their own locks. Only
`prober.probe_one()` and `is_blank()` actually run inside worker threads.

### Cross-process coordination

A manual Run fires inside the uwsgi process; a scheduled run fires inside
the celery worker process — two separate OS processes sharing no Python
memory. Coordination is therefore file-based, not in-memory:

- **`run.lock`** (`_LOCK_PATH`) — holds `{"holder": mode, "since": ts,
  "progress": {...}}`, guarded by `fcntl.flock`. `acquire_lock()` self-heals
  a stale lock (older than `_LOCK_TTL_SECONDS`, or `max_run_minutes*60+300`
  if larger) rather than blocking a plugin forever on a killed run.
  `update_progress()` is called after indexing, at every channel boundary,
  and every 25 probes — it's also what Show Status reads, and it no-ops
  silently if the lock has since been released or stolen by someone else,
  rather than resurrecting a finished run's lock with stale progress.
- **`cancel.flag`** (`_CANCEL_PATH`) — presence-only; `request_cancel()`
  touches it, `cancel_requested()` checks existence, `_clear_cancel()`
  removes it once a new run acquires the lock.
- **`state.json`** (`state.STATE_PATH`) — the probe cache: per-stream
  `verdict`, `url_hash`, `last_probe`, `failures`, `response_time_ms`.
  Written atomically (`tmp.write_text()` then `tmp.replace()`), saved every
  25 probes and at the end of a run. `is_fresh()` is what makes repeat runs
  fast — a stream probed within `probe_ttl_hours` and whose URL hasn't
  changed (`url_hash`) is skipped entirely, *except* an `INCONCLUSIVE`
  verdict is never cached as fresh, since "ask again" is the whole point of
  that verdict. This file is also the resume mechanism: a run that stops
  early (budget or cancel) has already saved every verdict it recorded, so
  the next run picks up where it left off rather than re-probing everything.

`clear_state()` refuses to run while an active (non-stale) lock is held —
that run holds its own `State` instance in memory and would silently
overwrite the reset with its own next `save()`.

### Execution model

`pipeline.spawn()` runs the background job either as a `gevent.spawn()`
greenlet (if gevent has monkey-patched `subprocess` in this process — the
condition under which a blocking `subprocess.run()` in a plain thread would
stall Dispatcharr's whole worker) or a plain daemon `threading.Thread`
otherwise. `execution_model()` reports which, and Diagnose surfaces it via
`models_access.environment_report()`.

### Broken-channel marker

After probing (and only if `candidates` is non-empty), `_update_broken_marker`
computes `_channel_broken_state()`: `False` if any candidate is `VALID`,
`True` only if every candidate has a conclusive `INVALID` verdict, `None`
("not yet known") if anything is still unresolved — a candidate that's
never-probed or stuck `INCONCLUSIVE` blocks the broken verdict the same way
it blocks ranking. `apply_broken_suffix()` is an idempotent string op
(append/strip a suffix like ` [BROKEN]`); `select_channels()` matches both
the bare and suffixed form of a configured channel name so a channel stays
selectable across the plugin's own rename, in either direction.

### Reports

`write_report()` writes a fixed-column CSV per Preview/Run
(`_REPORT_COLUMNS`) to `/data/exports/failoverr-<action>-<timestamp>.csv`,
then `_rotate_reports()` prunes to the newest `_MAX_REPORTS` (100) — best
effort, any failure here must never fail the run since the report was
already written. `_report_row()` and `_build_channel_report_rows()` are the
single source of a report row's shape, shared by `run_preview` and
`run_pipeline`, so the two can't drift on what a row means (e.g. one passing
`None` for a detached stream's row where the other passed the real
`StreamRow`). Both also report every matched-but-unacted-on candidate
(`"matched - would be probed"`, `"not attached - outranked"`, `"not attached
- not qualified"`) — without this, a valid stream that lost the
`max_streams_per_channel` cutoff would vanish with no trace, which is
exactly the gap that once made a legitimately-outranked stream look like a
matching bug.

## Scheduling

`django-celery-beat` drives the schedule, not an in-process timer — it
survives a celery worker restart, and fires from a separate process rather
than a thread inside whichever uwsgi worker happened to handle the toggle.
`scheduling.celery_beat_available()` checks both `core.scheduling` (Dispatcharr)
and `django_celery_beat.models` are importable; if not, `_scheduler_report()`
reports `"unavailable"` and `_ensure_scheduler()` logs a warning and no-ops.

`_ensure_scheduler()` is called at the end of every `diagnose` and every
`_start` (Run) — **not** on every action — so a schedule is (re)armed by
pressing any action button, using whatever settings were current at that
moment; this includes `dry_run`, so a schedule armed while dry run is still
on will run harmlessly until an action is pressed again after turning it
off. It never lets a bad `cron_expression` propagate to the caller — the
real action result must still come back even if arming the schedule failed.

`tasks.scheduled_run()` (the `@shared_task`, registered under
`tasks.TASK_NAME = "failoverr.scheduled_run"`) is what celery beat's
`PeriodicTask` row points at; it just calls
`PluginManager.get().run_action("failoverr", "run", {})` — same code path as
a manual Run button press, so there is no separate scheduled-run
implementation to keep in sync. Dispatcharr's celery worker startup hook
imports every enabled plugin's `plugin.py` at each worker (re)start, which
is what actually registers the task — celery beat itself never imports
`plugin.py`, it only stores the dotted task name string. This is also why a
freshly-armed or freshly-changed schedule doesn't fire immediately: it needs
the worker to next restart or fork a child that re-imports the plugin.

`Plugin.stop()` (Dispatcharr's disable/delete/reload hook) calls
`scheduling.disable_celery_beat()`, best-effort, to remove the
`PeriodicTask` row.

## Settings/manifest consistency

`plugin.json` (name/version/description/author, read by Dispatcharr's
plugin loader for display) and `Plugin.fields`/`Plugin.actions` in
`plugin.py` (the actual settings/actions Dispatcharr renders and passes back
through `context`) must stay in sync by hand — there is no code generation
between them. `tests/test_manifest.py` is the place to look for (or extend)
an assertion that keeps them from drifting.

## Testing

```
uv run --with pytest pytest tests/ -v
uv run --with ruff ruff check .
```

Neither `pytest` nor `ruff` is a project dependency or on `PATH` — `uv run
--with` fetches the right version on demand rather than requiring a local
install. Both must be clean before any commit.

Tests exercise the pure modules (`naming`, `ordering`, `state`) directly,
and `pipeline`/`models_access`/`probing`/`plugin` against fakes/monkeypatches
rather than a real Dispatcharr instance or Django ORM — the module-level
laziness contract described above (no top-level Django/Dispatcharr imports
anywhere in the package) is what makes that possible without installing
either.
