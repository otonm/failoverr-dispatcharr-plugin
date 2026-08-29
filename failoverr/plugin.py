"""Failoverr - Dispatcharr plugin entry point.

Actual django.*/apps.* calls are lazy (inside the functions that make
them) so a bare pytest run never needs Django installed. models_access,
naming, scheduling and state.State are imported below despite that: none
of those modules has a top-level Django import (each is lazy the same way,
one level down), so importing them here costs nothing a bare pytest run
can't afford. `pipeline` is the one exception, and it stays function-local
everywhere it's used: pipeline.py itself does `from .plugin import
Plugin` at module level, so a module-level `from . import pipeline` here
would be a real circular import, not just an unnecessary one.
"""

import json
import logging

from . import (
    models_access,
    naming,
    scheduling,
    tasks,  # noqa: F401 - import-time side effect: registers @shared_task.
)
from .state import State

# Dispatcharr's loader imports THIS file directly (preferring plugin.py over
# __init__.py when both exist), and its celery worker only ever registers a
# plugin's @shared_task by importing plugin.py at worker (re)start - so this
# import has to live here, not in __init__.py, for scheduled_run to exist in
# the worker's task registry at all.

logger = logging.getLogger("failoverr")

# How many raw stream_stats rows Diagnose shows.
_STATS_SAMPLES = 3

_BACKUP_WARNING = (
    "There is no undo. Back up your Dispatcharr database before running this."
)

# Substrings that mark a setting whose value must never reach the log.
_REDACT = ("password", "secret", "api_key")


def _ensure_scheduler(context):
    """Arm the schedule to match current settings via django-celery-beat.

    Never lets a bad setting (e.g. a malformed cron expression) escape to
    the caller - Diagnose/Run/etc. must still return their real result even
    when the schedule can't be armed. A scheduled run fires inside the
    celery worker process while a manual Run fires inside uwsgi's -
    pipeline.py's lock file (not an in-memory dict) is what keeps those
    from overlapping.
    """
    from . import pipeline  # circular otherwise - see the module docstring

    settings = pipeline.load_settings(context)
    if not scheduling.celery_beat_available():
        logger.warning("FAILOVERR django-celery-beat unavailable; schedule not armed")
        return
    try:
        scheduling.sync_celery_beat(
            settings["cron_expression"], settings["schedule_enabled"]
        )
    except Exception:
        # Broad on purpose, per the docstring above: a bad cron_expression
        # is the expected failure, but anything else create_or_update_
        # periodic_task raises must be swallowed the same way - the
        # caller's real result still has to come back.
        logger.exception(
            "FAILOVERR celery-beat schedule not armed: bad cron_expression %r",
            settings["cron_expression"],
        )
        return
    logger.info(
        "FAILOVERR celery-beat schedule armed: %r (enabled=%s)",
        settings["cron_expression"], settings["schedule_enabled"],
    )


def _scheduler_report(scheduling):
    """Report whether the schedule can be armed, and its one gotcha, for Diagnose."""
    if scheduling.celery_beat_available():
        return {
            "backend": "celery_beat",
            "note": (
                "Scheduled via django-celery-beat: runs in Dispatcharr's "
                "system timezone (Settings > General). A freshly-enabled or "
                "freshly-changed schedule won't fire until the celery "
                "worker process next restarts or forks a new child - it "
                "only re-imports plugins at that point."
            ),
        }
    return {
        "backend": "unavailable",
        "note": "django-celery-beat is not installed; scheduling is disabled.",
    }


def _log_report(log, action, label, payload):
    """Log one prefixed line per top-level key.

    Dispatcharr shows nothing in the UI, so `docker logs -f dispatcharr |
    grep FAILOVERR` is the only channel - and grep is line-based, so a
    single multi-line dump would match on its first line only. A nested
    value (diagnose's result, mostly) is rendered as one JSON blob rather
    than one line per leaf, so it still fits on its own greppable line.
    """
    for key, value in (payload or {}).items():
        if any(word in key.lower() for word in _REDACT):
            shown = "***"
        elif isinstance(value, (dict, list, tuple)):
            shown = json.dumps(value, default=str)
        else:
            shown = value
        log.info("FAILOVERR %s %s.%s = %s", action, label, key, shown)


def _status_message(lock, stop_requested, streams_tracked):
    """Build Show Status's human-readable line from the lock file's progress."""
    if not lock["holder"]:
        return f"Idle. {streams_tracked} streams tracked."
    holder = lock["holder"]
    progress = lock["progress"]
    if not progress:
        message = f"Running {holder}: starting up."
    else:
        message = (
            f"Running {holder}: channel {progress.get('channel_index')} of "
            f"{progress.get('channels_total')} ({progress.get('channel_name')}). "
            f"Found {progress.get('new_found', 0)} new streams."
        )
    if stop_requested:
        message += " Stop requested - finishing current probe, then stopping."
    return message


class Plugin:
    name = "Failoverr"
    version = "1.0.0"
    description = (
        "Matches M3U streams to your channels, probes them for real validity "
        "and quality, and maintains failover order with providers interleaved."
    )
    author = "otonvm"

    fields = [
        {
            "id": "dry_run",
            "label": "Dry run",
            "type": "boolean",
            "default": True,
            "help_text": (
                "ON: computes everything and writes a CSV report, but never "
                "attaches, detaches, or reorders a stream. Probe results are "
                "still saved, so turning this off afterwards runs almost "
                "instantly. Leave it on until a Preview looks right."
            ),
        },
        {
            "id": "ffprobe_path",
            "label": "ffprobe path",
            "type": "string",
            "default": "/usr/local/bin/ffprobe",
            "help_text": (
                "Wrong path means every probe fails as inconclusive and "
                "nothing is ever attached. Run Diagnose to confirm this path "
                "exists before your first run."
            ),
        },
        {
            "id": "ffmpeg_path",
            "label": "ffmpeg path",
            "type": "string",
            "default": "/usr/local/bin/ffmpeg",
            "help_text": (
                "Used only for blank-screen detection. Ignored when that "
                "setting is off."
            ),
        },
        {
            "id": "probe_timeout_seconds",
            "label": "Probe timeout (seconds)",
            "type": "number",
            "default": 15,
            "help_text": (
                "Lower is faster but marks slow-but-working streams as "
                "inconclusive, so they keep their old ranking and are retried "
                "next run. A timeout never counts toward removal."
            ),
        },
        {
            "id": "channel_group",
            "label": "Channel group",
            "type": "string",
            "default": "",
            "help_text": (
                "Only channels in this group are touched. Blank means every "
                "channel, which on a large lineup is a much longer run."
            ),
        },
        {
            "id": "channel_names",
            "label": "Channel names",
            "type": "text",
            "default": "",
            "help_text": (
                "One channel name per line. When set, only these channels are "
                "processed and the group filter is ignored. Use this to test "
                "on one channel before a full run. If 'Mark channels with no "
                "valid streams' is on, a name here also matches that channel "
                "once it carries the broken suffix, so a marked channel is "
                "not silently dropped from this list and can still recover."
            ),
        },
        {
            "id": "strip_tokens",
            "label": "Quality tokens to ignore",
            "type": "text",
            "default": (
                "4k,uhd,fhd,hd,sd,hevc,h265,h264,avc,raw,fullhd,ultrahd,"
                "1080p,1080i,720p,576p,480p,multi,backup,alt"
            ),
            "help_text": (
                "Comma separated. These are removed from names before "
                "matching, so 'RAI 1 HD' and 'RAI 1 4K' both reduce to "
                "'rai 1'. Removing a token here that is part of a real "
                "channel name will break that channel's matching."
            ),
        },
        {
            "id": "map_number_words",
            "label": "Treat spelled-out numbers as digits",
            "type": "boolean",
            "default": True,
            "help_text": (
                "Maps one-to-ten in English, Italian, German and French, so "
                "'Rai Uno' matches the channel 'RAI 1'. Turn off if your "
                "channel names contain those words literally."
            ),
        },
        {
            "id": "codec_priority",
            "label": "Codec priority",
            "type": "string",
            "default": "hevc,h265,h264,avc",
            "help_text": (
                "Best first, comma separated. Codecs not listed sort last. "
                "This only breaks ties within a resolution tier - a 1080p "
                "h264 stream always outranks a 720p HEVC one."
            ),
        },
        {
            "id": "rank_by_bitrate",
            "label": "Rank by bitrate",
            "type": "boolean",
            "default": True,
            "help_text": (
                "Uses measured video bitrate as a ranking factor. Lowest "
                "priority - only breaks ties left over after resolution, "
                "response time, codec, and fps, which always rank. Bitrate "
                "is measured per-probe and rarely comes out identical "
                "between streams, so leaving this on usually prevents an "
                "exact tie from ever reaching this point - which means "
                "provider interleaving rarely triggers even when "
                "everything above matches. Turn this off if you want "
                "similar-quality streams from different providers to "
                "actually interleave."
            ),
        },
        {
            "id": "max_streams_per_channel",
            "label": "Max streams per channel",
            "type": "number",
            "default": 10,
            "help_text": (
                "Streams beyond this are detached after ordering. Too low "
                "loses working fallbacks; too high makes Dispatcharr walk a "
                "long list of poor sources during an outage."
            ),
        },
        {
            "id": "probe_ttl_hours",
            "label": "Probe cache lifetime (hours)",
            "type": "number",
            "default": 24,
            "help_text": (
                "Streams probed more recently than this are skipped entirely, "
                "which is what makes repeat runs fast. A stream whose URL "
                "changed is always re-probed regardless of this setting."
            ),
        },
        {
            "id": "per_account_concurrency",
            "label": "Concurrent probes per provider",
            "type": "number",
            "default": 1,
            "help_text": (
                "Must not exceed your provider's connection limit. Setting "
                "this too high makes your own probes fail each other, and the "
                "plugin then concludes that working streams are dead. If "
                "unsure, leave it at 1."
            ),
        },
        {
            "id": "account_cooldown_seconds",
            "label": "Cooldown between probes per provider",
            "type": "number",
            "default": 2,
            "help_text": (
                "Pause after each probe on the same provider. Raise it if a "
                "provider starts returning connection-limit errors partway "
                "through a run."
            ),
        },
        {
            "id": "global_concurrency",
            "label": "Global concurrent probes",
            "type": "number",
            "default": 4,
            "help_text": (
                "Ceiling on how many probes run at once across all "
                "providers combined. Different providers are probed in "
                "parallel up to this limit; each provider is still capped "
                "separately by 'Concurrent probes per provider'."
            ),
        },
        {
            "id": "removal_failure_threshold",
            "label": "Failures before removal",
            "type": "number",
            "default": 3,
            "help_text": (
                "A stream is detached only after this many consecutive runs "
                "found it genuinely broken. Earlier failures just move it to "
                "the bottom of the list. Setting this to 1 will detach "
                "streams over a single provider hiccup."
            ),
        },
        {
            "id": "blank_detect",
            "label": "Detect blank (black) streams",
            "type": "boolean",
            "default": False,
            "help_text": (
                "Samples a few seconds of video and rejects an all-black "
                "picture. Roughly doubles run time, which at this lineup size "
                "is still only a few extra minutes. Any ffmpeg error leaves "
                "the stream accepted rather than rejected."
            ),
        },
        {
            "id": "blank_detect_seconds",
            "label": "Blank detection sample (seconds)",
            "type": "number",
            "default": 5,
            "help_text": (
                "Longer samples are more reliable but slower. A stream is "
                "only rejected when essentially the whole sample is black."
            ),
        },
        {
            "id": "max_probes_per_run",
            "label": "Max probes per run",
            "type": "number",
            "default": 400,
            "help_text": (
                "Safety stop, not a normal limit - a typical run uses about "
                "200. On reaching it the run stops cleanly and keeps what it "
                "learned; the next run continues where this one left off."
            ),
        },
        {
            "id": "max_run_minutes",
            "label": "Max run time (minutes)",
            "type": "number",
            "default": 60,
            "help_text": (
                "Safety stop for a provider that hangs every connection. A "
                "typical run takes 10 to 20 minutes. Stopping early is not "
                "destructive - progress is saved."
            ),
        },
        {
            "id": "mark_broken_channels",
            "label": "Mark channels with no valid streams",
            "type": "boolean",
            "default": True,
            "help_text": (
                "Appends a suffix to a channel's name once every one of its "
                "matched and attached streams is confirmed invalid, so a "
                "dead channel is obvious at a glance in the channel list. "
                "The suffix is removed automatically once the channel has a "
                "valid stream again. Only applies to channels that matched "
                "or already have at least one stream - a channel that "
                "matches nothing is left alone, as is one nothing has been "
                "probed for yet. Ignored while dry run is on."
            ),
        },
        {
            "id": "broken_channel_suffix",
            "label": "Broken channel suffix",
            "type": "string",
            "default": " [BROKEN]",
            "help_text": (
                "Appended to the channel name when 'Mark channels with no "
                "valid streams' is on. Keep it inside brackets or "
                "parentheses, like the default - that is what makes "
                "matching ignore it, so a marked channel is still found "
                "again once a stream becomes valid. A suffix without "
                "brackets becomes part of the name Failoverr matches "
                "against, which will break that channel's matching."
            ),
        },
        {
            "id": "schedule_enabled",
            "label": "Run on a schedule",
            "type": "boolean",
            "default": False,
            "help_text": (
                "Arms the scheduler the next time any action button is "
                "pressed - it does not start itself the instant the "
                "container comes up. After a container restart, press "
                "Diagnose once (it's read-only) to arm it. Settings, "
                "including dry run, are captured at that moment - re-press "
                "an action after changing settings to re-arm with the new "
                "values. Turn dry run off first, or the scheduled run will "
                "change nothing."
            ),
        },
        {
            "id": "cron_expression",
            "label": "Schedule (cron)",
            "type": "string",
            "default": "0 4 * * *",
            "help_text": (
                "Standard five-field cron, interpreted in Dispatcharr's "
                "system timezone (Settings > General). The default is "
                "04:00 daily. Probing consumes provider connections, so "
                "pick an hour when nobody is watching."
            ),
        },
    ]

    actions = [
        {
            "id": "diagnose",
            "label": "Diagnose",
            "description": (
                "Read-only. Reports what Failoverr can see: resolved database "
                "fields, ffprobe version, stream pool size, and how your "
                "channel names normalize. Run this first."
            ),
        },
        {
            "id": "preview",
            "label": "Preview",
            "description": (
                "Read-only. Shows which streams would be attached and in what "
                "order, using probe data already cached. Probes nothing, "
                "changes nothing, writes a CSV."
            ),
        },
        {
            "id": "run",
            "label": "Run",
            "description": "Full pipeline: match, probe, order, and write.",
            "confirm": {
                "required": True,
                "title": "Run Failoverr?",
                "message": (
                    "This will attach, detach and reorder streams on your "
                    "channels. " + _BACKUP_WARNING
                ),
            },
        },
        {
            "id": "stop",
            "label": "Stop",
            "description": (
                "Interrupts the run currently in progress. Cooperative, not "
                "instant: the probe already in flight finishes, then the run "
                "stops and releases its lock. Does nothing if nothing is "
                "running."
            ),
        },
        {
            "id": "clear_state",
            "label": "Clear State",
            "description": (
                "Wipes the probe cache: cached verdicts, URL hashes, and "
                "failure counters. Every stream is treated as unprobed and "
                "fully re-checked on the next run, at full probe cost. "
                "Attached streams are not touched until then."
            ),
            "confirm": {
                "required": True,
                "title": "Clear the probe cache?",
                "message": (
                    "This discards every cached probe result and failure "
                    "counter - it does not touch Dispatcharr's database or "
                    "any attached stream. The next run re-probes everything "
                    "from scratch, which at this lineup size can take a "
                    "while and consumes provider connections."
                ),
            },
        },
        {
            "id": "show_status",
            "label": "Show Status",
            "description": (
                "Current run progress, budget use, and which execution mode "
                "is active."
            ),
        },
    ]

    def run(self, action, params=None, context=None):
        context = context or {}
        log = context.get("logger") or logger
        log.info("FAILOVERR %s START", action)
        _log_report(log, action, "settings", context.get("settings", {}))
        handlers = {
            "diagnose": self._diagnose,
            "preview": self._preview,
            "run": lambda _, c: self._start(c, "run"),
            "stop": self._stop,
            "clear_state": self._clear_state,
            "show_status": self._show_status,
        }
        handler = handlers.get(action)
        if handler is None:
            log.error("FAILOVERR %s FAILED: unknown action", action)
            return {"status": "error", "message": f"Unknown action: {action}"}
        try:
            result = handler(params or {}, context)
        except Exception as exc:
            # This is the plugin's outer boundary: whichever of the six
            # action handlers just ran can fail in ways specific to it (a
            # bad ffprobe path, a Django error, a malformed setting), and
            # every one of them has to come back as a normal error dict -
            # an uncaught exception here would surface as a raw traceback
            # in Dispatcharr's UI instead, logged rather than swallowed.
            log.exception("FAILOVERR %s FAILED", action)
            return {"status": "error", "message": str(exc)}
        _log_report(log, action, "result", result)
        status = result.get("status") if isinstance(result, dict) else None
        # "started" means a background thread/greenlet just launched, not that
        # the work is done - the actual pipeline run logs its own COMPLETED /
        # CANCELED / INTERRUPTED line (pipeline.run_pipeline) once it's real.
        verb = {
            "error": "FAILED",
            "started": "STARTED",
            "canceled": "CANCELED",
            "interrupted": "INTERRUPTED",
        }.get(status, "COMPLETED")
        log.info("FAILOVERR %s %s", action, verb)
        return result

    def _diagnose(self, params, context):
        from . import pipeline  # circular otherwise - see the module docstring

        log = context.get("logger") or logger
        log.info("FAILOVERR diagnose START: loading settings and resolving models")

        settings = pipeline.load_settings(context)

        resolved = models_access.resolve_models()
        environment = models_access.environment_report(
            settings["ffprobe_path"], settings["ffmpeg_path"],
        )

        stream_model = resolved.stream_model
        pool_size = stream_model.objects.count()

        # A few real stream_stats rows, so Diagnose shows what Dispatcharr
        # actually populates without loading the whole pool.
        samples = [
            stats for stats in
            stream_model.objects.exclude(stream_stats__isnull=True)
            .values_list("stream_stats", flat=True)[:_STATS_SAMPLES]
            if isinstance(stats, dict)
        ]

        channel_model = resolved.channel_model
        channel_names = list(
            channel_model.objects.values_list("name", flat=True)[:10]
        )

        result = {
            "status": "ok",
            "models": {
                "channel_stream_model": str(resolved.channel_stream_model),
                "order_field": resolved.order_field,
                "provider_field": resolved.provider_field,
                "channel_stream_fields": sorted(
                    f.name for f in resolved.channel_stream_model._meta.get_fields()
                ),
                "stream_fields": sorted(
                    f.name for f in stream_model._meta.get_fields()
                ),
            },
            "environment": environment,
            "pool": {
                "stream_count": pool_size,
                "stream_stats_samples": samples,
            },
            "channels": {
                "total": channel_model.objects.count(),
                "normalization_examples": [
                    {"name": n, "tokens": list(naming.normalize(
                        n,
                        strip_tokens=settings["strip_tokens"],
                        map_number_words=settings["map_number_words"],
                    ))}
                    for n in channel_names
                ],
            },
            "scheduler": _scheduler_report(scheduling),
        }
        _ensure_scheduler(context)
        log.info(
            "FAILOVERR diagnose COMPLETED: pool=%d channels=%d",
            pool_size, channel_model.objects.count()
        )
        return result

    def _preview(self, params, context):
        from . import pipeline

        return pipeline.run_preview(context)

    def _start(self, context, mode):
        from . import pipeline

        result = pipeline.start(context, mode)
        _ensure_scheduler(context)
        return result

    def _stop(self, params, context):
        from . import pipeline

        (context.get("logger") or logger).info("FAILOVERR stop requested by user")
        return pipeline.stop_run()

    def _clear_state(self, params, context):
        from . import pipeline

        (context.get("logger") or logger).info("FAILOVERR state cleared by user")
        return pipeline.clear_state()

    def _show_status(self, params, context):
        from . import pipeline

        state = State.load(pipeline.STATE_PATH)
        lock = pipeline.lock_status()
        meta = state.meta
        return {
            "status": "ok",
            "running": lock["holder"],
            "progress": lock["progress"],
            "execution_model": pipeline.execution_model(),
            "streams_tracked": len(state.streams),
            "last_run": meta.get("last_run"),
            "last_mode": meta.get("last_mode"),
            "budget_stop": meta.get("budget_stop"),
            "message": _status_message(
                lock, pipeline.cancel_requested(), len(state.streams)
            ),
        }

    def stop(self, context=None):
        """Stop the scheduler. Called on disable/delete/reload."""
        log = (context or {}).get("logger") or logger
        log.info("FAILOVERR scheduler stop (disable/delete/reload)")
        try:
            scheduling.disable_celery_beat()
        except Exception:
            # Called from Dispatcharr's disable/delete/reload hook, which
            # has nowhere to surface a raised exception - log it and return
            # rather than let the plugin loader see a crash.
            log.exception("FAILOVERR scheduler stop FAILED")
            return
        log.info("FAILOVERR scheduler stop COMPLETED")
