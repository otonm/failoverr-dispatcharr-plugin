"""Failoverr - Dispatcharr plugin entry point.

Django and Dispatcharr imports are lazy (inside functions) so the pure
modules stay importable in a bare pytest run.
"""

import collections
import logging
import threading

logger = logging.getLogger("failoverr")

# How many raw stream_stats rows Diagnose shows, and how many it scans.
_STATS_SAMPLES = 3
_STATS_SCAN_LIMIT = 2000

BACKUP_WARNING = (
    "There is no undo. Back up your Dispatcharr database before running this."
)

# Substrings that mark a setting whose value must never reach the log.
_REDACT = ("password", "secret", "api_key")

_scheduler = None
_scheduler_guard = threading.Lock()


def _ensure_scheduler(context):
    """Start or restart the scheduler to match current settings.

    Never lets a bad setting (e.g. a malformed cron expression) escape to
    the caller - Diagnose/Run/etc. must still return their real result even
    when the schedule can't be armed. Locked, mirroring pipeline.py's
    _lock_guard, so two near-simultaneous calls can't each start a
    Scheduler and leak one's thread.
    """
    global _scheduler  # noqa: PLW0603 - module-level handle so stop() can reach it too
    from . import pipeline, scheduling

    settings = pipeline.load_settings(context)
    with _scheduler_guard:
        if _scheduler is not None:
            _scheduler.stop()
            _scheduler = None
        if not settings["schedule_enabled"]:
            return None
        try:
            new_scheduler = scheduling.Scheduler(
                settings["cron_expression"],
                settings["timezone"],
                lambda: pipeline.start(context, "run"),
            )
            new_scheduler.start()
        except Exception:
            logger.exception(
                "FAILOVERR scheduler not armed: bad cron_expression %r or timezone %r",
                settings["cron_expression"], settings["timezone"],
            )
            return None
        _scheduler = new_scheduler
        return _scheduler


def _flatten(value, path=""):
    """Yield (dotted.path, leaf) pairs. Empty containers are leaves."""
    if isinstance(value, dict) and value:
        for key, item in value.items():
            yield from _flatten(item, f"{path}.{key}" if path else str(key))
    elif isinstance(value, (list, tuple)) and any(
        isinstance(item, (dict, list, tuple)) for item in value
    ):
        for index, item in enumerate(value):
            yield from _flatten(item, f"{path}[{index}]")
    else:
        yield path or "value", value


def _log_report(log, action, label, payload):
    """Log one prefixed line per value.

    Dispatcharr shows nothing in the UI, so `docker logs -f dispatcharr |
    grep FAILOVERR` is the only channel - and grep is line-based, so a
    single multi-line dump would match on its first line only.
    """
    for path, value in _flatten(payload):
        lowered = path.lower()
        shown = "***" if any(word in lowered for word in _REDACT) else value
        log.info("FAILOVERR %s %s.%s = %s", action, label, path, shown)


class Plugin:
    name = "Failoverr"
    version = "0.1.0"
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
                "on one channel before a full run."
            ),
        },
        {
            "id": "match_mode",
            "label": "Match mode",
            "type": "select",
            "default": "strict",
            "options": ["strict", "fuzzy"],
            "help_text": (
                "strict requires an exact token match, so 'RAI 1' never picks "
                "up 'RAI 2' or 'RAI Sport 1'. fuzzy accepts near matches and "
                "will eventually attach a wrong channel - always Preview "
                "before running it."
            ),
        },
        {
            "id": "fuzzy_threshold",
            "label": "Fuzzy match threshold",
            "type": "number",
            "default": 85,
            "help_text": (
                "Only used in fuzzy mode. Lower values match more streams and "
                "more wrong ones: 'RAI 2 HD' scores 80 against 'RAI 1', so "
                "anything at or below 80 will attach the wrong channel."
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
            "id": "order_strategy",
            "label": "Order strategy",
            "type": "select",
            "default": "quality_first",
            "options": ["quality_first", "provider_first"],
            "help_text": (
                "quality_first puts the best stream at position 1 and "
                "alternates providers within each quality tier. "
                "provider_first alternates providers from position 1, giving "
                "better outage protection at the cost of sometimes ranking a "
                "lower-quality stream higher."
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
                "Ceiling across all providers combined, reserved for a "
                "planned parallel-probing mode. Probing is currently "
                "sequential - one probe at a time - regardless of this "
                "setting, so it has no effect on run time yet."
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
                "Standard five-field cron. The default is 04:00 daily. Probing "
                "consumes provider connections, so pick an hour when nobody "
                "is watching."
            ),
        },
        {
            "id": "timezone",
            "label": "Timezone",
            "type": "string",
            "default": "UTC",
            "help_text": (
                "Timezone the cron expression is interpreted in, e.g. "
                "Europe/Rome. Falls back to UTC with a log warning if the "
                "system has no timezone database."
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
                    "channels. " + BACKUP_WARNING
                ),
            },
        },
        {
            "id": "reorder_only",
            "label": "Reorder Only",
            "description": (
                "Re-sorts already attached streams using cached probe data. "
                "No probing, no matching, nothing attached or detached."
            ),
            "confirm": {
                "required": True,
                "title": "Reorder attached streams?",
                "message": (
                    "This will change the failover order on your channels. "
                    + BACKUP_WARNING
                ),
            },
        },
        {
            "id": "probe_only",
            "label": "Probe Only",
            "description": (
                "Refreshes probe data for attached streams. Does not change "
                "which streams are attached or their order."
            ),
            "confirm": {
                "required": True,
                "title": "Probe attached streams?",
                "message": (
                    "This will consume provider connections for several "
                    "minutes and update stored stream statistics. "
                    + BACKUP_WARNING
                ),
            },
        },
        {
            "id": "clear_lock",
            "label": "Clear Lock",
            "description": (
                "Releases a stuck run lock. Use only if a run was interrupted "
                "and Failoverr still reports one in progress."
            ),
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
        log = context.get("logger", logger)
        log.info("FAILOVERR %s START", action)
        _log_report(log, action, "settings", context.get("settings", {}))
        handlers = {
            "diagnose": self._diagnose,
            "preview": self._preview,
            "run": lambda _, c: self._start(c, "run"),
            "reorder_only": lambda _, c: self._start(c, "reorder_only"),
            "probe_only": lambda _, c: self._start(c, "probe_only"),
            "clear_lock": self._clear_lock,
            "show_status": self._show_status,
        }
        handler = handlers.get(action)
        if handler is None:
            log.error("FAILOVERR %s FAILED: unknown action", action)
            return {"status": "error", "message": f"Unknown action: {action}"}
        try:
            result = handler(params or {}, context)
        except Exception as exc:  # surfaced to the log rather than swallowed
            log.exception("FAILOVERR %s FAILED", action)
            return {"status": "error", "message": str(exc)}
        _log_report(log, action, "result", result)
        failed = isinstance(result, dict) and result.get("status") == "error"
        log.info("FAILOVERR %s %s", action, "FAILED" if failed else "COMPLETED")
        return result

    def _diagnose(self, params, context):
        from . import models_access, naming, pipeline

        settings = pipeline.load_settings(context)

        resolved = models_access.resolve_models()
        environment = models_access.environment_report(
            settings["ffprobe_path"], settings["ffmpeg_path"],
        )

        stream_model = resolved.stream_model
        pool_size = stream_model.objects.count()

        # Sample stream_stats without loading the pool: what keys are really
        # in use, and what do three real rows look like?
        key_counts = collections.Counter()
        samples = []
        sampled = 0
        for stats in (
            stream_model.objects.exclude(stream_stats__isnull=True)
            .values_list("stream_stats", flat=True)
            .iterator(chunk_size=500)
        ):
            if not isinstance(stats, dict):
                continue
            key_counts.update(stats.keys())
            if len(samples) < _STATS_SAMPLES:
                samples.append(stats)
            sampled += 1
            if sampled >= _STATS_SCAN_LIMIT:
                break

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
                "has_unique_order_constraint": resolved.has_unique_order_constraint,
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
                "sampled": sampled,
                "stream_stats_keys": dict(key_counts.most_common()),
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
        }
        _ensure_scheduler(context)
        return result

    def _preview(self, params, context):
        from . import pipeline

        return pipeline.run_preview(context)

    def _start(self, context, mode):
        from . import pipeline

        result = pipeline.start(context, mode)
        _ensure_scheduler(context)
        return result

    def _clear_lock(self, params, context):
        from . import pipeline

        context.get("logger", logger).info("FAILOVERR lock cleared by user")
        return pipeline.clear_lock()

    def _show_status(self, params, context):
        from . import pipeline
        from .state import DEFAULT_PATH, State

        state = State.load(DEFAULT_PATH)
        lock = pipeline.lock_status()
        meta = state.meta
        return {
            "status": "ok",
            "running": lock["holder"],
            "execution_model": pipeline.execution_model(),
            "streams_tracked": len(state.streams),
            "last_run": meta.get("last_run"),
            "last_mode": meta.get("last_mode"),
            "degraded_providers": meta.get("degraded_providers") or [],
            "budget_stop": meta.get("budget_stop"),
            "message": (
                f"{'Running: ' + lock['holder'] if lock['holder'] else 'Idle'}. "
                f"{len(state.streams)} streams tracked."
            ),
        }

    def stop(self, context=None):
        """Stop the scheduler. Called on disable/delete/reload."""
        global _scheduler  # noqa: PLW0603 - module-level handle set by _ensure_scheduler
        with _scheduler_guard:
            if _scheduler is not None:
                _scheduler.stop()
                _scheduler = None
