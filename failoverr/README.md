# Back up your database first

**There is no undo.** Failoverr attaches, detaches, and reorders streams on
your channels. Before you turn `dry_run` off and run it for real, back up
your Dispatcharr database. Every mutating action's confirmation dialog
repeats this warning — it is not optional.

# Failoverr

Version 0.1.0, by otonvm.

Matches M3U streams to your channels, probes them for real validity and
quality, and maintains failover order with providers interleaved.

## What it does

You create channels by hand. Failoverr does the rest, on a schedule:

1. It finds every stream across your imported M3U playlists whose name
   matches a channel you created.
2. It probes each candidate stream with `ffprobe` (and optionally `ffmpeg`
   for blank-screen detection) and only attaches ones that actually work —
   not dead, not blank, not missing audio.
3. It orders the working streams by measured quality (resolution, codec,
   fps, bitrate — never by name, because provider names lie about quality)
   and interleaves them by provider, so a single provider outage doesn't
   burn through several consecutive fallback positions at once.
4. It re-probes streams already attached to a channel, updates their stored
   stats, demotes or removes ones that stop working, and re-ranks the whole
   list if quality changed.
5. It runs on a cron schedule you set, so this stays current without you
   doing it by hand.

**Terminology**, because Dispatcharr's names don't match how people talk
about this:

- A **Stream** is one entry from an M3U playlist — the raw thing Failoverr
  finds and probes. There can be tens of thousands of these across your
  playlists.
- A **Channel** is a lineup entry you created by hand, with many Streams
  attached in failover order. Dispatcharr walks that order when a stream
  fails.

## Install

Copy the `failoverr/` folder to `/data/plugins/failoverr/` and reload
plugins in Dispatcharr. No other setup is required — the plugin runs
inside the Dispatcharr backend process and needs no URL, username, or
password.

## First run procedure

Follow this order. Do not skip steps or turn `dry_run` off early.

1. **Run Diagnose.** It's read-only. Confirm `ffprobe` was found at the
   configured path and reports a version, and that the reported stream pool
   size looks right for your setup.
2. **Leave `dry_run` ON (it's on by default) and run Preview.** Preview
   matches and orders using only probe data already cached — it probes
   nothing and changes nothing. It writes a CSV to `/data/exports/`.
3. **Read that CSV.** If the matching looks wrong — a channel picked up
   streams that don't belong to it, or missed ones that should have
   matched — adjust the **Quality tokens to ignore** setting, not the match
   mode. Wrong matches are almost always a token that should have been
   stripped from the name (or shouldn't have been) rather than a strict/fuzzy
   problem.
4. **Run "Run" with `dry_run` still ON.** This exercises the full pipeline,
   including probing, and writes a fresh report — but still changes
   nothing on your channels. Read that report the same way.
5. **Only once that report looks right, turn `dry_run` OFF.** Then run
   again, ideally against a small channel selection first (see the
   "Channel names" setting) before trusting it on your whole lineup.

## Every setting

- **Dry run** (`dry_run`, default **on**) — ON: computes everything and
  writes a CSV report, but never attaches, detaches, or reorders a stream.
  Probe results are still saved, so turning this off afterwards runs almost
  instantly. Leave it on until a Preview looks right.
- **ffprobe path** (`ffprobe_path`, default `/usr/local/bin/ffprobe`) —
  wrong path means every probe fails as inconclusive and nothing is ever
  attached. Run Diagnose to confirm this path exists before your first run.
- **ffmpeg path** (`ffmpeg_path`, default `/usr/local/bin/ffmpeg`) — used
  only for blank-screen detection. Ignored when that setting is off.
- **Probe timeout (seconds)** (`probe_timeout_seconds`, default `15`) —
  lower is faster but marks slow-but-working streams as inconclusive, so
  they keep their old ranking and are retried next run. A timeout never
  counts toward removal.
- **Channel group** (`channel_group`, default blank) — only channels in
  this group are touched. Blank means every channel, which on a large
  lineup is a much longer run.
- **Channel names** (`channel_names`, default blank) — one channel name per
  line. When set, only these channels are processed and the group filter is
  ignored. Use this to test on one channel before a full run.
- **Match mode** (`match_mode`, default `strict`) — strict requires an
  exact token match, so "RAI 1" never picks up "RAI 2" or "RAI Sport 1".
  fuzzy accepts near matches and will eventually attach a wrong channel —
  always Preview before running it.
- **Fuzzy match threshold** (`fuzzy_threshold`, default `85`) — only used
  in fuzzy mode. Lower values match more streams and more wrong ones:
  "RAI 2 HD" scores 80 against "RAI 1", so anything at or below 80 will
  attach the wrong channel.
- **Quality tokens to ignore** (`strip_tokens`, default `4k,uhd,fhd,hd,sd,
  hevc,h265,h264,avc,raw,fullhd,ultrahd,1080p,1080i,720p,576p,480p,multi,
  backup,alt`) — comma separated. These are removed from names before
  matching, so "RAI 1 HD" and "RAI 1 4K" both reduce to "rai 1". Removing a
  token here that is part of a real channel name will break that channel's
  matching.
- **Treat spelled-out numbers as digits** (`map_number_words`, default
  **on**) — maps one-to-ten in English, Italian, German and French, so
  "Rai Uno" matches the channel "RAI 1". Turn off if your channel names
  contain those words literally.
- **Codec priority** (`codec_priority`, default `hevc,h265,h264,avc`) —
  best first, comma separated. Codecs not listed sort last. This only
  breaks ties within a resolution tier — a 1080p h264 stream always
  outranks a 720p HEVC one.
- **Order strategy** (`order_strategy`, default `quality_first`) —
  quality_first puts the best stream at position 1 and alternates providers
  within each quality tier. provider_first alternates providers from
  position 1, giving better outage protection at the cost of sometimes
  ranking a lower-quality stream higher.
- **Max streams per channel** (`max_streams_per_channel`, default `10`) —
  streams beyond this are detached after ordering. Too low loses working
  fallbacks; too high makes Dispatcharr walk a long list of poor sources
  during an outage.
- **Probe cache lifetime (hours)** (`probe_ttl_hours`, default `24`) —
  streams probed more recently than this are skipped entirely, which is
  what makes repeat runs fast. A stream whose URL changed is always
  re-probed regardless of this setting.
- **Concurrent probes per provider** (`per_account_concurrency`, default
  `1`) — must not exceed your provider's connection limit. Setting this too
  high makes your own probes fail each other, and the plugin then concludes
  that working streams are dead. If unsure, leave it at 1.
- **Cooldown between probes per provider** (`account_cooldown_seconds`,
  default `2`) — pause after each probe on the same provider. Raise it if a
  provider starts returning connection-limit errors partway through a run.
- **Global concurrent probes** (`global_concurrency`, default `4`) —
  ceiling across all providers combined. Different providers are probed in
  parallel, which is most of the available speedup, but each is still
  limited by the per-provider setting above.
- **Failures before removal** (`removal_failure_threshold`, default `3`) —
  a stream is detached only after this many consecutive runs found it
  genuinely broken. Earlier failures just move it to the bottom of the
  list. Setting this to 1 will detach streams over a single provider
  hiccup.
- **Detect blank (black) streams** (`blank_detect`, default **off**) —
  samples a few seconds of video and rejects an all-black picture. Roughly
  doubles run time, which at this lineup size is still only a few extra
  minutes. Any ffmpeg error leaves the stream accepted rather than
  rejected.
- **Blank detection sample (seconds)** (`blank_detect_seconds`, default
  `5`) — longer samples are more reliable but slower. A stream is only
  rejected when essentially the whole sample is black.
- **Max probes per run** (`max_probes_per_run`, default `400`) — safety
  stop, not a normal limit — a typical run uses about 200. On reaching it
  the run stops cleanly and keeps what it learned; the next run continues
  where this one left off.
- **Max run time (minutes)** (`max_run_minutes`, default `60`) — safety
  stop for a provider that hangs every connection. A typical run takes 10
  to 20 minutes. Stopping early is not destructive — progress is saved.
- **Run on a schedule** (`schedule_enabled`, default **off**) — starts the
  scheduler when the plugin is enabled and survives container restarts.
  Turn dry run off first, or the scheduled run will change nothing.
- **Schedule (cron)** (`cron_expression`, default `0 4 * * *`) — standard
  five-field cron. The default is 04:00 daily. Probing consumes provider
  connections, so pick an hour when nobody is watching.
- **Timezone** (`timezone`, default `UTC`) — timezone the cron expression
  is interpreted in, e.g. `Europe/Rome`. Falls back to UTC with a log
  warning if the system has no timezone database.

## How long it takes

For a lineup around 20 channels with roughly 10 candidate streams each,
expect **10 to 20 minutes** for a full Run. Turning on blank detection
roughly doubles that. Larger catalogs take longer — probing is
single-threaded per provider (respecting each provider's connection cap),
so a run over thousands of candidates can take hours; the plugin is
designed to stop cleanly on its time/probe budget and resume from where it
left off on the next run rather than starting over.

Run and Probe Only execute in the background — pressing the button returns
immediately, and the Dispatcharr UI stays responsive while it works. Use
**Show Status** to check progress, budget consumption, and whether any
provider was aborted this run.

## Known limitations

- Under `quality_first` ordering, two streams from the same provider can
  appear consecutively when one quality tier holds only that provider —
  interleaving happens within each tier, not across tiers.
- **Fuzzy match mode will eventually attach a wrong channel.** "RAI 2 HD"
  scores 80 against "RAI 1" — with the default fuzzy threshold of 85 that
  particular case is excluded, but lowering the threshold (or trusting
  fuzzy mode blindly) will eventually pull in something that doesn't
  belong. Always Preview before relying on it.
- Ranking uses probe data only, never the stream's name. A channel whose
  streams have never been probed has no quality data to rank by and is
  left alone until a probe runs.
- Blank-screen detection fails open: any ffmpeg error while checking for a
  black picture leaves the stream accepted rather than rejected.
- Streams are only ever detached from a channel, never deleted from the
  M3U pool. Failoverr does not touch the stream pool itself, channel
  creation, EPG, or logos.

## Troubleshooting

**Show Status reports a run as degraded / lists a degraded provider.**
A provider is aborted for the rest of that run once it has produced at
least 5 probe verdicts and none of them were valid — the assumption being
that a provider failing that consistently is down or rate-limiting, not
that five of its streams all happen to be dead. Nothing is removed from
that provider's streams when this happens; their existing ranking and
attachment are left untouched, and they'll be retried on the next run.

**An action says an operation is already running, but nothing seems to be
happening.** Runs take an exclusive lock so two can't overlap; a lock is
automatically considered stale after 30 minutes. If a run was killed and
the lock didn't clear, use the **Clear Lock** action to release it, then
check **Show Status** to confirm it's idle before starting a new run.
