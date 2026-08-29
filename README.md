# Failoverr

A [Dispatcharr](https://github.com/Dispatcharr/Dispatcharr) plugin that keeps
your custom channels stocked with working streams.

You create a channel — say, `RAI 1` — and Failoverr finds every stream across
all your M3U playlists whose name matches it (`IT: RAI 1 HEVC`, `Rai Uno FHD`,
`RAI1 [Backup]`, ...), checks with `ffprobe` that each one actually plays
(video, audio, not dead, not blank), and attaches the working ones in
best-quality-first order — alternating between providers so a single
provider outage doesn't take down your whole failover list. It re-checks
everything on a schedule, so a stream that goes dark gets pushed down and
eventually removed, and a stream that comes back gets restored automatically.

## What it does

1. Matches every stream in your M3U pool to your existing channels by name.
2. Probes each candidate stream with `ffprobe` — only ones that are actually
   playable (real video, real audio) are ever attached.
3. Orders each channel's streams by measured quality (resolution, response
   time, codec, frame rate, bitrate), interleaving providers so failover
   doesn't depend on one provider staying up.
4. Re-probes streams you already have attached, demotes or removes ones that
   fail repeatedly, and re-ranks everything as quality changes.
5. Runs on a cron schedule so this all happens without you.

## Installing

### From a release (recommended)

1. Download `failoverr-vX.Y.Z.zip` from the
   [Releases page](https://github.com/otonm/failoverr-dispatcharr-plugin/releases/latest).
2. Extract it — this produces a `failoverr/` directory containing
   `plugin.py`, `plugin.json`, and the rest of the plugin's code.
3. Copy that `failoverr/` directory into Dispatcharr's plugins directory,
   alongside any other plugins you have installed.
4. In Dispatcharr, go to **Settings > Plugins**, find Failoverr, and enable
   it. Dispatcharr loads `plugin.py` directly — no build step, no
   dependencies to install.

To upgrade, replace the `failoverr/` directory with the new release's and
re-enable the plugin; your settings, probe cache, and reports are stored
outside the plugin directory (`/data/failoverr/`, `/data/exports/`) and
survive the upgrade.

### From source

Clone this repository and copy (or symlink) its `failoverr/` directory into
Dispatcharr's plugins directory the same way — useful if you want to track
`main` instead of a tagged release.

## First run

1. **Diagnose** (read-only) — confirms Failoverr can see your Dispatcharr
   models, that `ffprobe`/`ffmpeg` are on the configured paths, and shows a
   few examples of how your channel names normalize for matching. Run this
   first, and again any time something looks wrong.
2. Leave **Dry run** on and press **Preview** — read-only, shows exactly
   which streams would be attached and in what order, without probing or
   changing anything. Check the CSV report under `/data/exports/`.
3. Turn **Dry run** off and press **Run** for real, ideally starting with a
   single channel via the **Channel names** setting so you can check the
   result before turning it loose on your whole lineup.
4. **Back up your Dispatcharr database before your first real run.** Run
   attaches, detaches, and reorders streams on real channels — there is no
   undo built into the plugin.

## Actions

| Action | Effect |
|---|---|
| **Diagnose** | Read-only. Reports resolved database fields, ffprobe/ffmpeg version, stream pool size, and channel-name normalization examples. |
| **Preview** | Read-only. Shows what a real run would attach/detach/reorder, using already-cached probe data. Writes a CSV, probes nothing. |
| **Run** | The full pipeline: match, probe, order, write. Asks for confirmation. |
| **Stop** | Asks a running Run to stop at its next checkpoint. Cooperative — the probe already in flight finishes first. |
| **Clear State** | Wipes the probe cache (verdicts, failure counters). Every stream is re-checked from scratch on the next run. Does not touch attached streams. |
| **Show Status** | Current progress of a running job, or the outcome of the last one. |

## Key settings

Every setting has its own `help_text` in the Dispatcharr UI describing the
consequence of changing it — this is a quick map of the ones worth knowing
about up front.

- **Dry run** — on by default. Computes everything and writes a report, but
  never touches your channels. Turn off once a Preview looks right.
- **ffprobe path / ffmpeg path** — must point at real binaries inside the
  Dispatcharr container. Diagnose confirms these before you rely on them.
  ffmpeg is only used for blank-screen detection.
- **Channel group / Channel names** — scope a run to part of your lineup.
  Channel names (one per line) is the fastest way to test on a single
  channel; it takes priority over the group filter.
- **Quality tokens to ignore** — resolution/codec/misc tokens (`HD`, `4K`,
  `HEVC`, `backup`, ...) stripped before matching, so `RAI 1 HD` and `RAI 1
  4K` both reduce to `rai 1`.
- **Treat spelled-out numbers as digits** — maps `Rai Uno` → `RAI 1` in
  English, Italian, German, and French.
- **Codec priority / Rank by bitrate** — tune how ties in resolution are
  broken. Turning off "rank by bitrate" makes exact quality ties more common,
  which is what triggers provider interleaving more often.
- **Max streams per channel** — how many streams a channel keeps after
  ordering; extras are detached.
- **Probe cache lifetime (hours)** — a stream probed more recently than this
  is skipped, which is what makes repeat runs fast.
- **Concurrent probes per provider / Cooldown between probes / Global
  concurrent probes** — throttle probing so Failoverr's own testing doesn't
  trip your provider's connection limits. If unsure, leave per-provider
  concurrency at 1.
- **Failures before removal** — a stream is only detached after this many
  consecutive runs find it genuinely broken; earlier failures just demote it.
- **Detect blank (black) streams** — samples a few seconds of video and
  rejects an all-black picture. Off by default; roughly doubles run time.
- **Max probes per run / Max run time** — safety stops, not normal limits. A
  typical run uses far less than either default. Stopping early is not
  destructive — progress is saved and the next run continues from there.
- **Mark channels with no valid streams** — appends a suffix (default
  ` [BROKEN]`) to a channel's name once every attached stream is confirmed
  dead, and removes it automatically once the channel recovers.
- **Run on a schedule / Schedule (cron)** — arms a `django-celery-beat`
  schedule. It arms the next time any action button is pressed (Diagnose is
  read-only and safe to use for this), not the instant you flip the toggle —
  press an action again after changing settings to pick up the new values.
  Requires `django-celery-beat` to be installed.

## Reports

Every Preview or Run writes a CSV to `/data/exports/failoverr-<action>-
<timestamp>.csv` listing, per channel, every stream considered and what
happened to it (attached, kept, detached, matched-but-not-probed,
outranked, ...). The 100 most recent reports are kept; older ones are
pruned automatically.

## Troubleshooting

- **Nothing gets attached** — run Diagnose first and check the ffprobe path
  is correct; a wrong path makes every probe come back inconclusive.
- **A channel matches nothing** — check Diagnose's normalization examples
  against your channel and stream names; a channel name containing a word
  that's also in "Quality tokens to ignore" will fail to match.
- **A schedule doesn't fire after being enabled** — a freshly-enabled or
  freshly-changed schedule only takes effect once Dispatcharr's celery
  worker next restarts or forks a new child process.
- **A provider starts erroring mid-run** — raise the cooldown or lower
  concurrency for that provider; Failoverr's own probing can trip a
  provider's connection limit and get misread as dead streams if pushed
  too hard.
