"""Phase 4 checkpoint: probe real streams from the live xstream pool.

Not part of the plugin — this never gets copied to /data/plugins. It's a
throwaway diagnostic for the one manual check pytest can't cover: that
exhausting a real provider's connection cap classifies as inconclusive,
never invalid (CLAUDE.md §7 / phase4 plan checkpoint).

Usage, from Dispatcharr's own directory inside the LXC (failoverr must
already be installed at /data/plugins/failoverr, or set
DISPATCHARR_PLUGINS_DIR):

    python manage.py shell < /path/to/failoverr-dispatcharr-plugin/scripts/\
probe_checkpoint.py

Tune via environment variables before running:
    FFPROBE_PATH        default /usr/local/bin/ffprobe
    PROBE_TIMEOUT       default 20 (seconds)
    CAP_TEST_COUNT      default 8  (concurrent probes fired at one account
                                     to try to exceed its connection cap;
                                     raise this if nothing trips)
"""

import os
import sys
import threading

from django.db.models import Count

try:
    from failoverr import models_access, probing
except ImportError:
    sys.path.insert(0, os.environ.get("DISPATCHARR_PLUGINS_DIR", "/data/plugins"))
    from failoverr import models_access, probing

FFPROBE_PATH = os.environ.get("FFPROBE_PATH", "/usr/bin/ffprobe")
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "20"))
CAP_TEST_COUNT = int(os.environ.get("CAP_TEST_COUNT", "8"))


def show(label, result):
    print(f"  {label}: verdict={result.verdict!r} reason={result.reason!r}")
    if result.stats:
        print(f"    stats={result.stats}")


print("=== environment ===")
FFMPEG_PATH = FFPROBE_PATH.replace("ffprobe", "ffmpeg")
env = models_access.environment_report(FFPROBE_PATH, FFMPEG_PATH)
print(env)

resolved = models_access.resolve_models()
provider_field = resolved.provider_field
Stream = resolved.stream_model

print(f"\n=== picking a busy provider account (field: {provider_field}) ===")
busy = (
    Stream.objects.exclude(url="")
    .values(provider_field)
    .annotate(n=Count("id"))
    .filter(n__gte=CAP_TEST_COUNT)
    .order_by("-n")
    .first()
)
if not busy:
    print(
        f"No single account has >= {CAP_TEST_COUNT} streams with a URL. "
        "Lower CAP_TEST_COUNT or pick a bigger provider manually."
    )
    sys.exit(1)

account_id = busy[provider_field]
sample = list(
    Stream.objects.filter(**{provider_field: account_id})
    .exclude(url="")
    .order_by("?")
    .values_list("id", "url", "name")[:CAP_TEST_COUNT]
)
print(f"account={account_id} sample_size={len(sample)}")

print("\n=== single-stream sanity probe ===")
first_id, first_url, first_name = sample[0]
print(f"probing stream {first_id} ({first_name!r})")
show("result", probing.probe(first_url, FFPROBE_PATH, PROBE_TIMEOUT))

print(
    f"\n=== connection-cap exhaustion: {len(sample)} concurrent probes "
    f"against ONE account ===\n"
    "(the point: if this account caps concurrency below the sample size, "
    "the excess probes must come back INCONCLUSIVE, never INVALID)"
)
results = [None] * len(sample)


def worker(i, url):
    results[i] = probing.probe(url, FFPROBE_PATH, PROBE_TIMEOUT)


threads = [
    threading.Thread(target=worker, args=(i, url))
    for i, (_id, url, _name) in enumerate(sample)
]
for t in threads:
    t.start()
for t in threads:
    t.join()

for (sid, _url, name), result in zip(sample, results, strict=True):
    show(f"stream {sid} ({name!r})", result)

verdicts = [r.verdict for r in results]
if "invalid" in verdicts and "valid" not in verdicts:
    print(
        "\n!!! Every failure classified as INVALID and none VALID — if this "
        "account actually has a connection cap below the sample size, this "
        "is the bug the whole classification scheme exists to prevent. "
        "Inspect the raw stderr (rerun probing.run_command directly) before "
        "trusting these streams are really dead."
    )
else:
    print(
        "\nNo all-invalid pattern seen. If you expected the cap to trip and "
        "it didn't, the account tolerates this many connections — raise "
        "CAP_TEST_COUNT and try again."
    )
