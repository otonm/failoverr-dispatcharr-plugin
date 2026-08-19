"""Sidecar state: probe cache, TTL, and removal hysteresis.

Pure module: imports nothing from Django or Dispatcharr.

This file is also the resume mechanism. A run that stops early leaves
fresh entries behind, and the next run skips them (spec §4.1).

Plugin-private bookkeeping lives here and never in Stream.stream_stats.
"""

import hashlib
import json
import os
import pathlib
import time

VALID = "valid"
INVALID = "invalid"
INCONCLUSIVE = "inconclusive"

DEFAULT_PATH = "/data/failoverr/state.json"


def url_hash(url):
    return hashlib.sha256(str(url or "").encode("utf-8")).hexdigest()[:16]


class State:
    def __init__(self, path=DEFAULT_PATH, streams=None, meta=None):
        self.path = pathlib.Path(path)
        self.streams = streams or {}
        self.meta = meta or {}

    @classmethod
    def load(cls, path=DEFAULT_PATH):
        path = pathlib.Path(path)
        try:
            data = json.loads(path.read_text())
        except (OSError, ValueError):
            # Missing or truncated: start clean rather than brick the plugin.
            return cls(path)
        return cls(path, data.get("streams") or {}, data.get("meta") or {})

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            {"streams": self.streams, "meta": self.meta}, indent=1, sort_keys=True
        )
        tmp = self.path.with_name(self.path.name + f".tmp{os.getpid()}")
        tmp.write_text(payload)
        tmp.replace(self.path)  # atomic

    def _entry(self, stream_id):
        return self.streams.get(str(stream_id)) or {}

    def is_fresh(self, stream_id, url, ttl_hours, now=None):
        entry = self._entry(stream_id)
        if not entry:
            return False
        if entry.get("verdict") == INCONCLUSIVE:
            # "Ask again" — caching this would mean never retrying it.
            return False
        if entry.get("url_hash") != url_hash(url):
            return False
        now = time.time() if now is None else now
        return (now - float(entry.get("last_probe", 0))) < ttl_hours * 3600

    def record(self, stream_id, url, verdict, now=None):
        now = time.time() if now is None else now
        entry = dict(self._entry(stream_id))
        failures = int(entry.get("failures", 0))
        if verdict == VALID:
            failures = 0
        elif verdict == INVALID:
            failures += 1
        # INCONCLUSIVE leaves the counter untouched — spec §9.
        entry.update(
            {
                "url_hash": url_hash(url),
                "last_probe": now,
                "verdict": verdict,
                "failures": failures,
            }
        )
        self.streams[str(stream_id)] = entry

    def failure_count(self, stream_id):
        return int(self._entry(stream_id).get("failures", 0))

    def last_verdict(self, stream_id):
        return self._entry(stream_id).get("verdict")

    def should_remove(self, stream_id, threshold):
        return self.failure_count(stream_id) >= max(1, int(threshold))
