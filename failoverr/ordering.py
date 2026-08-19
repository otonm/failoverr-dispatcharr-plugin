"""Quality ranking, provider interleaving, and order-rewrite planning.

Pure module: imports nothing from Django or Dispatcharr.
"""

import re
from collections import defaultdict
from itertools import zip_longest
from typing import Any, NamedTuple

DEFAULT_CODEC_PRIORITY = ("hevc", "h265", "h264", "avc")

# (minimum height, tier). Higher tier sorts first.
_TIERS = ((2160, 4), (1440, 3), (1080, 2), (720, 1))

_RESOLUTION = re.compile(r"(\d+)\s*[xX*]\s*(\d+)")


class Candidate(NamedTuple):
    stream_id: int
    name: str
    provider_id: Any
    stats: dict


def _height(stats):
    match = _RESOLUTION.search(str(stats.get("resolution") or ""))
    if match:
        return int(match.group(2))
    try:
        return int(stats.get("height") or 0)
    except (TypeError, ValueError):
        return 0


def _tier(height):
    for minimum, tier in _TIERS:
        if height >= minimum:
            return tier
    return 0


def _codec_rank(stats, codec_priority):
    """Negative index so that better codecs sort higher under reverse=False."""
    codec = str(stats.get("video_codec") or "").lower()
    priority = list(codec_priority)
    if codec in priority:
        return -priority.index(codec)
    return -len(priority) - 1


def _number(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def quality_key(stats, codec_priority=DEFAULT_CODEC_PRIORITY):
    """Sort key, descending. Derived from probe data only, never from names."""
    stats = stats or {}
    height = _height(stats)
    return (
        _tier(height),
        _codec_rank(stats, codec_priority),
        _number(stats.get("fps")),
        _number(stats.get("bitrate_kbps")),
        height,
    )


def _group_by_provider(candidates):
    """Provider ids sorted for determinism across runs."""
    groups = defaultdict(list)
    for candidate in candidates:
        groups[candidate.provider_id].append(candidate)
    return [groups[key] for key in sorted(groups, key=lambda p: str(p))]


def _interleave(groups):
    """Round-robin across groups. zip_longest degrades correctly when one
    provider has fewer entries than another."""
    return [c for row in zip_longest(*groups) for c in row if c is not None]


def _quality_first(candidates, codec_priority):
    buckets = defaultdict(list)
    for candidate in candidates:
        buckets[quality_key(candidate.stats, codec_priority)].append(candidate)
    ordered = []
    for key in sorted(buckets, reverse=True):
        ordered.extend(_interleave(_group_by_provider(buckets[key])))
    return ordered


def _provider_first(candidates, codec_priority):
    ranked = [
        sorted(group, key=lambda c: quality_key(c.stats, codec_priority), reverse=True)
        for group in _group_by_provider(candidates)
    ]
    return _interleave(ranked)


def order_candidates(candidates, strategy="quality_first", codec_priority=DEFAULT_CODEC_PRIORITY):
    """Rank candidates and interleave providers.

    Known limitation (spec §11, documented not fixed): under quality_first,
    if one bucket holds only provider A and the next bucket also starts with
    A, two A entries appear consecutively. Interleaving is within-bucket by
    design.
    """
    candidates = list(candidates)
    if not candidates:
        return []
    if strategy == "provider_first":
        return _provider_first(candidates, codec_priority)
    return _quality_first(candidates, codec_priority)


def rewrite_plan(current, desired, use_offset, offset=100000):
    """Ordered (stream_id, new_order) assignments to reach `desired`.

    When a unique (channel, order) constraint exists, every existing row is
    first bumped by `offset` — which preserves relative uniqueness — so that
    final positions 0..n-1 are free to assign in any order.

    An empty `desired` returns an empty plan: a channel that matched nothing
    is never cleared (spec §12).
    """
    if not desired:
        return []
    plan = []
    if use_offset:
        for stream_id, order in sorted(current.items(), key=lambda kv: kv[1]):
            plan.append((stream_id, order + offset))
    plan.extend((stream_id, index) for index, stream_id in enumerate(desired))
    return plan
