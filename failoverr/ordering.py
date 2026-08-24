"""Quality ranking, provider interleaving, and order-rewrite planning.

Pure module: imports nothing from Django or Dispatcharr.
"""

import re
from collections import defaultdict
from itertools import zip_longest
from typing import NamedTuple

DEFAULT_CODEC_PRIORITY = ("hevc", "h265", "h264", "avc")

# Response time is bucketed to this granularity before ranking. Response
# time is a near-continuous value (network jitter); at raw millisecond
# precision, quality_first's exact-tie bucketing (see _quality_first) would
# almost never fire, silently disabling provider interleaving wherever
# response time is in play. Bucketing restores enough ties for interleaving
# to still work, the same trick already used for resolution tiers.
DEFAULT_RESPONSE_TIME_BUCKET_MS = 250

# rewrite_plan bumps existing rows by this much when a unique (channel,
# order) constraint requires clearing space for the final 0..n-1 positions.
# Public and shared: models_access.placeholder_orders starts its create-time
# orders past this same value, and that disjointness is what keeps the two
# halves of the offset trick from colliding. One constant, not two copies -
# a divergence of 1..n (n = rows attached in a pass) silently collides.
ORDER_OFFSET = 100000

# (minimum height, tier). Higher tier sorts first.
_TIERS = ((2160, 4), (1440, 3), (1080, 2), (720, 1))

_RESOLUTION = re.compile(r"(\d+)\s*[xX*]\s*(\d+)")


class Candidate(NamedTuple):
    stream_id: int
    name: str
    provider_id: int | None
    stats: dict
    response_time_ms: float | None = None


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
    """Negative index so that better codecs sort higher under reverse=True."""
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


def _response_time_component(response_time_ms, bucket_ms):
    """Bucketed, negated so a lower response time sorts higher.

    Missing/never-measured response time sorts worst, mirroring how an
    unlisted codec sorts worst in _codec_rank.
    """
    if response_time_ms is None:
        return float("-inf")
    bucket_ms = max(1, int(bucket_ms))
    return -((int(response_time_ms) // bucket_ms) * bucket_ms)


def quality_key(  # noqa: PLR0913, PLR0917 - one ranking factor per toggle
    stats,
    codec_priority=DEFAULT_CODEC_PRIORITY,
    response_time_ms=None,
    response_time_bucket_ms=DEFAULT_RESPONSE_TIME_BUCKET_MS,
    rank_by_resolution=True,
    rank_by_response_time=True,
    rank_by_codec=True,
    rank_by_fps=True,
    rank_by_bitrate=True,
):
    """Sort key, descending. Derived from probe data only, never from names.

    Order is fixed: resolution -> response time -> codec -> fps -> bitrate,
    with height as an unconditional final tiebreaker that is never itself
    toggleable — it is a sub-tier tiebreak, not an independent factor. Each
    factor above can be individually disabled; a disabled factor is omitted
    from the key entirely rather than zeroed, so it plays no role at all.
    """
    stats = stats or {}
    height = _height(stats)
    key = []
    if rank_by_resolution:
        key.append(_tier(height))
    if rank_by_response_time:
        key.append(
            _response_time_component(response_time_ms, response_time_bucket_ms)
        )
    if rank_by_codec:
        key.append(_codec_rank(stats, codec_priority))
    if rank_by_fps:
        key.append(_number(stats.get("source_fps")))
    if rank_by_bitrate:
        key.append(_number(stats.get("video_bitrate")))
    key.append(height)
    return tuple(key)


def _group_by_provider(candidates):
    """Group candidates by provider, ids sorted for determinism across runs."""
    groups = defaultdict(list)
    for candidate in candidates:
        groups[candidate.provider_id].append(candidate)
    return [groups[key] for key in sorted(groups, key=str)]


def _interleave(groups):
    """Round-robin across groups.

    zip_longest degrades correctly when one provider has fewer entries
    than another.
    """
    return [c for row in zip_longest(*groups) for c in row if c is not None]


def _quality_first(candidates, codec_priority, response_time_bucket_ms, toggles):
    buckets = defaultdict(list)
    for candidate in candidates:
        key = quality_key(
            candidate.stats, codec_priority, candidate.response_time_ms,
            response_time_bucket_ms, **toggles,
        )
        buckets[key].append(candidate)
    ordered = []
    for key in sorted(buckets, reverse=True):
        ordered.extend(_interleave(_group_by_provider(buckets[key])))
    return ordered


def _provider_first(candidates, codec_priority, response_time_bucket_ms, toggles):
    def _key(candidate):
        return quality_key(
            candidate.stats, codec_priority, candidate.response_time_ms,
            response_time_bucket_ms, **toggles,
        )

    ranked = [
        sorted(group, key=_key, reverse=True)
        for group in _group_by_provider(candidates)
    ]
    return _interleave(ranked)


def order_candidates(  # noqa: PLR0913, PLR0917 - one ranking factor per toggle
    candidates,
    strategy="quality_first",
    codec_priority=DEFAULT_CODEC_PRIORITY,
    response_time_bucket_ms=DEFAULT_RESPONSE_TIME_BUCKET_MS,
    rank_by_resolution=True,
    rank_by_response_time=True,
    rank_by_codec=True,
    rank_by_fps=True,
    rank_by_bitrate=True,
):
    """Rank candidates and interleave providers.

    Known limitation (spec §11, documented not fixed): under quality_first,
    if one bucket holds only provider A and the next bucket also starts with
    A, two A entries appear consecutively. Interleaving is within-bucket by
    design.
    """
    candidates = list(candidates)
    if not candidates:
        return []
    toggles = {
        "rank_by_resolution": rank_by_resolution,
        "rank_by_response_time": rank_by_response_time,
        "rank_by_codec": rank_by_codec,
        "rank_by_fps": rank_by_fps,
        "rank_by_bitrate": rank_by_bitrate,
    }
    if strategy == "provider_first":
        return _provider_first(
            candidates, codec_priority, response_time_bucket_ms, toggles
        )
    return _quality_first(
        candidates, codec_priority, response_time_bucket_ms, toggles
    )


def rewrite_plan(current, desired, use_offset):
    """Ordered (stream_id, new_order) assignments to reach `desired`.

    When a unique (channel, order) constraint exists, every existing row is
    first bumped by ORDER_OFFSET — which preserves relative uniqueness — so that
    final positions 0..n-1 are free to assign in any order.

    An empty `desired` returns an empty plan: a channel that matched nothing
    is never cleared (spec §12).
    """
    if not desired:
        return []
    plan = []
    if use_offset:
        for stream_id, order in sorted(current.items(), key=lambda kv: kv[1]):
            plan.append((stream_id, order + ORDER_OFFSET))
    plan.extend((stream_id, index) for index, stream_id in enumerate(desired))
    return plan
