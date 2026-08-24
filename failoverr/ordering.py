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
# precision, quality_first's exact-tie bucketing (see order_candidates) would
# almost never fire, silently disabling provider interleaving wherever
# response time is in play. Bucketing restores enough ties for interleaving
# to still work, the same trick already used for resolution tiers. Not
# user-configurable: this value already balances tie granularity against
# interleaving, and the field it used to back only ever invited detuning it.
DEFAULT_RESPONSE_TIME_BUCKET_MS = 250

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


def _response_time_component(response_time_ms):
    """Bucketed, negated so a lower response time sorts higher.

    Missing/never-measured response time sorts worst, mirroring how an
    unlisted codec sorts worst in _codec_rank.
    """
    if response_time_ms is None:
        return float("-inf")
    bucket_ms = DEFAULT_RESPONSE_TIME_BUCKET_MS
    return -((int(response_time_ms) // bucket_ms) * bucket_ms)


def quality_key(
    stats,
    codec_priority=DEFAULT_CODEC_PRIORITY,
    response_time_ms=None,
    rank_by_bitrate=True,
):
    """Sort key, descending. Derived from probe data only, never from names.

    Order is fixed: resolution -> response time -> codec -> fps -> bitrate,
    with height as an unconditional final tiebreaker. Only bitrate is
    toggleable - it is the one factor that, left on, tends to break every
    other tie before the provider interleaving in order_candidates ever gets
    a chance to fire (see the field's help_text).
    """
    stats = stats or {}
    height = _height(stats)
    key = [
        _tier(height),
        _response_time_component(response_time_ms),
        _codec_rank(stats, codec_priority),
        _number(stats.get("source_fps")),
    ]
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


def order_candidates(
    candidates,
    codec_priority=DEFAULT_CODEC_PRIORITY,
    rank_by_bitrate=True,
):
    """Rank candidates by quality, interleaving providers within each tier.

    Known limitation (spec §11, documented not fixed): if one bucket holds
    only provider A and the next bucket also starts with A, two A entries
    appear consecutively. Interleaving is within-bucket by design.
    """
    candidates = list(candidates)
    if not candidates:
        return []
    buckets = defaultdict(list)
    for candidate in candidates:
        key = quality_key(
            candidate.stats, codec_priority, candidate.response_time_ms,
            rank_by_bitrate,
        )
        buckets[key].append(candidate)
    ordered = []
    for key in sorted(buckets, reverse=True):
        ordered.extend(_interleave(_group_by_provider(buckets[key])))
    return ordered


def rewrite_plan(desired):
    """Ordered (stream_id, new_order) assignments to reach `desired`.

    An empty `desired` returns an empty plan: a channel that matched nothing
    is never cleared (spec §12).
    """
    return [(stream_id, index) for index, stream_id in enumerate(desired)]
