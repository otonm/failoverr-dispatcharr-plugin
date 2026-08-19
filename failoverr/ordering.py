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
