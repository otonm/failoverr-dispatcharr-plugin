"""Channel and stream name normalization and matching.

Pure module: imports nothing from Django or Dispatcharr.
"""

import re
import unicodedata
from difflib import SequenceMatcher

DEFAULT_STRIP_TOKENS = (
    "4k", "uhd", "fhd", "hd", "sd", "hevc", "h265", "h264", "avc", "raw",
    "fullhd", "ultrahd", "1080p", "1080i", "720p", "576p", "480p",
    "multi", "backup", "alt",
)

# one-ten in English, Italian, German and French. Accents are already
# folded by the time this is consulted, so 'fünf' arrives as 'funf'.
NUMBER_WORDS = {
    "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
    "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
    "uno": "1", "due": "2", "tre": "3", "quattro": "4", "cinque": "5",
    "sei": "6", "sette": "7", "otto": "8", "nove": "9", "dieci": "10",
    "eins": "1", "zwei": "2", "drei": "3", "vier": "4", "funf": "5",
    "sechs": "6", "sieben": "7", "acht": "8", "neun": "9", "zehn": "10",
    "deux": "2", "trois": "3", "quatre": "4", "cinq": "5",
    "sept": "7", "huit": "8", "neuf": "9", "dix": "10",
}

_COUNTRY_PREFIX = re.compile(r"^\s*[a-z]{2,4}\s*[:|]\s*")
_BRACKETED = re.compile(r"\[[^\]]*\]|\([^)]*\)|\{[^}]*\}")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_LETTER_DIGIT = re.compile(r"(?<=[a-z])(?=\d)|(?<=\d)(?=[a-z])")


def _strip_token_pattern(strip_tokens):
    # Longest first so 'uhd' wins over 'hd'. The (?<=\d) alternative is what
    # catches '1hd' in 'RAI1HD', where a plain \b does not apply.
    ordered = sorted({t.lower() for t in strip_tokens if t}, key=len, reverse=True)
    if not ordered:
        return None
    alternatives = "|".join(re.escape(t) for t in ordered)
    return re.compile(rf"(?:\b|(?<=\d))(?:{alternatives})\b")


_DEFAULT_TOKEN_PATTERN = _strip_token_pattern(DEFAULT_STRIP_TOKENS)


def normalize(name, strip_tokens=DEFAULT_STRIP_TOKENS, map_number_words=True):
    """Reduce a channel or stream name to a comparable token tuple."""
    if not name:
        return ()

    text = unicodedata.normalize("NFKD", str(name))
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower()

    text = _COUNTRY_PREFIX.sub("", text)
    text = _BRACKETED.sub(" ", text)

    pattern = (
        _DEFAULT_TOKEN_PATTERN
        if strip_tokens == DEFAULT_STRIP_TOKENS
        else _strip_token_pattern(strip_tokens)
    )
    if pattern is not None:
        text = pattern.sub(" ", text)

    text = _NON_ALNUM.sub(" ", text)
    text = _LETTER_DIGIT.sub(" ", text)

    tokens = text.split()
    if map_number_words:
        tokens = [NUMBER_WORDS.get(t, t) for t in tokens]
    return tuple(tokens)


def score(a, b):
    """0-100 similarity between two token tuples.

    Truncates rather than rounds: 'rai sport 1' against 'rai 1' is 0.625,
    which spec §16 pins at 62.
    """
    if not a or not b:
        return 0
    if tuple(a) == tuple(b):
        return 100
    if sorted(a) == sorted(b):
        return 98
    ratio = SequenceMatcher(None, " ".join(a), " ".join(b)).ratio()
    return int(ratio * 100)


def matches(channel_tokens, stream_tokens, mode="strict", threshold=85):
    """Decide whether this stream belongs on this channel."""
    if not channel_tokens or not stream_tokens:
        return False
    if mode == "strict":
        return set(channel_tokens) == set(stream_tokens)
    return score(channel_tokens, stream_tokens) >= threshold
