import pytest

from failoverr.naming import matches, normalize, score

# Spec §16, verbatim. Channel "RAI 1" normalizes to ('rai', '1').
FIXTURES = [
    ("RAI 1", ("rai", "1")),
    ("IT: Rai 1 4K", ("rai", "1")),
    ("IT: RAI 1 4K", ("rai", "1")),
    ("IT: RAI 1 HD ◉", ("rai", "1")),
    ("IT: RAI 1 HEVC", ("rai", "1")),
    ("IT: RAI 1 SD", ("rai", "1")),
    ("IT: RAI 1 UHD", ("rai", "1")),
    ("IT: Rai Uno", ("rai", "1")),
    ("IT: RAI1 FHD", ("rai", "1")),
    ("IT: RAI 2 HD", ("rai", "2")),
    ("IT: RAI News 24 HD", ("rai", "news", "24")),
    ("IT: RAI Sport 1 HD", ("rai", "sport", "1")),
]


@pytest.mark.parametrize("name,expected", FIXTURES)
def test_normalize_fixtures(name, expected):
    assert normalize(name) == expected


def test_strips_country_prefix_with_pipe():
    assert normalize("UK | Sky Sports") == ("sky", "sports")


def test_does_not_strip_a_leading_word_without_a_separator():
    """'RAI 1' must not lose 'RAI' — there is no colon or pipe."""
    assert normalize("RAI 1") == ("rai", "1")


def test_strips_bracketed_segments():
    assert normalize("IT: Rai 1 [backup] (alt)") == ("rai", "1")


def test_quality_token_glued_to_a_digit_is_stripped():
    """The lookbehind case: '1hd' in 'RAI1HD'."""
    assert normalize("IT: RAI1HD") == ("rai", "1")


def test_underscore_separators_are_normalized_before_stripping():
    r"""Underscore is \w so \b doesn't apply; _NON_ALNUM must run first.

    'RAI_1_HD' -> ('rai', '1') not ('rai', '1', 'hd')
    """
    assert normalize("IT: RAI_1_HD") == ("rai", "1")


def test_accents_are_folded():
    assert normalize("FR: Canal Plus Café") == ("canal", "plus", "cafe")


def test_number_word_mapping_can_be_disabled():
    assert normalize("IT: Rai Uno", map_number_words=False) == ("rai", "uno")


def test_custom_strip_tokens_replace_the_defaults():
    """Custom tokens are stripped; default tokens are not.

    Every other test passes the default tuple, so the _strip_token_pattern
    recompilation branch (the one a user's "Quality tokens to ignore" setting
    actually exercises) is never hit. This test exercises it and confirms
    custom tokens replace rather than extend the defaults.
    """
    assert normalize("IT: RAI 1 FOO HD", strip_tokens=("foo",)) == ("rai", "1", "hd")


def test_unknown_token_survives():
    """'sport' is not a quality token and must not be stripped."""
    assert "sport" in normalize("IT: RAI Sport 1 HD")


def test_empty_name_gives_empty_tuple():
    assert normalize("") == ()
    assert normalize("   ") == ()


CHANNEL = normalize("RAI 1")

SCORE_FIXTURES = [
    ("IT: Rai 1 4K", 100),
    ("IT: RAI 1 4K", 100),
    ("IT: RAI 1 HD ◉", 100),
    ("IT: RAI 1 HEVC", 100),
    ("IT: RAI 1 SD", 100),
    ("IT: RAI 1 UHD", 100),
    ("IT: Rai Uno", 100),
    ("IT: RAI1 FHD", 100),
    ("IT: RAI 2 HD", 80),
    ("IT: RAI News 24 HD", 50),
    ("IT: RAI Sport 1 HD", 62),
]


@pytest.mark.parametrize("stream_name,expected", SCORE_FIXTURES)
def test_score_fixtures(stream_name, expected):
    assert score(CHANNEL, normalize(stream_name)) == expected


def test_reordered_tokens_score_98():
    assert score(("rai", "1"), ("1", "rai")) == 98


def test_identical_scores_100():
    assert score(("rai", "1"), ("rai", "1")) == 100


@pytest.mark.parametrize("stream_name", [
    "IT: Rai 1 4K", "IT: RAI 1 HD ◉", "IT: Rai Uno", "IT: RAI1 FHD",
])
def test_strict_mode_accepts_true_matches(stream_name):
    assert matches(CHANNEL, normalize(stream_name), mode="strict")


@pytest.mark.parametrize("stream_name", [
    "IT: RAI 2 HD", "IT: RAI News 24 HD", "IT: RAI Sport 1 HD",
])
def test_strict_mode_excludes_near_misses(stream_name):
    """The whole point of strict mode: no false positives."""
    assert not matches(CHANNEL, normalize(stream_name), mode="strict")


def test_fuzzy_mode_at_default_threshold_still_excludes_rai_2():
    """RAI 2 scores 80; the default threshold of 85 must exclude it."""
    assert not matches(CHANNEL, normalize("IT: RAI 2 HD"), mode="fuzzy")


def test_fuzzy_mode_with_a_low_threshold_attaches_the_wrong_channel():
    """Documents the hazard the help text warns about."""
    assert matches(CHANNEL, normalize("IT: RAI 2 HD"), mode="fuzzy", threshold=80)


def test_empty_tokens_never_match():
    assert not matches((), (), mode="strict")
    assert not matches(CHANNEL, (), mode="strict")
