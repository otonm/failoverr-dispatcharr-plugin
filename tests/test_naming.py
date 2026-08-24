import pytest

from failoverr.naming import normalize

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
