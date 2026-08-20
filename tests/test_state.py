from failoverr.state import INCONCLUSIVE, INVALID, VALID, State, url_hash

URL = "http://provider.example/live/1.ts"
HOUR = 3600.0


def fresh_state(tmp_path):
    return State(tmp_path / "state.json")


def test_unknown_stream_is_not_fresh(tmp_path):
    assert not fresh_state(tmp_path).is_fresh(1, URL, 24, now=0.0)


def test_valid_result_is_fresh_within_ttl(tmp_path):
    state = fresh_state(tmp_path)
    state.record(1, URL, VALID, now=0.0)
    assert state.is_fresh(1, URL, 24, now=23 * HOUR)


def test_valid_result_is_stale_after_ttl(tmp_path):
    state = fresh_state(tmp_path)
    state.record(1, URL, VALID, now=0.0)
    assert not state.is_fresh(1, URL, 24, now=25 * HOUR)


def test_changed_url_invalidates_the_cache(tmp_path):
    """Providers rotate what sits behind a URL; the old verdict is meaningless."""
    state = fresh_state(tmp_path)
    state.record(1, URL, VALID, now=0.0)
    changed_url = "http://provider.example/live/CHANGED.ts"
    assert not state.is_fresh(1, changed_url, 24, now=HOUR)


def test_inconclusive_is_never_fresh(tmp_path):
    """Inconclusive means 'ask again', so it must always be re-probed."""
    state = fresh_state(tmp_path)
    state.record(1, URL, INCONCLUSIVE, now=0.0)
    assert not state.is_fresh(1, URL, 24, now=1.0)


def test_invalid_result_is_fresh_within_ttl(tmp_path):
    state = fresh_state(tmp_path)
    state.record(1, URL, INVALID, now=0.0)
    assert state.is_fresh(1, URL, 24, now=HOUR)


def test_invalid_increments_the_failure_counter(tmp_path):
    state = fresh_state(tmp_path)
    for _ in range(3):
        state.record(1, URL, INVALID, now=0.0)
    assert state.failure_count(1) == 3


def test_inconclusive_does_not_count_toward_removal(tmp_path):
    """A timeout or a rate limit must never contribute to deleting a stream."""
    state = fresh_state(tmp_path)
    state.record(1, URL, INVALID, now=0.0)
    for _ in range(10):
        state.record(1, URL, INCONCLUSIVE, now=0.0)
    assert state.failure_count(1) == 1


def test_valid_resets_the_failure_counter(tmp_path):
    state = fresh_state(tmp_path)
    state.record(1, URL, INVALID, now=0.0)
    state.record(1, URL, INVALID, now=0.0)
    state.record(1, URL, VALID, now=0.0)
    assert state.failure_count(1) == 0


def test_removal_requires_the_full_threshold(tmp_path):
    state = fresh_state(tmp_path)
    state.record(1, URL, INVALID, now=0.0)
    assert not state.should_remove(1, threshold=3)
    state.record(1, URL, INVALID, now=0.0)
    assert not state.should_remove(1, threshold=3)
    state.record(1, URL, INVALID, now=0.0)
    assert state.should_remove(1, threshold=3)


def test_save_and_load_round_trip(tmp_path):
    path = tmp_path / "nested" / "state.json"
    state = State(path)
    state.record(7, URL, INVALID, now=123.0)
    state.save()

    reloaded = State.load(path)
    assert reloaded.failure_count(7) == 1
    assert reloaded.last_verdict(7) == INVALID


def test_save_leaves_no_temp_file_behind(tmp_path):
    path = tmp_path / "state.json"
    state = State(path)
    state.record(1, URL, VALID, now=0.0)
    state.save()
    assert [p.name for p in tmp_path.iterdir()] == ["state.json"]


def test_load_of_a_missing_file_gives_empty_state(tmp_path):
    assert State.load(tmp_path / "absent.json").failure_count(1) == 0


def test_load_of_a_corrupt_file_gives_empty_state_without_raising(tmp_path):
    """A truncated write must not brick the plugin."""
    path = tmp_path / "state.json"
    path.write_text('{"streams": {"1": {"fail')
    assert State.load(path).failure_count(1) == 0


def test_url_hash_is_stable_and_distinguishing():
    assert url_hash(URL) == url_hash(URL)
    assert url_hash(URL) != url_hash(URL + "?token=2")


def test_record_stores_response_time_on_a_valid_verdict(tmp_path):
    state = State(tmp_path / "state.json")
    state.record(1, "http://a.example/1.ts", VALID, response_time_ms=350)
    assert state.response_time_ms(1) == 350


def test_response_time_defaults_to_none_when_never_recorded(tmp_path):
    state = State(tmp_path / "state.json")
    assert state.response_time_ms(1) is None


def test_inconclusive_preserves_prior_response_time(tmp_path):
    state = State(tmp_path / "state.json")
    state.record(1, "http://a.example/1.ts", VALID, response_time_ms=350)
    state.record(1, "http://a.example/1.ts", INCONCLUSIVE)
    assert state.response_time_ms(1) == 350


def test_a_fresh_valid_probe_updates_the_response_time(tmp_path):
    state = State(tmp_path / "state.json")
    state.record(1, "http://a.example/1.ts", VALID, response_time_ms=350)
    state.record(1, "http://a.example/1.ts", VALID, response_time_ms=120)
    assert state.response_time_ms(1) == 120
