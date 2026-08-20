"""Tests for plugin.py's pure helpers. No Django import required."""

from failoverr.plugin import _status_message


def test_status_message_reports_idle_with_no_lock():
    message = _status_message({"holder": None, "progress": {}}, False, 55)
    assert message == "Idle. 55 streams tracked."


def test_status_message_reports_starting_up_before_the_first_channel():
    lock = {"holder": "run", "progress": {}}
    message = _status_message(lock, False, 0)
    assert message == "Running run: starting up."


def test_status_message_reports_per_stream_progress_while_probing():
    lock = {
        "holder": "run",
        "progress": {
            "stream_index": 44, "streams_total": 99, "channel_name": "RAI 1",
            "channel_index": 3, "channels_total": 6,
            "new_found": 5, "attached": 2, "detached": 0,
        },
    }
    message = _status_message(lock, False, 0)
    assert message == (
        "Running run. Processing stream 44 of 99 (RAI 1). Found 5 new streams."
    )


def test_status_message_falls_back_to_channel_progress_when_nothing_is_stale():
    """Both cases never have a stream total, so they fall back the same way.

    reorder_only never probes anything, and a run/probe_only with an
    all-cached lineup has nothing stale left to probe either.
    """
    lock = {
        "holder": "reorder_only",
        "progress": {
            "channel_index": 3, "channels_total": 6, "channel_name": "RAI Movie",
            "new_found": 0, "attached": 0, "detached": 0,
        },
    }
    message = _status_message(lock, False, 0)
    assert message == "Running reorder_only: channel 3 of 6 (RAI Movie)."


def test_status_message_flags_a_pending_stop_request():
    lock = {"holder": "run", "progress": {}}
    message = _status_message(lock, True, 0)
    assert message.endswith("Stop requested - finishing current probe, then stopping.")
