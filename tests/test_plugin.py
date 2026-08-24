"""Tests for plugin.py's pure helpers. No Django import required."""

from failoverr.plugin import _status_message


def test_status_message_reports_idle_with_no_lock():
    message = _status_message({"holder": None, "progress": {}}, False, 55)
    assert message == "Idle. 55 streams tracked."


def test_status_message_reports_starting_up_before_the_first_channel():
    lock = {"holder": "run", "progress": {}}
    message = _status_message(lock, False, 0)
    assert message == "Running run: starting up."


def test_status_message_reports_channel_progress_while_running():
    lock = {
        "holder": "run",
        "progress": {
            "channel_name": "RAI 1", "channel_index": 3, "channels_total": 6,
            "new_found": 5, "attached": 2, "detached": 0,
        },
    }
    message = _status_message(lock, False, 0)
    assert message == (
        "Running run: channel 3 of 6 (RAI 1). Found 5 new streams."
    )


def test_status_message_flags_a_pending_stop_request():
    lock = {"holder": "run", "progress": {}}
    message = _status_message(lock, True, 0)
    assert message.endswith("Stop requested - finishing current probe, then stopping.")


def test_status_message_flags_stop_while_running():
    lock = {
        "holder": "run",
        "progress": {
            "channel_name": "RAI 1", "channel_index": 1, "channels_total": 3,
            "new_found": 2, "attached": 1, "detached": 0,
        },
    }
    message = _status_message(lock, True, 0)
    assert "channel 1 of 3 (RAI 1)" in message
    assert message.endswith("Stop requested - finishing current probe, then stopping.")


def test_status_message_fallback_when_channel_index_missing():
    """Missing channel_index shows as None in the message."""
    lock = {
        "holder": "run",
        "progress": {
            "channels_total": 6, "channel_name": "RAI Movie",
            "new_found": 0, "attached": 0, "detached": 0,
        },
    }
    message = _status_message(lock, False, 0)
    assert "channel None of 6" in message  # channel_index defaults to None
    assert "RAI Movie" in message
