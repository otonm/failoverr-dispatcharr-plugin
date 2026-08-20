"""Tests for plugin.py's pure helpers. No Django import required."""

from failoverr.plugin import _status_message


def test_status_message_reports_idle_with_no_lock():
    message = _status_message({"holder": None, "progress": {}}, False, 55)
    assert message == "Idle. 55 streams tracked."


def test_status_message_reports_starting_up_before_the_first_channel():
    lock = {"holder": "run", "progress": {}}
    message = _status_message(lock, False, 0)
    assert message == "Running run: starting up."


def test_status_message_reports_channel_progress_and_new_finds():
    lock = {
        "holder": "run",
        "progress": {
            "channel_index": 44, "channels_total": 99, "channel_name": "RAI 1",
            "probed": 44, "new_found": 5, "attached": 2, "detached": 0,
        },
    }
    message = _status_message(lock, False, 0)
    assert message == (
        "Running run: channel 44 of 99 (RAI 1). 44 probed, 5 new streams found."
    )


def test_status_message_flags_a_pending_stop_request():
    lock = {"holder": "run", "progress": {}}
    message = _status_message(lock, True, 0)
    assert message.endswith("Stop requested - finishing current probe, then stopping.")
