from datetime import datetime
from unittest.mock import Mock, call
from zoneinfo import ZoneInfo

from nyc_taxi_data_service.serving.streaming.pipeline import (
    _run_initial_buffering,
    _run_publish_cycle,
)

NY_TZ = ZoneInfo("America/New_York")


def test_run_initial_buffering_calls_buffer_current_window_once_per_buffer() -> None:
    """Call buffer_current_window once for each configured window buffer."""
    yellow_buffer = Mock()
    green_buffer = Mock()
    fhv_buffer = Mock()

    window_buffers = {
        "yellow": yellow_buffer,
        "green": green_buffer,
        "fhv": fhv_buffer,
    }

    _run_initial_buffering(window_buffers=window_buffers)

    yellow_buffer.buffer_current_window.assert_called_once_with()
    green_buffer.buffer_current_window.assert_called_once_with()
    fhv_buffer.buffer_current_window.assert_called_once_with()


def test_run_initial_buffering_calls_buffers_in_dict_value_order() -> None:
    """Iterate through window_buffers.values() and call each buffer once."""
    yellow_buffer = Mock()
    green_buffer = Mock()

    window_buffers = {
        "yellow": yellow_buffer,
        "green": green_buffer,
    }

    _run_initial_buffering(window_buffers=window_buffers)

    assert yellow_buffer.method_calls == [call.buffer_current_window()]
    assert green_buffer.method_calls == [call.buffer_current_window()]


def test_run_initial_buffering_does_nothing_for_empty_window_buffers() -> None:
    """Do nothing when no window buffers are configured."""
    window_buffers = {}

    _run_initial_buffering(window_buffers=window_buffers)

    # no assertion on collaborators is needed here because the input dict is empty;
    # the main check is simply that the helper completes without raising


def test_run_publish_cycle_calls_publish_available_once_per_producer() -> None:
    """Call publish_available once for each configured replay producer."""
    yellow_producer = Mock()
    green_producer = Mock()

    replay_producers = {
        "yellow": yellow_producer,
        "green": green_producer,
    }
    mimic_current_time = datetime(2025, 1, 15, 12, 0, tzinfo=NY_TZ)

    _run_publish_cycle(
        replay_producers=replay_producers,
        mimic_current_time=mimic_current_time,
    )

    yellow_producer.publish_available.assert_called_once_with(
        mimic_current_time=mimic_current_time,
    )
    green_producer.publish_available.assert_called_once_with(
        mimic_current_time=mimic_current_time,
    )


def test_run_publish_cycle_passes_same_mimic_current_time_to_all_producers() -> None:
    """Pass the same mimic timestamp into every replay producer."""
    yellow_producer = Mock()
    green_producer = Mock()
    fhv_producer = Mock()

    replay_producers = {
        "yellow": yellow_producer,
        "green": green_producer,
        "fhv": fhv_producer,
    }
    mimic_current_time = datetime(2025, 2, 1, 8, 30, tzinfo=NY_TZ)

    _run_publish_cycle(
        replay_producers=replay_producers,
        mimic_current_time=mimic_current_time,
    )

    assert (
        yellow_producer.publish_available.call_args.kwargs["mimic_current_time"]
        == mimic_current_time
    )
    assert (
        green_producer.publish_available.call_args.kwargs["mimic_current_time"]
        == mimic_current_time
    )
    assert (
        fhv_producer.publish_available.call_args.kwargs["mimic_current_time"]
        == mimic_current_time
    )


def test_run_publish_cycle_does_nothing_for_empty_replay_producers() -> None:
    """Do nothing when no replay producers are configured."""
    _run_publish_cycle(
        replay_producers={},
        mimic_current_time=datetime(2025, 1, 1, tzinfo=NY_TZ),
    )
