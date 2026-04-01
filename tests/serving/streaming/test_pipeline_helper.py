from datetime import date, datetime, timezone
from unittest.mock import Mock, call
from zoneinfo import ZoneInfo

import pytest

from nyc_taxi_data_service.serving.streaming.pipeline import (
    _build_service_clock,
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


def test_build_service_clock_uses_new_session_when_anchor_is_absent(mocker) -> None:
    """Build a fresh ServiceClock when no shared startup anchor is provided."""
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025-01-15",
        }
    }
    mock_service_clock = Mock()

    mock_from_new_session = mocker.patch(
        "nyc_taxi_data_service.serving.common.service_clock_loader.ServiceClock.from_new_session",
        return_value=mock_service_clock,
    )
    mock_from_existing_anchor = mocker.patch(
        "nyc_taxi_data_service.serving.common.service_clock_loader.ServiceClock.from_existing_anchor"
    )

    result = _build_service_clock(resolved_runtime_config)

    assert result is mock_service_clock
    mock_from_new_session.assert_called_once_with(mimic_current_date=date(2025, 1, 15))
    mock_from_existing_anchor.assert_not_called()


def test_build_service_clock_uses_existing_anchor_when_present(mocker) -> None:
    """Build ServiceClock from shared startup anchor when anchor values exist."""
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_clock_anchor": {
                "real_service_start_time_utc": "2026-03-31T07:15:19+00:00",
                "mimic_service_start_time": "2025-01-15T03:15:19-05:00",
            },
        }
    }
    mock_service_clock = Mock()

    mock_from_new_session = mocker.patch(
        "nyc_taxi_data_service.serving.common.service_clock_loader.ServiceClock.from_new_session"
    )
    mock_from_existing_anchor = mocker.patch(
        "nyc_taxi_data_service.serving.common.service_clock_loader.ServiceClock.from_existing_anchor",
        return_value=mock_service_clock,
    )

    result = _build_service_clock(resolved_runtime_config)

    assert result is mock_service_clock
    mock_from_new_session.assert_not_called()
    mock_from_existing_anchor.assert_called_once_with(
        mimic_current_date=date(2025, 1, 15),
        real_service_start_time=datetime(2026, 3, 31, 7, 15, 19, tzinfo=timezone.utc),
        mimic_service_start_time=datetime(2025, 1, 15, 3, 15, 19, tzinfo=NY_TZ),
    )


def test_build_service_clock_raises_for_invalid_mimic_current_date() -> None:
    """Raise ValueError when serving.mimic_current_date is not a valid ISO date."""
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025/01/15",
        }
    }

    with pytest.raises(ValueError):
        _build_service_clock(resolved_runtime_config)


def test_build_service_clock_raises_for_invalid_anchor_datetime_strings() -> None:
    """Raise ValueError when startup anchor timestamps are not valid ISO datetimes."""
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_clock_anchor": {
                "real_service_start_time_utc": "2026/03/31 07:15:19",
                "mimic_service_start_time": "2025-01-15T03:15:19-05:00",
            },
        }
    }

    with pytest.raises(ValueError):
        _build_service_clock(resolved_runtime_config)
