from datetime import date, datetime, timezone
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest

from nyc_taxi_data_service.serving.common.service_clock_loader import (
    _build_service_clock,
)

NY_TZ = ZoneInfo("America/New_York")


def test_build_service_clock_uses_new_session_when_anchor_absent(mocker) -> None:
    """
    Build a fresh ServiceClock when no shared startup anchor is present.
    """
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025-01-15",
        }
    }

    mock_clock = Mock()

    mock_from_new = mocker.patch(
        "nyc_taxi_data_service.serving.common.service_clock_loader.ServiceClock.from_new_session",
        return_value=mock_clock,
    )
    mock_from_existing = mocker.patch(
        "nyc_taxi_data_service.serving.common.service_clock_loader.ServiceClock.from_existing_anchor"
    )

    result = _build_service_clock(resolved_runtime_config)

    assert result is mock_clock
    mock_from_new.assert_called_once_with(mimic_current_date=date(2025, 1, 15))
    mock_from_existing.assert_not_called()


def test_build_service_clock_uses_existing_anchor_when_present(mocker) -> None:
    """
    Build ServiceClock from shared startup anchor when anchor is provided.
    """
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_clock_anchor": {
                "real_service_start_time_utc": "2026-03-31T07:15:19+00:00",
                "mimic_service_start_time": "2025-01-15T03:15:19-05:00",
            },
        }
    }

    mock_clock = Mock()

    mock_from_new = mocker.patch(
        "nyc_taxi_data_service.serving.common.service_clock_loader.ServiceClock.from_new_session"
    )
    mock_from_existing = mocker.patch(
        "nyc_taxi_data_service.serving.common.service_clock_loader.ServiceClock.from_existing_anchor",
        return_value=mock_clock,
    )

    result = _build_service_clock(resolved_runtime_config)

    assert result is mock_clock
    mock_from_new.assert_not_called()
    mock_from_existing.assert_called_once_with(
        mimic_current_date=date(2025, 1, 15),
        real_service_start_time=datetime(2026, 3, 31, 7, 15, 19, tzinfo=timezone.utc),
        mimic_service_start_time=datetime(
            2025,
            1,
            15,
            3,
            15,
            19,
            tzinfo=NY_TZ,
        ),
    )


def test_build_service_clock_raises_for_invalid_mimic_current_date() -> None:
    """
    Raise ValueError when mimic_current_date is invalid.
    """
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025/01/15",
        }
    }

    with pytest.raises(ValueError):
        _build_service_clock(resolved_runtime_config)


def test_build_service_clock_raises_for_invalid_anchor_datetime() -> None:
    """
    Raise ValueError when anchor datetime strings are invalid.
    """
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_clock_anchor": {
                "real_service_start_time_utc": "invalid",
                "mimic_service_start_time": "2025-01-15T03:15:19-05:00",
            },
        }
    }

    with pytest.raises(ValueError):
        _build_service_clock(resolved_runtime_config)


def test_build_service_clock_builds_real_service_clock_from_existing_anchor() -> None:
    """
    Build a real ServiceClock from startup-context anchor values.

    This verifies that the parsed mimic_service_start_time is normalized into
    the exact America/New_York timezone representation required by
    ServiceClock validation.
    """
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_clock_anchor": {
                "real_service_start_time_utc": "2026-03-31T12:22:54+00:00",
                "mimic_service_start_time": "2025-01-15T08:22:54-05:00",
            },
        }
    }

    service_clock = _build_service_clock(resolved_runtime_config)

    assert service_clock.mimic_current_date == date(2025, 1, 15)
    assert service_clock.real_service_start_time.tzinfo == timezone.utc
    assert service_clock.mimic_service_start_time.tzinfo == NY_TZ


def test_build_service_clock_normalizes_mimic_service_start_time_to_new_york_tz(
    mocker,
) -> None:
    """
    Normalize mimic_service_start_time from startup-context JSON into the
    America/New_York timezone before calling ServiceClock.from_existing_anchor().
    """
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_clock_anchor": {
                "real_service_start_time_utc": "2026-03-31T12:22:54+00:00",
                "mimic_service_start_time": "2025-01-15T08:22:54-05:00",
            },
        }
    }

    mocker.patch(
        "nyc_taxi_data_service.serving.common.service_clock_loader.ServiceClock.from_new_session"
    )
    mock_from_existing = mocker.patch(
        "nyc_taxi_data_service.serving.common.service_clock_loader.ServiceClock.from_existing_anchor",
        return_value=Mock(),
    )

    _build_service_clock(resolved_runtime_config)

    actual_mimic_service_start_time = mock_from_existing.call_args.kwargs[
        "mimic_service_start_time"
    ]
    actual_real_service_start_time = mock_from_existing.call_args.kwargs[
        "real_service_start_time"
    ]

    assert actual_real_service_start_time.tzinfo == timezone.utc
    assert actual_mimic_service_start_time.tzinfo == NY_TZ
