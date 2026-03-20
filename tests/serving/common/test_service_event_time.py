import pytest

from nyc_taxi_data_service.serving.common.service_event_time import (
    get_service_event_time_column,
)


def test_get_service_event_time_column_returns_mapping() -> None:
    """Should return configured column for vehicle type."""
    serving_config = {
        "service_event_time_column_by_vehicle_type": {
            "yellow": "tpep_dropoff_datetime",
            "green": "lpep_dropoff_datetime",
        }
    }

    result = get_service_event_time_column("yellow", serving_config)

    assert result == "tpep_dropoff_datetime"


def test_get_service_event_time_column_raises_for_unknown_vehicle() -> None:
    """Should raise ValueError when vehicle type is not configured."""
    serving_config = {
        "service_event_time_column_by_vehicle_type": {
            "yellow": "tpep_dropoff_datetime",
        }
    }

    with pytest.raises(ValueError):
        get_service_event_time_column("test_veh", serving_config)


def test_get_service_event_time_column_raises_when_mapping_missing() -> None:
    """Should raise ValueError when mapping key is missing."""
    serving_config = {}

    with pytest.raises(KeyError):
        get_service_event_time_column("yellow", serving_config)
