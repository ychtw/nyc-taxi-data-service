from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from nyc_taxi_data_service.serving.api.app import (
    app,
    get_parquet_path,
    get_resolved_runtime_config,
    get_vehicle_event_time_column,
)


@pytest.fixture
def client():
    """
    Create a TestClient and clear dependency overrides after the test.
    """
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


@pytest.fixture
def api_test_data_dir(tmp_path: Path) -> Path:
    """
    Create a minimal parquet layout that matches the API's expected glob pattern.

    Layout:
        <tmp>/yellow/year=2025/month=01/*.parquet
    """
    data_dir = tmp_path / "yellow" / "year=2025" / "month=01"
    data_dir.mkdir(parents=True)

    rows = [
        {
            "vendor_id": 1,
            "tpep_dropoff_datetime": datetime(2025, 1, 15, 9, 0, 0),
            "fare_amount": 12.5,
        },
        {
            "vendor_id": 2,
            "tpep_dropoff_datetime": datetime(2025, 1, 15, 9, 0, 30),
            "fare_amount": 18.0,
        },
        {
            "vendor_id": 1,
            "tpep_dropoff_datetime": datetime(2025, 1, 15, 9, 1, 0),
            "fare_amount": 25.0,
        },
    ]

    df = pl.DataFrame(rows)
    df.write_parquet(data_dir / "yellow_tripdata_2025-01.parquet")

    return tmp_path


@pytest.fixture
def resolved_runtime_config_override(api_test_data_dir: Path) -> dict:
    """
    Return a minimal resolved runtime config for API endpoint tests.
    """
    return {
        "storage": {
            "target_base_path": str(api_test_data_dir),
        },
        "serving": {
            "service_event_time_column_by_vehicle_type": {
                "yellow": "tpep_dropoff_datetime",
                "green": "lpep_dropoff_datetime",
                "fhv": "dropOff_datetime",
                "fhvhv": "dropoff_datetime",
            }
        },
    }


def override_resolved_runtime_config(resolved_runtime_config: dict):
    """
    Build a FastAPI dependency override function for resolved runtime config.
    """

    def _override() -> dict:
        return resolved_runtime_config

    return _override


def test_get_trips_returns_rows_for_valid_request(
    client: TestClient,
    resolved_runtime_config_override: dict,
):
    """
    Return matching rows for a valid request and preserve response metadata.
    """
    app.dependency_overrides[get_resolved_runtime_config] = (
        override_resolved_runtime_config(resolved_runtime_config_override)
    )

    response = client.get(
        "/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:01:00",
            "limit": 10,
        },
    )

    assert response.status_code == 200

    payload = response.json()
    assert payload["vehicle_type"] == "yellow"
    assert payload["start_time"] == "2025-01-15T09:00:00"
    assert payload["end_time"] == "2025-01-15T09:01:00"
    assert payload["limit"] == 10
    assert len(payload["rows"]) == 2

    returned_times = [row["tpep_dropoff_datetime"] for row in payload["rows"]]
    assert returned_times == [
        "2025-01-15T09:00:00",
        "2025-01-15T09:00:30",
    ]


def test_get_trips_returns_400_for_unsupported_vehicle_type(
    client: TestClient,
    resolved_runtime_config_override: dict,
):
    """
    Reject requests for vehicle types that are not supported by config.
    """
    app.dependency_overrides[get_resolved_runtime_config] = (
        override_resolved_runtime_config(resolved_runtime_config_override)
    )

    response = client.get(
        "/trips/fakeveh",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:01:00",
            "limit": 10,
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "Unsupported vehicle_type: fakeveh",
    }


def test_get_trips_returns_400_when_start_time_is_not_earlier_than_end_time(
    client: TestClient,
    resolved_runtime_config_override: dict,
):
    """
    Reject requests whose time window is empty or reversed.
    """
    app.dependency_overrides[get_resolved_runtime_config] = (
        override_resolved_runtime_config(resolved_runtime_config_override)
    )

    response = client.get(
        "/trips/yellow",
        params={
            "start_time": "2025-01-15T09:01:00",
            "end_time": "2025-01-15T09:01:00",
            "limit": 10,
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "start_time must be earlier than end_time",
    }


def test_get_trips_returns_400_when_limit_is_not_positive(
    client: TestClient,
    resolved_runtime_config_override: dict,
):
    """
    Reject requests whose limit is zero or negative.
    """
    app.dependency_overrides[get_resolved_runtime_config] = (
        override_resolved_runtime_config(resolved_runtime_config_override)
    )

    response = client.get(
        "/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:01:00",
            "limit": 0,
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "limit must be positive",
    }


def test_get_trips_returns_422_for_invalid_datetime_input(
    client: TestClient,
    resolved_runtime_config_override: dict,
):
    """
    Let FastAPI reject malformed datetime query parameters before endpoint logic runs.
    """
    app.dependency_overrides[get_resolved_runtime_config] = (
        override_resolved_runtime_config(resolved_runtime_config_override)
    )

    response = client.get(
        "/trips/yellow",
        params={
            "start_time": "abc",
            "end_time": "2025-01-15T09:01:00",
            "limit": 10,
        },
    )

    assert response.status_code == 422


def test_get_trips_returns_empty_rows_when_no_records_match(
    client: TestClient,
    resolved_runtime_config_override: dict,
):
    """
    Return an empty rows list when the request is valid but no data matches.
    """
    app.dependency_overrides[get_resolved_runtime_config] = (
        override_resolved_runtime_config(resolved_runtime_config_override)
    )

    response = client.get(
        "/trips/yellow",
        params={
            "start_time": "2025-03-15T09:00:00",
            "end_time": "2025-03-15T09:01:00",
            "limit": 10,
        },
    )

    assert response.status_code == 200

    payload = response.json()
    assert payload["vehicle_type"] == "yellow"
    assert payload["rows"] == []


def test_get_trips_respects_limit(
    client: TestClient,
    resolved_runtime_config_override: dict,
):
    """
    Return no more than the requested number of rows.
    """
    app.dependency_overrides[get_resolved_runtime_config] = (
        override_resolved_runtime_config(resolved_runtime_config_override)
    )

    response = client.get(
        "/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:02:00",
            "limit": 1,
        },
    )

    assert response.status_code == 200

    payload = response.json()
    assert payload["limit"] == 1
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["tpep_dropoff_datetime"] == "2025-01-15T09:00:00"


def test_get_vehicle_event_time_column_returns_expected_column():
    """
    Return the configured event time column for a supported vehicle type.
    """
    resolved_runtime_config = {
        "serving": {
            "service_event_time_column_by_vehicle_type": {
                "yellow": "tpep_dropoff_datetime",
                "green": "lpep_dropoff_datetime",
            }
        }
    }

    result = get_vehicle_event_time_column(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
    )

    assert result == "tpep_dropoff_datetime"


def test_get_vehicle_event_time_column_raises_for_unsupported_vehicle_type():
    """
    Raise an HTTP 400 error when the vehicle type is not configured.
    """
    resolved_runtime_config = {
        "serving": {
            "service_event_time_column_by_vehicle_type": {
                "yellow": "tpep_dropoff_datetime",
            }
        }
    }

    with pytest.raises(HTTPException) as exc_info:
        get_vehicle_event_time_column(
            vehicle_type="fakeveh",
            resolved_runtime_config=resolved_runtime_config,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Unsupported vehicle_type: fakeveh"


def test_get_parquet_path_builds_expected_glob():
    """
    Build the parquet glob path from storage base path and vehicle type.
    """
    resolved_runtime_config = {
        "storage": {
            "target_base_path": "data/raw",
        }
    }

    result = get_parquet_path(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
    )

    assert result == "data/raw/yellow/year=*/month=*/*.parquet"
