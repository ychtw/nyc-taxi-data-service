from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import pytest
from fastapi import HTTPException

from nyc_taxi_data_service.serving.api.service import HistoricalApiService


@pytest.fixture
def api_test_data_dir(tmp_path: Path) -> Path:
    """
    Create a minimal parquet layout that matches the API's expected glob pattern.
    """
    data_dir = tmp_path / "yellow" / "year=2025" / "month=01"
    data_dir.mkdir(parents=True)

    rows = [
        {
            "vendor_id": 1,
            "tpep_dropoff_datetime": datetime(
                2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc
            ),
            "fare_amount": 12.5,
        },
        {
            "vendor_id": 2,
            "tpep_dropoff_datetime": datetime(
                2025, 1, 15, 9, 0, 30, tzinfo=timezone.utc
            ),
            "fare_amount": 18.0,
        },
        {
            "vendor_id": 1,
            "tpep_dropoff_datetime": datetime(
                2025, 1, 15, 9, 1, 0, tzinfo=timezone.utc
            ),
            "fare_amount": 25.0,
        },
    ]

    pl.DataFrame(rows).write_parquet(data_dir / "yellow_tripdata_2025-01.parquet")
    return tmp_path


@pytest.fixture
def resolved_runtime_config(api_test_data_dir: Path) -> dict:
    """
    Return a minimal resolved runtime config for HistoricalApiService tests.
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


@pytest.fixture
def api_service(resolved_runtime_config: dict) -> HistoricalApiService:
    """
    Build a HistoricalApiService for direct service tests.
    """
    return HistoricalApiService(resolved_runtime_config=resolved_runtime_config)


@pytest.fixture
def duckdb_connection():
    """
    Create a DuckDB connection for direct service tests.
    """
    con = duckdb.connect(database=":memory:")
    try:
        yield con
    finally:
        con.close()


def test_get_vehicle_event_time_column_returns_expected_column(
    api_service: HistoricalApiService,
):
    """
    Return the configured event time column for a supported vehicle type.
    """
    result = api_service.get_vehicle_event_time_column(vehicle_type="yellow")
    assert result == "tpep_dropoff_datetime"


def test_get_vehicle_event_time_column_raises_for_unsupported_vehicle_type(
    api_service: HistoricalApiService,
):
    """
    Raise an HTTP 400 error when the vehicle type is not configured.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service.get_vehicle_event_time_column(vehicle_type="fakeveh")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Unsupported vehicle_type: fakeveh"


def test_get_parquet_path_builds_expected_glob(
    api_service: HistoricalApiService,
    api_test_data_dir: Path,
):
    """
    Build the parquet glob path from storage base path and vehicle type.
    """
    result = api_service.get_parquet_path(vehicle_type="yellow")

    expected = str(api_test_data_dir / "yellow" / "year=*" / "month=*" / "*.parquet")
    assert result == expected


def test_normalize_request_datetime_interprets_naive_datetime_as_utc(
    api_service: HistoricalApiService,
):
    """
    Interpret a naive request datetime as a timezone-aware UTC datetime.
    """
    dt = datetime(2025, 1, 15, 9, 0, 0)

    result = api_service.normalize_request_datetime(dt, "start_time")

    assert result == datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc)


def test_normalize_request_datetime_converts_aware_datetime_to_utc(
    api_service: HistoricalApiService,
):
    """
    Convert a timezone-aware request datetime to UTC.
    """
    dt = datetime.fromisoformat("2025-01-15T17:00:00+08:00")

    result = api_service.normalize_request_datetime(dt, "start_time")

    assert result == datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc)


def test_get_available_through_returns_mimic_current_time(
    api_service: HistoricalApiService,
):
    """
    Return mimic_current_time unchanged for the initial API availability behavior.
    """
    mimic_current_time = datetime(2025, 1, 15, 9, 30, 0, tzinfo=timezone.utc)

    result = api_service.get_available_through(mimic_current_time=mimic_current_time)

    assert result == mimic_current_time


def test_validate_request_window_accepts_valid_inputs(
    api_service: HistoricalApiService,
):
    """
    Accept a request window when the time bounds, availability boundary, and limit are valid.
    """
    api_service.validate_request_window(
        start_time=datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        end_time=datetime(2025, 1, 15, 9, 1, 0, tzinfo=timezone.utc),
        available_through=datetime(2025, 1, 15, 9, 30, 0, tzinfo=timezone.utc),
        limit=10,
    )


def test_validate_request_window_raises_when_start_time_is_not_earlier_than_end_time(
    api_service: HistoricalApiService,
):
    """
    Raise an HTTP 400 error when the request window is empty or reversed.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service.validate_request_window(
            start_time=datetime(2025, 1, 15, 9, 1, 0, tzinfo=timezone.utc),
            end_time=datetime(2025, 1, 15, 9, 1, 0, tzinfo=timezone.utc),
            available_through=datetime(2025, 1, 15, 9, 30, 0, tzinfo=timezone.utc),
            limit=10,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "start_time must be earlier than end_time"


def test_validate_request_window_raises_when_limit_is_not_positive(
    api_service: HistoricalApiService,
):
    """
    Raise an HTTP 400 error when the requested limit is zero or negative.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service.validate_request_window(
            start_time=datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2025, 1, 15, 9, 1, 0, tzinfo=timezone.utc),
            available_through=datetime(2025, 1, 15, 9, 30, 0, tzinfo=timezone.utc),
            limit=0,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "limit must be positive"


def test_validate_request_window_raises_when_end_time_exceeds_available_range(
    api_service: HistoricalApiService,
):
    """
    Raise an HTTP 400 error when the requested end_time exceeds API availability.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service.validate_request_window(
            start_time=datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
            end_time=datetime(2025, 1, 15, 9, 31, 0, tzinfo=timezone.utc),
            available_through=datetime(2025, 1, 15, 9, 30, 0, tzinfo=timezone.utc),
            limit=10,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == (
        "end_time exceeds currently available API range: 2025-01-15T09:30:00+00:00"
    )


def test_get_trips_returns_expected_rows(
    api_service: HistoricalApiService,
    duckdb_connection: duckdb.DuckDBPyConnection,
):
    """
    Return matching trip rows and preserve request metadata for a valid query.
    """
    payload = api_service.get_trips(
        vehicle_type="yellow",
        start_time=datetime(2025, 1, 15, 9, 0, 0),
        end_time=datetime(2025, 1, 15, 9, 1, 0),
        limit=10,
        mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=timezone.utc),
        con=duckdb_connection,
    )

    assert payload["vehicle_type"] == "yellow"
    assert payload["limit"] == 10
    assert payload["start_time"] == datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
    assert payload["end_time"] == datetime(2025, 1, 15, 9, 1, 0, tzinfo=timezone.utc)
    assert payload["mimic_current_time"] == datetime(
        2025, 1, 15, 9, 30, 0, tzinfo=timezone.utc
    )
    assert len(payload["rows"]) == 2
    assert [row["tpep_dropoff_datetime"] for row in payload["rows"]] == [
        datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        datetime(2025, 1, 15, 9, 0, 30, tzinfo=timezone.utc),
    ]


def test_get_trips_returns_empty_rows_when_no_records_match(
    api_service: HistoricalApiService,
    duckdb_connection: duckdb.DuckDBPyConnection,
):
    """
    Return an empty rows list when the query is valid but no records match.
    """
    payload = api_service.get_trips(
        vehicle_type="yellow",
        start_time=datetime(2025, 1, 15, 8, 0, 0),
        end_time=datetime(2025, 1, 15, 8, 1, 0),
        limit=10,
        mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=timezone.utc),
        con=duckdb_connection,
    )

    assert payload["rows"] == []


def test_get_trips_respects_limit(
    api_service: HistoricalApiService,
    duckdb_connection: duckdb.DuckDBPyConnection,
):
    """
    Return no more than the requested number of rows.
    """
    payload = api_service.get_trips(
        vehicle_type="yellow",
        start_time=datetime(2025, 1, 15, 9, 0, 0),
        end_time=datetime(2025, 1, 15, 9, 2, 0),
        limit=1,
        mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=timezone.utc),
        con=duckdb_connection,
    )

    assert payload["limit"] == 1
    assert len(payload["rows"]) == 1


def test_get_trips_raises_when_end_time_exceeds_available_range(
    api_service: HistoricalApiService,
    duckdb_connection: duckdb.DuckDBPyConnection,
):
    """
    Raise an HTTP 400 error when the requested query exceeds API availability.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service.get_trips(
            vehicle_type="yellow",
            start_time=datetime(2025, 1, 15, 9, 0, 0),
            end_time=datetime(2025, 1, 15, 9, 31, 0),
            limit=10,
            mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=timezone.utc),
            con=duckdb_connection,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == (
        "end_time exceeds currently available API range: 2025-01-15T09:30:00+00:00"
    )
