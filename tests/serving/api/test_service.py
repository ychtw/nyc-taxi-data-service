from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
import pytest
from fastapi import HTTPException

from nyc_taxi_data_service.serving.api.service import HistoricalApiService

NY_TZ = ZoneInfo("America/New_York")


@pytest.fixture
def api_test_data_dir(tmp_path: Path) -> Path:
    """
    Create a minimal parquet layout that matches the API's expected glob pattern.

    The TLC-style event-time column is written as naive New York-local timestamps
    so it matches the service's DuckDB query semantics.
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
) -> None:
    """
    Return the configured event time column for a supported vehicle type.
    """
    result = api_service._get_vehicle_event_time_column(vehicle_type="yellow")
    assert result == "tpep_dropoff_datetime"


def test_get_vehicle_event_time_column_raises_for_unsupported_vehicle_type(
    api_service: HistoricalApiService,
) -> None:
    """
    Raise an HTTP 400 error when the vehicle type is not configured.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service._get_vehicle_event_time_column(vehicle_type="fakeveh")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Unsupported vehicle_type: fakeveh"


def test_get_parquet_path_builds_expected_glob(
    api_service: HistoricalApiService,
    api_test_data_dir: Path,
) -> None:
    """
    Build the parquet glob path from storage base path and vehicle type.
    """
    result = api_service._get_parquet_path(vehicle_type="yellow")

    expected = str(api_test_data_dir / "yellow" / "year=*" / "month=*" / "*.parquet")
    assert result == expected


def test_normalize_request_datetime_interprets_naive_datetime_as_new_york_time(
    api_service: HistoricalApiService,
) -> None:
    """
    Interpret a naive request datetime as a timezone-aware New York datetime.
    """
    dt = datetime(2025, 1, 15, 9, 0, 0)

    result = api_service._normalize_request_datetime(dt, "start_time")

    assert result == datetime(2025, 1, 15, 9, 0, 0, tzinfo=NY_TZ)


def test_normalize_request_datetime_converts_aware_datetime_to_new_york_time(
    api_service: HistoricalApiService,
) -> None:
    """
    Convert a timezone-aware request datetime into New York time.
    """
    dt = datetime.fromisoformat("2025-01-15T17:00:00+08:00")

    result = api_service._normalize_request_datetime(dt, "start_time")

    assert result == datetime(2025, 1, 15, 4, 0, 0, tzinfo=NY_TZ)


def test_to_duckdb_timestamp_value_returns_naive_new_york_local_datetime(
    api_service: HistoricalApiService,
) -> None:
    """
    Convert a service-layer New York datetime into a naive DuckDB timestamp value.
    """
    dt = datetime(2025, 1, 15, 9, 0, 0, tzinfo=NY_TZ)

    result = api_service._to_duckdb_timestamp_value(dt)

    assert result == datetime(2025, 1, 15, 9, 0, 0)
    assert result.tzinfo is None


def test_get_available_through_returns_mimic_current_time(
    api_service: HistoricalApiService,
) -> None:
    """
    Return mimic_current_time unchanged for the initial API availability behavior.
    """
    mimic_current_time = datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ)

    result = api_service._get_available_through(mimic_current_time=mimic_current_time)

    assert result == mimic_current_time


def test_validate_request_window_accepts_valid_inputs(
    api_service: HistoricalApiService,
) -> None:
    """
    Accept a request window when the time bounds, availability boundary, and limit are valid.
    """
    api_service._validate_request_window(
        start_time=datetime(2025, 1, 15, 9, 0, 0, tzinfo=NY_TZ),
        end_time=datetime(2025, 1, 15, 9, 1, 0, tzinfo=NY_TZ),
        available_through=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
        limit=10,
        vehicle_type="yellow",
    )


def test_validate_request_window_raises_when_start_time_is_not_earlier_than_end_time(
    api_service: HistoricalApiService,
) -> None:
    """
    Raise an HTTP 400 error when the request window is empty or reversed.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service._validate_request_window(
            start_time=datetime(2025, 1, 15, 9, 1, 0, tzinfo=NY_TZ),
            end_time=datetime(2025, 1, 15, 9, 1, 0, tzinfo=NY_TZ),
            available_through=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
            limit=10,
            vehicle_type="yellow",
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "start_time must be earlier than end_time"


def test_validate_request_window_raises_when_limit_is_not_positive(
    api_service: HistoricalApiService,
) -> None:
    """
    Raise an HTTP 400 error when the requested limit is zero or negative.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service._validate_request_window(
            start_time=datetime(2025, 1, 15, 9, 0, 0, tzinfo=NY_TZ),
            end_time=datetime(2025, 1, 15, 9, 1, 0, tzinfo=NY_TZ),
            available_through=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
            limit=0,
            vehicle_type="yellow",
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "limit must be positive"


def test_validate_request_window_raises_when_limit_exceeds_maximum(
    api_service: HistoricalApiService,
) -> None:
    """
    Raise an HTTP 400 error when the requested limit exceeds the maximum allowed page size.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service._validate_request_window(
            start_time=datetime(2025, 1, 15, 9, 0, 0, tzinfo=NY_TZ),
            end_time=datetime(2025, 1, 15, 9, 1, 0, tzinfo=NY_TZ),
            available_through=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
            limit=1001,
            vehicle_type="yellow",
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "limit must be no more than 1000"


def test_validate_request_window_raises_when_end_time_exceeds_available_range(
    api_service: HistoricalApiService,
) -> None:
    """
    Raise an HTTP 400 error when the requested end_time exceeds API availability.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service._validate_request_window(
            start_time=datetime(2025, 1, 15, 9, 0, 0, tzinfo=NY_TZ),
            end_time=datetime(2025, 1, 15, 9, 31, 0, tzinfo=NY_TZ),
            available_through=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
            limit=10,
            vehicle_type="yellow",
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == (
        "end_time exceeds currently available API range: 2025-01-15T09:30:00-05:00"
    )


def test_encode_cursor_and_decode_cursor_round_trip(
    api_service: HistoricalApiService,
) -> None:
    """
    Encode cursor fields into an opaque string and decode them back into the original values.
    """
    cursor = api_service._encode_cursor(
        event_time=datetime(2025, 1, 15, 9, 0, 30),
        filename="/tmp/yellow/year=2025/month=01/file.parquet",
        file_row_number=42,
    )

    decoded = api_service._decode_cursor(cursor)

    assert decoded == {
        "event_time": datetime(2025, 1, 15, 9, 0, 30),
        "filename": "/tmp/yellow/year=2025/month=01/file.parquet",
        "file_row_number": 42,
    }


def test_decode_cursor_raises_for_invalid_cursor(
    api_service: HistoricalApiService,
) -> None:
    """
    Raise an HTTP 400 error when the cursor is malformed.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service._decode_cursor("not-a-valid-cursor")

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Invalid cursor"


def test_build_paginated_query_returns_first_page_query_when_cursor_is_none(
    api_service: HistoricalApiService,
) -> None:
    """
    Build the first-page query and parameters when no cursor is provided.
    """
    query, query_params = api_service._build_paginated_query(
        vehicle_type="yellow",
        event_time_column="tpep_dropoff_datetime",
        parquet_path="/tmp/data/yellow/year=*/month=*/*.parquet",
        query_start_time=datetime(2025, 1, 15, 9, 0, 0),
        query_end_time=datetime(2025, 1, 15, 9, 2, 0),
        limit=2,
        cursor=None,
    )

    assert "FROM read_parquet(?, filename=True, file_row_number=True)" in query
    assert "WHERE tpep_dropoff_datetime >= ?" in query
    assert "AND tpep_dropoff_datetime < ?" in query
    assert (
        "ORDER BY tpep_dropoff_datetime ASC, filename ASC, file_row_number ASC" in query
    )
    assert query_params == [
        "/tmp/data/yellow/year=*/month=*/*.parquet",
        datetime(2025, 1, 15, 9, 0, 0),
        datetime(2025, 1, 15, 9, 2, 0),
        3,  # limit + 1
    ]


def test_build_paginated_query_returns_continuation_query_when_cursor_is_provided(
    api_service: HistoricalApiService,
) -> None:
    """
    Build the continuation query and parameters when a cursor is provided.
    """
    cursor = api_service._encode_cursor(
        event_time=datetime(2025, 1, 15, 9, 0, 30),
        filename="/tmp/data/yellow/year=2025/month=01/file.parquet",
        file_row_number=7,
    )

    query, query_params = api_service._build_paginated_query(
        vehicle_type="yellow",
        event_time_column="tpep_dropoff_datetime",
        parquet_path="/tmp/data/yellow/year=*/month=*/*.parquet",
        query_start_time=datetime(2025, 1, 15, 9, 0, 0),
        query_end_time=datetime(2025, 1, 15, 9, 2, 0),
        limit=2,
        cursor=cursor,
    )

    assert "AND ( tpep_dropoff_datetime > ?" in query
    assert "filename > ?" in query
    assert "file_row_number > ?" in query
    assert query_params == [
        "/tmp/data/yellow/year=*/month=*/*.parquet",
        datetime(2025, 1, 15, 9, 0, 0),
        datetime(2025, 1, 15, 9, 2, 0),
        datetime(2025, 1, 15, 9, 0, 30),
        datetime(2025, 1, 15, 9, 0, 30),
        "/tmp/data/yellow/year=2025/month=01/file.parquet",
        "/tmp/data/yellow/year=2025/month=01/file.parquet",
        7,
        3,  # limit + 1
    ]


def test_get_trips_returns_expected_rows_and_pagination_metadata(
    api_service: HistoricalApiService,
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """
    Return matching trip rows and pagination metadata for a valid first-page query.
    """
    payload = api_service.get_trips(
        vehicle_type="yellow",
        start_time=datetime(2025, 1, 15, 9, 0, 0),
        end_time=datetime(2025, 1, 15, 9, 1, 0),
        limit=10,
        cursor=None,
        mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
        con=duckdb_connection,
    )

    assert payload["vehicle_type"] == "yellow"
    assert payload["limit"] == 10
    assert payload["start_time"] == datetime(2025, 1, 15, 9, 0, 0, tzinfo=NY_TZ)
    assert payload["end_time"] == datetime(2025, 1, 15, 9, 1, 0, tzinfo=NY_TZ)
    assert payload["mimic_current_time"] == datetime(
        2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ
    )
    assert payload["available_through"] == datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ)
    assert payload["returned_count"] == 2
    assert payload["has_more"] is False
    assert payload["next_cursor"] is None
    assert len(payload["rows"]) == 2
    assert [row["tpep_dropoff_datetime"] for row in payload["rows"]] == [
        datetime(2025, 1, 15, 9, 0, 0),
        datetime(2025, 1, 15, 9, 0, 30),
    ]
    assert "filename" not in payload["rows"][0]
    assert "file_row_number" not in payload["rows"][0]


def test_get_trips_returns_empty_rows_when_no_records_match(
    api_service: HistoricalApiService,
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """
    Return an empty rows list when the query is valid but no records match.
    """
    payload = api_service.get_trips(
        vehicle_type="yellow",
        start_time=datetime(2025, 1, 15, 8, 0, 0),
        end_time=datetime(2025, 1, 15, 8, 1, 0),
        limit=10,
        cursor=None,
        mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
        con=duckdb_connection,
    )

    assert payload["returned_count"] == 0
    assert payload["has_more"] is False
    assert payload["next_cursor"] is None
    assert payload["rows"] == []


def test_get_trips_returns_next_cursor_when_more_rows_exist(
    api_service: HistoricalApiService,
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """
    Return a next cursor and set has_more when additional rows remain after the current page.
    """
    payload = api_service.get_trips(
        vehicle_type="yellow",
        start_time=datetime(2025, 1, 15, 9, 0, 0),
        end_time=datetime(2025, 1, 15, 9, 2, 0),
        limit=2,
        cursor=None,
        mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
        con=duckdb_connection,
    )

    assert payload["returned_count"] == 2
    assert payload["has_more"] is True
    assert payload["next_cursor"] is not None
    assert len(payload["rows"]) == 2
    assert [row["tpep_dropoff_datetime"] for row in payload["rows"]] == [
        datetime(2025, 1, 15, 9, 0, 0),
        datetime(2025, 1, 15, 9, 0, 30),
    ]


def test_get_trips_continuation_query_returns_next_page_rows(
    api_service: HistoricalApiService,
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """
    Return the next page of rows when a valid cursor from the previous page is provided.
    """
    first_page = api_service.get_trips(
        vehicle_type="yellow",
        start_time=datetime(2025, 1, 15, 9, 0, 0),
        end_time=datetime(2025, 1, 15, 9, 2, 0),
        limit=2,
        cursor=None,
        mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
        con=duckdb_connection,
    )

    second_page = api_service.get_trips(
        vehicle_type="yellow",
        start_time=datetime(2025, 1, 15, 9, 0, 0),
        end_time=datetime(2025, 1, 15, 9, 2, 0),
        limit=2,
        cursor=first_page["next_cursor"],
        mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
        con=duckdb_connection,
    )

    assert first_page["has_more"] is True
    assert first_page["next_cursor"] is not None

    assert second_page["returned_count"] == 1
    assert second_page["has_more"] is False
    assert second_page["next_cursor"] is None
    assert [row["tpep_dropoff_datetime"] for row in second_page["rows"]] == [
        datetime(2025, 1, 15, 9, 1, 0),
    ]


def test_get_trips_respects_limit(
    api_service: HistoricalApiService,
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """
    Return no more than the requested number of rows.
    """
    payload = api_service.get_trips(
        vehicle_type="yellow",
        start_time=datetime(2025, 1, 15, 9, 0, 0),
        end_time=datetime(2025, 1, 15, 9, 2, 0),
        limit=1,
        cursor=None,
        mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
        con=duckdb_connection,
    )

    assert payload["limit"] == 1
    assert payload["returned_count"] == 1
    assert len(payload["rows"]) == 1


def test_get_trips_raises_when_cursor_is_invalid(
    api_service: HistoricalApiService,
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """
    Raise an HTTP 400 error when the client provides a malformed pagination cursor.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service.get_trips(
            vehicle_type="yellow",
            start_time=datetime(2025, 1, 15, 9, 0, 0),
            end_time=datetime(2025, 1, 15, 9, 2, 0),
            limit=2,
            cursor="not-a-valid-cursor",
            mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
            con=duckdb_connection,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "Invalid cursor"


def test_get_trips_raises_when_end_time_exceeds_available_range(
    api_service: HistoricalApiService,
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """
    Raise an HTTP 400 error when the requested query exceeds API availability.
    """
    with pytest.raises(HTTPException) as exc_info:
        api_service.get_trips(
            vehicle_type="yellow",
            start_time=datetime(2025, 1, 15, 9, 0, 0),
            end_time=datetime(2025, 1, 15, 9, 31, 0),
            limit=10,
            cursor=None,
            mimic_current_time=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ),
            con=duckdb_connection,
        )

    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == (
        "end_time exceeds currently available API range: 2025-01-15T09:30:00-05:00"
    )
