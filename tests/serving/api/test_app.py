from datetime import datetime
from pathlib import Path
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from fastapi.testclient import TestClient

import nyc_taxi_data_service.serving.api.app as api_app_module
from nyc_taxi_data_service.serving.api.service import HistoricalApiService

NY_TZ = ZoneInfo("America/New_York")


class DummyClock:
    """
    Return a fixed mimic current time for API endpoint tests.
    """

    def __init__(self, now_value: datetime):
        self._now_value = now_value

    def now(self) -> datetime:
        return self._now_value


@pytest.fixture
def api_test_data_dir(tmp_path: Path) -> Path:
    """
    Create a minimal parquet layout that matches the API's expected glob pattern.

    Layout:
        <tmp>/yellow/year=2025/month=01/*.parquet

    The TLC-style event-time column is written as naive New York-local
    timestamps so it matches the API service's DuckDB query semantics.
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


@pytest.fixture
def client(resolved_runtime_config: dict):
    """
    Create a TestClient backed by test app state and clean up after the test.
    """
    api_app_module.app.state.api_service = HistoricalApiService(
        resolved_runtime_config=resolved_runtime_config
    )
    api_app_module.app.state.service_clock = DummyClock(
        now_value=datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ)
    )

    with TestClient(api_app_module.app) as test_client:
        yield test_client


def test_get_trips_returns_rows_and_pagination_metadata_for_valid_request(
    client: TestClient,
) -> None:
    """
    Return matching rows and expected pagination metadata for a valid first-page request.
    """
    response = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:01:00",
            "limit": 10,
        },
    )

    assert response.status_code == 200

    payload = response.json()
    assert payload["vehicle_type"] == "yellow"
    assert payload["start_time"] == "2025-01-15T09:00:00-05:00"
    assert payload["end_time"] == "2025-01-15T09:01:00-05:00"
    assert payload["limit"] == 10
    assert payload["returned_count"] == 2
    assert payload["has_more"] is False
    assert payload["next_cursor"] is None
    assert payload["available_through"] == "2025-01-15T09:30:00-05:00"
    assert payload["mimic_current_time"] == "2025-01-15T09:30:00-05:00"
    assert len(payload["rows"]) == 2

    returned_times = [row["tpep_dropoff_datetime"] for row in payload["rows"]]
    assert returned_times == [
        "2025-01-15T09:00:00",
        "2025-01-15T09:00:30",
    ]

    # DuckDB-only pagination metadata columns should not be exposed in response rows.
    assert "filename" not in payload["rows"][0]
    assert "file_row_number" not in payload["rows"][0]


def test_get_trips_returns_400_for_unsupported_vehicle_type(
    client: TestClient,
) -> None:
    """
    Reject requests for vehicle types that are not supported by config.
    """
    response = client.get(
        "/api/v1/trips/fakeveh",
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
) -> None:
    """
    Reject requests whose time window is empty or reversed.
    """
    response = client.get(
        "/api/v1/trips/yellow",
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
) -> None:
    """
    Reject requests whose limit is zero or negative.
    """
    response = client.get(
        "/api/v1/trips/yellow",
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


def test_get_trips_returns_400_when_limit_exceeds_maximum(
    client: TestClient,
) -> None:
    """
    Reject requests whose limit exceeds the maximum allowed page size.
    """
    response = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:01:00",
            "limit": 1001,
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "limit must be no more than 1000",
    }


def test_get_trips_returns_400_when_end_time_exceeds_available_range(
    client: TestClient,
) -> None:
    """
    Reject requests whose end_time exceeds the current API availability boundary.
    """
    response = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:31:00",
            "limit": 10,
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": (
            "end_time exceeds currently available API range: 2025-01-15T09:30:00-05:00"
        ),
    }


def test_get_trips_returns_422_for_invalid_datetime_input(
    client: TestClient,
) -> None:
    """
    Let FastAPI reject malformed datetime query parameters before endpoint logic runs.
    """
    response = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "abc",
            "end_time": "2025-01-15T09:01:00",
            "limit": 10,
        },
    )

    assert response.status_code == 422


def test_get_trips_returns_empty_rows_when_no_records_match(
    client: TestClient,
) -> None:
    """
    Return an empty rows list when the request is valid but no data matches.
    """
    response = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T08:00:00",
            "end_time": "2025-01-15T08:01:00",
            "limit": 10,
        },
    )

    assert response.status_code == 200

    payload = response.json()
    assert payload["vehicle_type"] == "yellow"
    assert payload["returned_count"] == 0
    assert payload["has_more"] is False
    assert payload["next_cursor"] is None
    assert payload["rows"] == []


def test_get_trips_returns_next_cursor_when_more_rows_exist(
    client: TestClient,
) -> None:
    """
    Return a next cursor and set has_more when additional rows remain after the current page.
    """
    response = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:02:00",
            "limit": 2,
        },
    )

    assert response.status_code == 200

    payload = response.json()
    assert payload["returned_count"] == 2
    assert payload["has_more"] is True
    assert payload["next_cursor"] is not None
    assert len(payload["rows"]) == 2
    assert [row["tpep_dropoff_datetime"] for row in payload["rows"]] == [
        "2025-01-15T09:00:00",
        "2025-01-15T09:00:30",
    ]


def test_get_trips_accepts_cursor_and_returns_continuation_page(
    client: TestClient,
) -> None:
    """
    Return the next page of rows when the client supplies a valid cursor.
    """
    first_response = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:02:00",
            "limit": 2,
        },
    )
    assert first_response.status_code == 200

    first_payload = first_response.json()
    assert first_payload["has_more"] is True
    assert first_payload["next_cursor"] is not None

    second_response = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:02:00",
            "limit": 2,
            "cursor": first_payload["next_cursor"],
        },
    )
    assert second_response.status_code == 200

    second_payload = second_response.json()
    assert second_payload["returned_count"] == 1
    assert second_payload["has_more"] is False
    assert second_payload["next_cursor"] is None
    assert [row["tpep_dropoff_datetime"] for row in second_payload["rows"]] == [
        "2025-01-15T09:01:00",
    ]


def test_get_trips_returns_400_for_invalid_cursor(
    client: TestClient,
) -> None:
    """
    Reject continuation requests whose cursor is malformed.
    """
    response = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:02:00",
            "limit": 2,
            "cursor": "not-a-valid-cursor",
        },
    )

    assert response.status_code == 400
    assert response.json() == {
        "detail": "Invalid cursor",
    }


def test_get_trips_respects_limit(client: TestClient) -> None:
    """
    Return no more than the requested number of rows.
    """
    response = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:02:00",
            "limit": 1,
        },
    )

    assert response.status_code == 200

    payload = response.json()
    assert payload["limit"] == 1
    assert payload["returned_count"] == 1
    assert len(payload["rows"]) == 1
    assert payload["rows"][0]["tpep_dropoff_datetime"] == "2025-01-15T09:00:00"


def test_create_app_builds_service_clock_with_helper(mocker) -> None:
    """
    create_app() should build the app-level ServiceClock via _build_service_clock().
    """
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_event_time_column_by_vehicle_type": {
                "yellow": "tpep_dropoff_datetime",
                "green": "lpep_dropoff_datetime",
                "fhv": "dropOff_datetime",
                "fhvhv": "dropoff_datetime",
            },
            "api": {
                "rate_limit": {
                    "enabled": True,
                    "max_concurrent_requests": 4,
                    "requests_per_second": 10,
                    "burst_capacity": 20,
                }
            },
        },
        "storage": {"target_base_path": "data/raw"},
    }

    mock_service_clock = Mock()
    mock_service_clock.mimic_service_start_time = datetime(
        2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ
    )

    mock_load_serving_runtime_config = mocker.patch(
        "nyc_taxi_data_service.serving.api.app.load_serving_runtime_config",
        return_value=resolved_runtime_config,
    )
    mock_build_service_clock = mocker.patch(
        "nyc_taxi_data_service.serving.api.app._build_service_clock",
        return_value=mock_service_clock,
    )

    created_app = api_app_module.create_app()

    mock_load_serving_runtime_config.assert_called_once_with()
    mock_build_service_clock.assert_called_once_with(resolved_runtime_config)

    assert created_app.state.service_clock is mock_service_clock
    assert isinstance(created_app.state.api_service, HistoricalApiService)


def test_create_app_stores_historical_api_service_with_resolved_config(mocker) -> None:
    """
    create_app() should store a HistoricalApiService built from the resolved runtime config.
    """
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_event_time_column_by_vehicle_type": {
                "yellow": "tpep_dropoff_datetime",
                "green": "lpep_dropoff_datetime",
                "fhv": "dropOff_datetime",
                "fhvhv": "dropoff_datetime",
            },
            "api": {
                "rate_limit": {
                    "enabled": True,
                    "max_concurrent_requests": 4,
                    "requests_per_second": 10,
                    "burst_capacity": 20,
                }
            },
        },
        "storage": {"target_base_path": "data/raw"},
    }

    mocker.patch(
        "nyc_taxi_data_service.serving.api.app.load_serving_runtime_config",
        return_value=resolved_runtime_config,
    )

    mock_service_clock = Mock()
    mock_service_clock.mimic_service_start_time = datetime(
        2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.api.app._build_service_clock",
        return_value=mock_service_clock,
    )

    created_app = api_app_module.create_app()

    assert created_app.state.service_clock is mock_service_clock
    assert isinstance(created_app.state.api_service, HistoricalApiService)
    assert (
        created_app.state.api_service._resolved_runtime_config
        == resolved_runtime_config
    )


def test_get_trips_full_pagination_traversal(client: TestClient) -> None:
    """
    Iterate through all pages using next_cursor and verify:
    - no duplicates
    - no missing rows
    - correct ordering across pages
    """

    all_rows = []
    cursor = None

    while True:
        response = client.get(
            "/api/v1/trips/yellow",
            params={
                "start_time": "2025-01-15T09:00:00",
                "end_time": "2025-01-15T09:02:00",
                "limit": 2,
                **({"cursor": cursor} if cursor else {}),
            },
        )

        assert response.status_code == 200
        payload = response.json()

        all_rows.extend(payload["rows"])

        if not payload["has_more"]:
            break

        cursor = payload["next_cursor"]
        assert cursor is not None  # safety check

    # final validation
    assert len(all_rows) == 3

    # ensure correct ordering
    returned_times = [row["tpep_dropoff_datetime"] for row in all_rows]
    assert returned_times == [
        "2025-01-15T09:00:00",
        "2025-01-15T09:00:30",
        "2025-01-15T09:01:00",
    ]


def test_pagination_has_no_duplicates(client: TestClient):
    """
    Verify that cursor-based pagination does not return duplicate records
    across pages.

    This test iterates through all pages using successive `next_cursor` values
    and tracks a composite key for each row. It asserts that no row appears
    more than once across the full traversal.

    This ensures that the continuation query correctly resumes *after* the
    last row of the previous page, rather than overlapping with it.
    """
    seen = set()
    cursor = None

    while True:
        response = client.get(
            "/api/v1/trips/yellow",
            params={
                "start_time": "2025-01-15T09:00:00",
                "end_time": "2025-01-15T09:02:00",
                "limit": 2,
                **({"cursor": cursor} if cursor else {}),
            },
        )

        payload = response.json()

        for row in payload["rows"]:
            key = (
                row["tpep_dropoff_datetime"],
                row["fare_amount"],
            )
            assert key not in seen
            seen.add(key)

        if not payload["has_more"]:
            break

        cursor = payload["next_cursor"]


def test_cursor_is_deterministic(client: TestClient):
    """
    Verify that a given cursor produces deterministic results.

    This test issues multiple requests using the same cursor and asserts that
    the returned rows are identical across calls.

    This ensures that cursor decoding and continuation query logic are stable
    and do not depend on non-deterministic factors such as execution order or
    implicit state.
    """
    first = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:02:00",
            "limit": 2,
        },
    ).json()

    cursor = first["next_cursor"]

    second_a = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:02:00",
            "limit": 2,
            "cursor": cursor,
        },
    ).json()

    second_b = client.get(
        "/api/v1/trips/yellow",
        params={
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:02:00",
            "limit": 2,
            "cursor": cursor,
        },
    ).json()

    assert second_a["rows"] == second_b["rows"]


def test_pagination_respects_limit_on_all_pages(client: TestClient):
    """
    Verify that each page respects the requested limit across the full
    pagination sequence.

    This test iterates through all pages and asserts that the number of rows
    returned in each response does not exceed the specified limit.

    This ensures that the service consistently enforces page size constraints,
    including for continuation queries.
    """
    cursor = None

    while True:
        response = client.get(
            "/api/v1/trips/yellow",
            params={
                "start_time": "2025-01-15T09:00:00",
                "end_time": "2025-01-15T09:02:00",
                "limit": 2,
                **({"cursor": cursor} if cursor else {}),
            },
        )

        payload = response.json()

        assert len(payload["rows"]) <= 2

        if not payload["has_more"]:
            break

        cursor = payload["next_cursor"]


def test_create_app_skips_concurrency_limiter_when_rate_limit_disabled(mocker) -> None:
    """
    create_app() should not attach a concurrency limiter when rate limiting is disabled.
    """
    resolved_runtime_config = {
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_event_time_column_by_vehicle_type": {
                "yellow": "tpep_dropoff_datetime",
                "green": "lpep_dropoff_datetime",
                "fhv": "dropOff_datetime",
                "fhvhv": "dropoff_datetime",
            },
            "api": {
                "rate_limit": {
                    "enabled": False,
                    "max_concurrent_requests": 4,
                    "requests_per_second": 10,
                    "burst_capacity": 20,
                }
            },
        },
        "storage": {"target_base_path": "data/raw"},
    }

    mocker.patch(
        "nyc_taxi_data_service.serving.api.app.load_serving_runtime_config",
        return_value=resolved_runtime_config,
    )

    mock_service_clock = Mock()
    mock_service_clock.mimic_service_start_time = datetime(
        2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.api.app._build_service_clock",
        return_value=mock_service_clock,
    )

    created_app = api_app_module.create_app()

    assert not hasattr(created_app.state, "api_concurrency_limiter")
