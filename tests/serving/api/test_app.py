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


# ============================================================
# Helpers
# ============================================================


class DummyClock:
    """Return a fixed mimic current time for API endpoint tests."""

    def __init__(self, now_value: datetime):
        self._now_value = now_value

    def now(self) -> datetime:
        return self._now_value


def get_trips(client: TestClient, **params):
    """Helper to call /trips endpoint."""
    return client.get("/api/v1/trips/yellow", params=params)


def iterate_pages(client: TestClient, base_params: dict):
    """Yield payloads across pagination."""
    cursor = None

    while True:
        response = get_trips(
            client, **base_params, **({"cursor": cursor} if cursor else {})
        )
        payload = response.json()

        yield payload

        if not payload["has_more"]:
            break

        cursor = payload["next_cursor"]


def build_runtime_config(rate_limit_enabled: bool = True) -> dict:
    """Return a consistent resolved_runtime_config for create_app tests."""
    return {
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
                    "enabled": rate_limit_enabled,
                    "max_concurrent_requests": 4,
                    "requests_per_second": 10,
                    "burst_capacity": 20,
                }
            },
        },
        "storage": {"target_base_path": "data/raw"},
        "time_frame": {
            "start_year_month": "2025-01",
            "end_year_month": "2025-03",
        },
        "vehicle_types": ["yellow", "green", "fhv", "fhvhv"],
    }


# ============================================================
# Fixtures
# ============================================================


@pytest.fixture
def api_test_data_dir(tmp_path: Path) -> Path:
    """Create minimal parquet layout for testing."""
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
    """Return minimal runtime config for endpoint tests."""
    return {
        "storage": {"target_base_path": str(api_test_data_dir)},
        "serving": {
            "service_event_time_column_by_vehicle_type": {
                "yellow": "tpep_dropoff_datetime",
                "green": "lpep_dropoff_datetime",
                "fhv": "dropOff_datetime",
                "fhvhv": "dropoff_datetime",
            }
        },
        "time_frame": {"start_year_month": "2025-01", "end_year_month": "2025-03"},
        "vehicle_types": ["yellow", "green", "fhv", "fhvhv"],
    }


@pytest.fixture
def client(resolved_runtime_config: dict):
    """Create TestClient with injected app state."""
    api_app_module.app.state.api_service = HistoricalApiService(
        resolved_runtime_config=resolved_runtime_config
    )
    api_app_module.app.state.service_clock = DummyClock(
        datetime(2025, 1, 15, 9, 30, 0, tzinfo=NY_TZ)
    )

    with TestClient(api_app_module.app) as test_client:
        yield test_client


# ============================================================
# Core endpoint tests
# ============================================================


def test_get_trips_returns_rows_and_metadata(client: TestClient):
    """Return rows and pagination metadata for valid request."""
    response = get_trips(
        client,
        start_time="2025-01-15T09:00:00",
        end_time="2025-01-15T09:01:00",
        limit=10,
    )

    payload = response.json()

    assert response.status_code == 200
    assert payload["returned_count"] == 2
    assert payload["has_more"] is False
    assert payload["next_cursor"] is None
    assert len(payload["rows"]) == 2
    assert "filename" not in payload["rows"][0]


def test_get_trips_returns_400_for_unsupported_vehicle_type(client):
    """Reject unsupported vehicle type."""
    response = client.get(
        "/api/v1/trips/fakeveh",
        params={"start_time": "2025-01-15T09:00:00", "end_time": "2025-01-15T09:01:00"},
    )

    assert response.status_code == 400


@pytest.mark.parametrize(
    "params, error",
    [
        (
            {"start_time": "2025-01-15T09:01:00", "end_time": "2025-01-15T09:01:00"},
            "start_time must be earlier",
        ),
        ({"limit": 0}, "limit must be positive"),
        ({"limit": 1001}, "limit must be no more than 1000"),
    ],
)
def test_get_trips_common_validation_errors(client, params, error):
    """Reject invalid request parameters."""
    base = {
        "start_time": "2025-01-15T09:00:00",
        "end_time": "2025-01-15T09:01:00",
        "limit": 10,
    }
    base.update(params)

    response = get_trips(client, **base)

    assert response.status_code == 400
    assert error in response.json()["detail"]


def test_get_trips_returns_422_for_invalid_datetime(client):
    """Reject malformed datetime."""
    response = get_trips(client, start_time="abc", end_time="2025-01-15T09:01:00")

    assert response.status_code == 422


def test_get_trips_returns_empty_rows(client):
    """Return empty result when no match."""
    response = get_trips(
        client,
        start_time="2025-01-15T08:00:00",
        end_time="2025-01-15T08:01:00",
    )

    assert response.json()["rows"] == []


# ============================================================
# Pagination tests
# ============================================================


def test_pagination_full_traversal(client):
    """Traverse all pages without loss."""
    all_rows = []

    for payload in iterate_pages(
        client,
        {
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:02:00",
            "limit": 2,
        },
    ):
        all_rows.extend(payload["rows"])

    assert len(all_rows) == 3


def test_pagination_has_no_duplicates(client):
    """Ensure no duplicates across pages."""
    seen = set()

    for payload in iterate_pages(
        client,
        {
            "start_time": "2025-01-15T09:00:00",
            "end_time": "2025-01-15T09:02:00",
            "limit": 2,
        },
    ):
        for row in payload["rows"]:
            key = (row["tpep_dropoff_datetime"], row["fare_amount"])
            assert key not in seen
            seen.add(key)


def test_cursor_is_deterministic(client):
    """Same cursor returns same results."""
    first = get_trips(
        client,
        start_time="2025-01-15T09:00:00",
        end_time="2025-01-15T09:02:00",
        limit=2,
    ).json()

    cursor = first["next_cursor"]

    second_a = get_trips(
        client,
        cursor=cursor,
        start_time="2025-01-15T09:00:00",
        end_time="2025-01-15T09:02:00",
    ).json()
    second_b = get_trips(
        client,
        cursor=cursor,
        start_time="2025-01-15T09:00:00",
        end_time="2025-01-15T09:02:00",
    ).json()

    assert second_a["rows"] == second_b["rows"]


# ============================================================
# App creation tests
# ============================================================


def test_create_app_builds_service_clock(mocker):
    """create_app builds ServiceClock."""
    config = build_runtime_config()

    mock_clock = Mock()
    mocker.patch(
        "nyc_taxi_data_service.serving.api.app.load_serving_runtime_config",
        return_value=config,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.api.app._build_service_clock",
        return_value=mock_clock,
    )

    app = api_app_module.create_app()

    assert app.state.service_clock is mock_clock
    assert isinstance(app.state.api_service, HistoricalApiService)


def test_create_app_stores_api_service(mocker):
    """create_app stores HistoricalApiService."""
    config = build_runtime_config()

    mocker.patch(
        "nyc_taxi_data_service.serving.api.app.load_serving_runtime_config",
        return_value=config,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.api.app._build_service_clock",
        return_value=Mock(),
    )

    app = api_app_module.create_app()

    assert app.state.api_service._resolved_runtime_config == config


def test_create_app_skips_rate_limit_when_disabled(mocker):
    """create_app skips limiter if disabled."""
    config = build_runtime_config(rate_limit_enabled=False)

    mocker.patch(
        "nyc_taxi_data_service.serving.api.app.load_serving_runtime_config",
        return_value=config,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.api.app._build_service_clock",
        return_value=Mock(),
    )

    app = api_app_module.create_app()

    assert not hasattr(app.state, "api_concurrency_limiter")


# ============================================================
# Service context
# ============================================================


def test_get_service_context_returns_payload(client):
    """Return service context from app state."""
    api_app_module.app.state.service_context = {"test": "value"}

    response = client.get("/api/v1/service/context")

    assert response.status_code == 200
    assert response.json() == {"test": "value"}
