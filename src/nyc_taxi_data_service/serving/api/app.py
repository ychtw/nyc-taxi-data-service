from collections.abc import Generator
from datetime import datetime

import duckdb
from fastapi import Depends, FastAPI

from nyc_taxi_data_service.config.config_loader import (
    load_runtime_config,
    resolve_runtime_config,
)
from nyc_taxi_data_service.serving.api.service import HistoricalApiService

app = FastAPI()


def get_duckdb_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Create a DuckDB connection for a single API request.

    This dependency yields a fresh in-memory DuckDB connection and ensures it is
    closed after the request completes. At this initial stage, the service queries
    Parquet files directly, so a request-scoped in-memory connection is sufficient.
    """
    # open a fresh DuckDB connection for this request
    con = duckdb.connect(database=":memory:")
    try:
        yield con
    finally:
        # close the connection during request teardown
        con.close()


def get_resolved_runtime_config() -> dict:
    """
    Load and resolve the runtime configuration for the API service.

    The API uses the resolved runtime config to determine items such as the
    storage base path and the event-time column for each vehicle type.
    """
    runtime_config = load_runtime_config()
    resolved_runtime_config = resolve_runtime_config(runtime_config=runtime_config)
    return resolved_runtime_config


def get_api_service(
    resolved_runtime_config: dict = Depends(get_resolved_runtime_config),
) -> HistoricalApiService:
    """
    Build the historical API service from the resolved runtime configuration.

    A single service abstraction handles all supported vehicle types. Vehicle-
    specific behavior is resolved dynamically per request.
    """
    return HistoricalApiService(resolved_runtime_config=resolved_runtime_config)


@app.get("/trips/{vehicle_type}")
def get_trips(
    vehicle_type: str,
    start_time: datetime,
    end_time: datetime,
    limit: int = 100,
    api_service: HistoricalApiService = Depends(get_api_service),
    con: duckdb.DuckDBPyConnection = Depends(get_duckdb_connection),
):
    """
    Return trip records for a vehicle type within a requested time window.

    The endpoint delegates request validation and query construction to
    HistoricalApiService, while FastAPI remains responsible for HTTP request
    parsing and dependency injection.

    Args:
        vehicle_type: Requested vehicle type from the route path.
        start_time: Inclusive lower bound for the event-time filter.
        end_time: Exclusive upper bound for the event-time filter.
        limit: Maximum number of rows to return.
        api_service: Historical API service injected by FastAPI.
        con: DuckDB connection injected by FastAPI.

    Returns:
        A JSON-serializable dictionary containing request metadata and matching
        trip rows.
    """

    return api_service.get_trips(
        vehicle_type=vehicle_type,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        con=con,
    )
