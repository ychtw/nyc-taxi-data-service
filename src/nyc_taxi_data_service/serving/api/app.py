import textwrap
from collections.abc import Generator
from datetime import datetime
from pathlib import Path

import duckdb
from fastapi import Depends, FastAPI, HTTPException

from nyc_taxi_data_service.config.config_loader import (
    load_runtime_config,
    resolve_runtime_config,
)

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


def get_vehicle_event_time_column(
    vehicle_type: str, resolved_runtime_config: dict
) -> str:
    """
    Return the configured event-time column for the given vehicle type.

    Args:
        vehicle_type: Vehicle type requested by the API client.
        resolved_runtime_config: Fully resolved runtime configuration.

    Returns:
        The event-time column name used to filter records for the given
        vehicle type.

    Raises:
        HTTPException: If the requested vehicle type is not supported.
    """
    mapping = resolved_runtime_config["serving"][
        "service_event_time_column_by_vehicle_type"
    ]
    try:
        return mapping[vehicle_type]
    except KeyError as e:
        raise HTTPException(
            status_code=400, detail=f"Unsupported vehicle_type: {vehicle_type}"
        ) from e


def get_parquet_path(vehicle_type: str, resolved_runtime_config: dict) -> str:
    """
    Build the Parquet glob path for the requested vehicle type.

    Args:
        vehicle_type: Vehicle type requested by the API client.
        resolved_runtime_config: Fully resolved runtime configuration.

    Returns:
        A glob path string that matches partitioned Parquet files for the
        requested vehicle type.
    """
    target_base_path = resolved_runtime_config["storage"]["target_base_path"]
    parquet_path = str(
        Path(target_base_path) / vehicle_type / "year=*" / "month=*" / "*.parquet"
    )
    return parquet_path


@app.get("/trips/{vehicle_type}")
def get_trips(
    vehicle_type: str,
    start_time: datetime,
    end_time: datetime,
    limit: int = 100,
    resolved_runtime_config: dict = Depends(get_resolved_runtime_config),
    con: duckdb.DuckDBPyConnection = Depends(get_duckdb_connection),
):
    """
    Return trip records for a vehicle type within a requested time window.

    The endpoint reads matching Parquet files directly with DuckDB and filters
    records using a half-open time interval:

        start_time <= event_time < end_time

    Args:
        vehicle_type: Requested vehicle type from the route path.
        start_time: Inclusive lower bound for the event-time filter.
        end_time: Exclusive upper bound for the event-time filter.
        limit: Maximum number of rows to return.
        resolved_runtime_config: Resolved runtime configuration injected by
            FastAPI dependency injection.
        con: DuckDB connection injected by FastAPI dependency injection.

    Returns:
        A JSON-serializable dictionary containing request metadata and the
        matching trip rows.

    Raises:
        HTTPException: If the time window is invalid or the requested limit is
            not positive.
    """
    # enforce a valid half-open time window before querying data
    if start_time >= end_time:
        raise HTTPException(
            status_code=400, detail="start_time must be earlier than end_time"
        )

    # reject zero or negative limits to prevent invalid query behavior
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")

    # gather required info for query
    event_time_column = get_vehicle_event_time_column(
        vehicle_type=vehicle_type,
        resolved_runtime_config=resolved_runtime_config,
    )
    parquet_path = get_parquet_path(
        vehicle_type=vehicle_type, resolved_runtime_config=resolved_runtime_config
    )

    # construct query
    query = textwrap.dedent(
        f"""
        SELECT *
        FROM read_parquet(?)
        WHERE {event_time_column} >= ?
            AND {event_time_column} < ?
        ORDER BY {event_time_column} ASC
        LIMIT ?
        """
    )
    query_params = [parquet_path, start_time, end_time, limit]

    # convert the query result into plain Python dictionaries so FastAPI can
    # serialize the response body as JSON
    rows = con.execute(query, query_params).pl().to_dicts()

    return {
        "vehicle_type": vehicle_type,
        "start_time": start_time,
        "end_time": end_time,
        "limit": limit,
        "rows": rows,
    }
