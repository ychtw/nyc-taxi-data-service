import logging
from collections.abc import Generator
from datetime import date, datetime

import duckdb
from fastapi import Depends, FastAPI, Request

from nyc_taxi_data_service.serving.api.service import HistoricalApiService
from nyc_taxi_data_service.serving.common.log_time import format_log_dt
from nyc_taxi_data_service.serving.common.service_clock_loader import (
    _build_service_clock,
)
from nyc_taxi_data_service.serving.common.startup_context_loader import (
    load_serving_runtime_config,
)

logger = logging.getLogger(__name__)


def get_duckdb_connection() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """
    Create a DuckDB connection for a single API request.

    This dependency yields a fresh in-memory DuckDB connection and ensures it is
    closed after the request completes. At this initial stage, the API service queries
    parquet files directly, so a request-scoped in-memory connection is sufficient.
    """
    # open a fresh DuckDB connection for this request
    con = duckdb.connect(database=":memory:")
    try:
        yield con
    finally:
        # close the connection during request teardown
        con.close()


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application for the historical data API service.
    """
    app = FastAPI()

    # load runtime config:
    # - with SERVICE_STARTUP_CONTEXT_PATH env variable -> use shared startup context JSON
    # - otherwise -> resolve runtime.yaml (standalone mode)
    resolved_runtime_config = load_serving_runtime_config()

    # build long-lived API service components once and store them on app state
    mimic_current_date = date.fromisoformat(
        resolved_runtime_config["serving"]["mimic_current_date"]
    )
    app.state.service_clock = _build_service_clock(resolved_runtime_config)
    app.state.api_service = HistoricalApiService(
        resolved_runtime_config=resolved_runtime_config
    )

    logger.info(
        "Starting NYC taxi historical API service | mimic_current_date=%s | mimic_service_start_time=%s",
        mimic_current_date,
        format_log_dt(app.state.service_clock.mimic_service_start_time),
    )

    @app.get("/trips/{vehicle_type}")
    def get_trips(
        request: Request,
        vehicle_type: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 100,
        con: duckdb.DuckDBPyConnection = Depends(get_duckdb_connection),
    ) -> dict:
        """
        Return trip records for a vehicle type within a requested time window.

        The endpoint delegates request validation and query construction to
        HistoricalApiService, while FastAPI remains responsible for HTTP request
        parsing and dependency injection.

        Args:
            vehicle_type: Requested vehicle type from the route path.
            start_time: Inclusive lower bound for the requested event-time window.
            end_time: Exclusive upper bound for the requested event-time window.
            limit: Maximum number of rows to return.
            con: DuckDB connection injected by FastAPI.

        Returns:
            A JSON-serializable dictionary containing request metadata and matching
            trip rows.
        """
        api_service: HistoricalApiService = request.app.state.api_service

        # service clock now returns the current mimic time as an aware
        # America/New_York datetime
        mimic_current_time: datetime = request.app.state.service_clock.now()

        logger.debug(
            "Received API trip request | vehicle_type=%s | start_time=%s | end_time=%s | limit=%d | mimic_current_time=%s",
            vehicle_type,
            format_log_dt(start_time),
            format_log_dt(end_time),
            limit,
            format_log_dt(mimic_current_time),
        )

        return api_service.get_trips(
            vehicle_type=vehicle_type,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            mimic_current_time=mimic_current_time,
            con=con,
        )

    return app


app = create_app()
