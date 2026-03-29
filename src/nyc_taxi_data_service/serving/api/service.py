import textwrap
from datetime import datetime
from pathlib import Path

import duckdb
from fastapi import HTTPException


class HistoricalApiService:
    """
    Serve historically available trip data for the API layer.

    This service handles all supported vehicle types. Vehicle-specific details,
    such as the event-time column and Parquet path, are resolved from the
    runtime configuration for each request.
    """

    def __init__(self, resolved_runtime_config: dict):
        """
        Initialize the historical API service.

        Args:
            resolved_runtime_config: Fully resolved runtime configuration used
                by the API layer.
        """
        self._resolved_runtime_config = resolved_runtime_config

    def get_vehicle_event_time_column(self, vehicle_type: str) -> str:
        """
        Return the configured event-time column for the given vehicle type.

        Args:
            vehicle_type: Vehicle type requested by the API client.

        Returns:
            The event-time column name used to filter records for the given
            vehicle type.

        Raises:
            HTTPException: If the requested vehicle type is not supported.
        """
        mapping = self._resolved_runtime_config["serving"][
            "service_event_time_column_by_vehicle_type"
        ]
        try:
            return mapping[vehicle_type]
        except KeyError as e:
            raise HTTPException(
                status_code=400, detail=f"Unsupported vehicle_type: {vehicle_type}"
            ) from e

    def get_parquet_path(self, vehicle_type: str) -> str:
        """
        Build the Parquet glob path for the requested vehicle type.

        Args:
            vehicle_type: Vehicle type requested by the API client.

        Returns:
            A glob path string that matches partitioned Parquet files for the
            requested vehicle type.
        """
        target_base_path = self._resolved_runtime_config["storage"]["target_base_path"]
        parquet_path = str(
            Path(target_base_path) / vehicle_type / "year=*" / "month=*" / "*.parquet"
        )
        return parquet_path

    def validate_request_window(
        self,
        start_time: datetime,
        end_time: datetime,
        limit: int,
    ) -> None:
        """
        Validate the requested API time window and row limit.

        Args:
            start_time: Inclusive lower bound for the request window.
            end_time: Exclusive upper bound for the request window.
            limit: Maximum number of rows requested.

        Raises:
            HTTPException: If the time window is invalid or the limit is not
                positive.
        """
        # enforce a valid half-open time window before querying data
        if start_time >= end_time:
            raise HTTPException(
                status_code=400, detail="start_time must be earlier than end_time"
            )

        # reject zero or negative limits to prevent invalid query behavior
        if limit <= 0:
            raise HTTPException(status_code=400, detail="limit must be positive")

    def get_trips(
        self,
        vehicle_type: str,
        start_time: datetime,
        end_time: datetime,
        limit: int,
        con: duckdb.DuckDBPyConnection,
    ) -> dict:
        """
        Return trip rows for the requested vehicle type and time window.

        The query uses a half-open interval:

            start_time <= event_time < end_time

        This avoids overlap at boundary timestamps when adjacent requests are
        made.

        Args:
            vehicle_type: Requested vehicle type.
            start_time: Inclusive lower bound for the event-time filter.
            end_time: Exclusive upper bound for the event-time filter.
            limit: Maximum number of rows to return.
            con: DuckDB connection used to execute the query.

        Returns:
            A JSON-serializable dictionary containing request metadata and the
            matching trip rows.
        """

        # validate request inputs
        self.validate_request_window(
            start_time=start_time, end_time=end_time, limit=limit
        )

        # gather required info for the query
        event_time_column = self.get_vehicle_event_time_column(
            vehicle_type=vehicle_type
        )
        parquet_path = self.get_parquet_path(vehicle_type=vehicle_type)

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
