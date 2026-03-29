import textwrap
from datetime import datetime, timezone
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

    def normalize_request_datetime(self, dt: datetime, field_name: str) -> datetime:
        """
        Normalize a request datetime to a timezone-aware UTC datetime.

        Naive datetimes are interpreted as UTC for the current API implementation.

        Args:
            dt: Datetime provided by the API request.
            field_name: Name of the request field, used in error messages.

        Returns:
            A timezone-aware UTC datetime.

        Raises:
            HTTPException: If the datetime is timezone-aware but not in UTC and you
                choose to reject non-UTC input.
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    def get_available_through(self, mimic_current_time: datetime) -> datetime:
        """
        Return the latest timestamp currently available through the API.

        For the initial implementation, the API exposes data up to the supplied
        mimic current time. Later, this method can apply a release cadence such
        as daily, hourly, or every 15 minutes.

        Args:
            mimic_current_time: The current service time supplied by the outer
                service loop.

        Returns:
            The latest timestamp that API requests are allowed to access.
        """
        # use mimic_current_time for now, and later adapt to different service cadence to be more realistic
        return mimic_current_time

    def validate_request_window(
        self,
        start_time: datetime,
        end_time: datetime,
        available_through: datetime,
        limit: int,
    ) -> None:
        """
        Validate the requested API time window and row limit.

        Args:
            start_time: Inclusive lower bound for the request window.
            end_time: Exclusive upper bound for the request window.
            available_through: Latest timestamp currently available through the API.
            limit: Maximum number of rows requested.

        Raises:
            HTTPException: If the time window is invalid, the requested limit is not
                positive, or the requested end_time exceeds the currently available
                API range.
        """
        # enforce a valid half-open time window before querying data
        if start_time >= end_time:
            raise HTTPException(
                status_code=400, detail="start_time must be earlier than end_time"
            )

        # reject zero or negative limits to prevent invalid query behavior
        if limit <= 0:
            raise HTTPException(status_code=400, detail="limit must be positive")

        if end_time > available_through:
            raise HTTPException(
                status_code=400,
                detail=(
                    "end_time exceeds currently available API range: "
                    f"{available_through.isoformat()}"
                ),
            )

    def get_trips(
        self,
        vehicle_type: str,
        start_time: datetime,
        end_time: datetime,
        limit: int,
        mimic_current_time: datetime,
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
            mimic_current_time: Current mimic service time used to derive the
                API availability boundary.
            con: DuckDB connection used to execute the query.

        Returns:
            A JSON-serializable dictionary containing request metadata and the
            matching trip rows.
        """

        # validate request inputs
        start_time = self.normalize_request_datetime(start_time, "start_time")
        end_time = self.normalize_request_datetime(end_time, "end_time")
        available_through = self.get_available_through(mimic_current_time)
        self.validate_request_window(
            start_time=start_time,
            end_time=end_time,
            available_through=available_through,
            limit=limit,
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
            "mimic_current_time": mimic_current_time,
            "rows": rows,
        }
