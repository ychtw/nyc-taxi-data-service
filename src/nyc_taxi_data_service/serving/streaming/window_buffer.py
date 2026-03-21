import re
from datetime import datetime, timedelta
from pathlib import Path

import duckdb


def _parse_window_size(window_size: str) -> timedelta:
    """
    Parse and validate a streaming window-size string into a timedelta.

    The runtime config stores the streaming window size as a compact duration
    string such as "10m", "2h", or "1d". This helper validates the
    format, converts the value to a timedelta, and enforces project-level
    bounds for acceptable streaming query windows.

    Supported format:
    - one or more digits, followed immediately by a unit
    - allowed units:
        - m: minutes
        - h: hours
        - d: days

    Examples of valid values:
    - "10m"
    - "30m"
    - "2h"
    - "1d"

    Examples of invalid values:
    - "30s"  # seconds are intentionally not supported
    - "90"
    - "1hour"
    - " 30m"
    - "30 m"
    - "2D"

    Validation rules:
    - window_size must be a string in the supported compact format
    - seconds are forbidden because windows shorter than 1 minute are too small
      for this project and would likely lead to excessive query frequency
    - the effective duration must be at least 1 minute
    - the effective duration must be no more than 1 day

    Args:
        window_size: Streaming window-size string from runtime config, such as
            "30m", "2h", or "1d".

    Returns:
        A timedelta representing the validated window size.

    Raises:
        ValueError:
            - if window_size is not a string
            - if the format is invalid
            - if the unit is unsupported
            - if the parsed duration is shorter than 1 minute
            - if the parsed duration is longer than 1 day
    """
    if not isinstance(window_size, str):
        raise ValueError(
            f"window_size must be a string, got {type(window_size).__name__}"
        )

    # require a compact "<integer><unit>" format with no spaces
    # examples:
    #   "30m" -> valid
    #   "2h"  -> valid
    #   "1d"  -> valid
    #   "30 s", "1hour", "m30" -> invalid
    match = re.fullmatch(r"(\d+)([mhd])", window_size)
    if match is None:
        raise ValueError(
            "Invalid window_size format. Expected a compact duration like "
            "'1m', '30m', '2h', or '1d'. "
            "Allowed units are: m (minutes), h (hours), d (days)."
        )

    value_str, unit = match.groups()
    value = int(value_str)

    # convert the parsed value into timedelta
    if unit == "m":
        duration = timedelta(minutes=value)
    elif unit == "h":
        duration = timedelta(hours=value)
    else:  # unit == "d"
        duration = timedelta(days=value)

    min_window = timedelta(minutes=1)
    max_window = timedelta(days=1)

    # enforce project-level window bounds
    if duration < min_window:
        raise ValueError(
            f"window_size '{window_size}' is too short. Minimum allowed value is 1m."
        )

    if duration > max_window:
        raise ValueError(
            f"window_size '{window_size}' is too long. Maximum allowed value is 1d."
        )

    return duration


class StreamingWindowBuffer:
    def __init__(
        self,
        vehicle_type: str,
        resolved_runtime_config: dict,
        mimic_service_start_time: datetime,
    ):
        """
        Initialize a streaming window buffer for one vehicle type.

        This class manages streaming-window state for a single vehicle type,
        including:
        - the configured streaming window size
        - the event-time column used for filtering records
        - the vehicle-specific parquet data path
        - the per-vehicle DuckDB buffer database path
        - the next window start time for querying new records

        On initialization, the buffer starts from the provided
        `mimic_service_start_time` and resets its DuckDB buffer database
        to a clean state for the current service run.

        Args:
            vehicle_type: Vehicle type handled by this buffer, such as
                "yellow", "green", "fhv", or "fhvhv".
            resolved_runtime_config: Resolved runtime configuration containing
                serving and storage settings.
            mimic_service_start_time: Initial start timestamp for the first
                streaming query window. This is usually derived once from the
                shared service clock and passed in so all vehicle buffers begin
                from the same service-time boundary.
        """
        # general settings
        self.window_size = _parse_window_size(
            resolved_runtime_config["serving"]["streaming"]["buffer"]["window_size"]
        )

        # vehicle-type-specific settings
        self.vehicle_type = vehicle_type
        self.event_time_column = resolved_runtime_config["serving"][
            "service_event_time_column_by_vehicle_type"
        ][vehicle_type]
        self.vehicle_data_path = (
            Path(resolved_runtime_config["storage"]["target_base_path"]) / vehicle_type
        )

        # buffer database settings
        buffer_cfg = resolved_runtime_config["serving"]["streaming"]["buffer"]
        self.buffer_table_name = buffer_cfg["db_table_name"]
        self.buffer_db_path = (
            Path(buffer_cfg["db_base_path"]) / f"{vehicle_type}.duckdb"
        )

        # streaming progress state: start the first query window at the
        # provided mimic service start time
        self._next_window_start = mimic_service_start_time

        # initialize a duckdb database
        self._initialize_buffer_db()

    def _initialize_buffer_db(self) -> None:
        """
        Create a fresh DuckDB buffer database file for this vehicle type.

        Any existing buffer database file at the same path is removed first so
        each service run starts from a clean state.
        """
        # ensure the directory exists
        self.buffer_db_path.parent.mkdir(parents=True, exist_ok=True)

        # if the file exists, delete it to ensure a clean state (idempotency)
        if self.buffer_db_path.exists():
            self.buffer_db_path.unlink()

        # open and close a connection once so DuckDB creates the database file
        with duckdb.connect(str(self.buffer_db_path)) as con:
            pass

    def get_current_window(self) -> tuple[datetime, datetime]:
        """
        Return the current query window for this buffer.

        The window starts at `_next_window_start` and ends one configured
        `window_size` later.

        Returns:
            A `(window_start, window_end)` tuple representing the current
            streaming query window.
        """
        start = self._next_window_start
        end = start + self.window_size
        return start, end

    def advance_window(self, window_end: datetime) -> None:
        """
        Advance the next query start time to the given window end.

        This should be called only after the current window has been
        successfully buffered, so that future queries continue from the end
        of the last completed window.

        Args:
            window_end: End timestamp of the window that was successfully
                processed.
        """
        self._next_window_start = window_end

    @staticmethod
    def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        """
        Return True if the given table exists in the current DuckDB connection.

        This attempts to access the table and returns False if DuckDB raises
        CatalogException.
        """
        try:
            con.table(table_name)
            return True
        except duckdb.CatalogException:
            return False

    def buffer_current_window(self) -> int:
        """
        Buffer one full streaming window of records into the DuckDB buffer table.

        This method queries records whose event time falls within the current
        window returned by `get_current_window()`, materializes them into a
        temporary table, and then inserts them into the buffer table.

        The buffer table is created on first use and appended to on later calls.

        Returns:
            Number of rows buffered for this window.
            Returns 0 if the window contains no matching records.
        """
        window_start, window_end = self.get_current_window()

        # define required info for query
        parquet_path = str(self.vehicle_data_path / "year=*" / "month=*" / "*.parquet")
        window_start_str = window_start.strftime("%Y-%m-%d %H:%M:%S")
        window_end_str = window_end.strftime("%Y-%m-%d %H:%M:%S")

        # query for data within the current window and save to the buffer table in the buffer database
        with duckdb.connect(str(self.buffer_db_path)) as con:
            # create a temp table to materialize the filtered data
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE temp_window AS
                SELECT *
                FROM read_parquet('{parquet_path}')
                WHERE {self.event_time_column} >= TIMESTAMP '{window_start_str}'
                AND {self.event_time_column} < TIMESTAMP '{window_end_str}'
                ORDER BY {self.event_time_column} ASC
            """)

            # get count from the temp table (avoid scanning parquet files)
            row_count = con.execute("SELECT count(*) FROM temp_window").fetchone()[0]

            # if the window contains no records, still advance the window
            if row_count == 0:
                self.advance_window(window_end)
                return 0

            # persist matching records from temp table into the buffer table
            if self._table_exists(con, self.buffer_table_name):
                con.execute(
                    f"INSERT INTO {self.buffer_table_name} SELECT * FROM temp_window"
                )
            else:
                con.execute(
                    f"CREATE TABLE {self.buffer_table_name} AS SELECT * FROM temp_window"
                )

            # cleanup temp table (optional; connection close would also remove it)
            con.execute("DROP TABLE temp_window")

        # advance to the next query window after successful buffering
        self.advance_window(window_end)

        return row_count
