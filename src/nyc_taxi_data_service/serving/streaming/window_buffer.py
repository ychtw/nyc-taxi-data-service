import logging
import re
import textwrap
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

logger = logging.getLogger(__name__)


def _parse_buffer_duration(raw_value: str, field_name: str) -> timedelta:
    """
    Parse and validate a streaming buffer duration string into a timedelta.

    The runtime config stores streaming buffer durations such as `window_size`
    and `retention` as compact strings like "10m", "2h", or "1d". This helper
    validates the format, converts the value to a timedelta, and enforces the
    shared duration bounds used by the streaming buffer config.

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
    - `raw_value` must be a string in the supported compact format
    - seconds are forbidden because windows shorter than 1 minute are too small
      for this project and would likely lead to excessive query frequency
    - the effective duration must be at least 1 minute
    - the effective duration must be no more than 1 day

    Args:
        raw_value: Raw duration string from runtime config, such as
            "30m", "2h", or "1d".
        field_name: Config field name used in validation error messages,
            such as "window_size" or "retention".

    Returns:
        A validated timedelta for the requested buffer duration field.

    Raises:
        ValueError:
            - if `raw_value` is not a string
            - if the format is invalid
            - if the unit is unsupported
            - if the parsed duration is shorter than 1 minute
            - if the parsed duration is longer than 1 day
    """
    if not isinstance(raw_value, str):
        raise ValueError(
            f"{field_name} must be a string, got {type(raw_value).__name__}"
        )

    # require a compact "<integer><unit>" format with no spaces
    # examples:
    #   "30m" -> valid
    #   "2h"  -> valid
    #   "1d"  -> valid
    #   "30 s", "1hour", "m30" -> invalid
    match = re.fullmatch(r"(\d+)([mhd])", raw_value)
    if match is None:
        raise ValueError(
            f"Invalid {field_name} format. Expected a compact duration like "
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

    min_duration = timedelta(minutes=1)
    max_duration = timedelta(days=1)

    # enforce project-level window bounds
    if duration < min_duration:
        raise ValueError(
            f"{field_name} '{raw_value}' is too short. Minimum allowed value is 1m."
        )

    if duration > max_duration:
        raise ValueError(
            f"{field_name} '{raw_value}' is too long. Maximum allowed value is 1d."
        )

    return duration


class StreamingWindowBuffer:
    """
    Stage streaming windows for one vehicle type into a per-vehicle DuckDB buffer.
    """

    def __init__(
        self,
        vehicle_type: str,
        resolved_runtime_config: dict,
        mimic_service_start_time: datetime,
    ) -> None:
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
        buffer_cfg = resolved_runtime_config["serving"]["streaming"]["buffer"]
        self.window_size = _parse_buffer_duration(
            raw_value=buffer_cfg["window_size"], field_name="buffer.window_size"
        )
        self.retention = _parse_buffer_duration(
            raw_value=buffer_cfg["retention"], field_name="buffer.retention"
        )
        if self.retention < self.window_size:
            raise ValueError(
                "buffer.retention must be greater than or equal to buffer.window_size"
            )

        # vehicle-type-specific settings
        self.vehicle_type = vehicle_type
        self.event_time_column = resolved_runtime_config["serving"][
            "service_event_time_column_by_vehicle_type"
        ][vehicle_type]
        self.event_time_column_utc = f"{self.event_time_column}_utc"
        self.vehicle_data_path = (
            Path(resolved_runtime_config["storage"]["target_base_path"]) / vehicle_type
        )

        # buffer database settings
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

    @staticmethod
    def _to_nyc_local_timestamp_value(dt: datetime) -> datetime:
        """
        Convert an aware UTC datetime into a naive America/New_York datetime for
        filtering against raw TLC event-time columns.

        TLC parquet timestamps are stored without timezone information but are
        interpreted as New York local time. This helper converts service-level UTC
        window boundaries into naive New York local datetimes so the buffer query
        selects the correct source rows.
        """
        nyc_dt = dt.astimezone(ZoneInfo("America/New_York"))
        return nyc_dt.replace(tzinfo=None)

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
        window_start_local = self._to_nyc_local_timestamp_value(window_start)
        window_end_local = self._to_nyc_local_timestamp_value(window_end)

        # define required info for query
        parquet_path = str(self.vehicle_data_path / "year=*" / "month=*" / "*.parquet")

        # query for data within the current window and save to the buffer table in the buffer database
        # note: TLC event timestamps are stored as New York local time without timezone metadata.
        # Filter source rows using that local-time column, and also materialize a UTC-normalized
        # event-time column for downstream replay and watermark comparisons against the UTC-based ServiceClock.
        with duckdb.connect(str(self.buffer_db_path)) as con:
            # create a temp table to materialize the filtered data
            query = textwrap.dedent(
                f"""
                    CREATE OR REPLACE TEMP TABLE temp_window AS
                    SELECT 
                        *,
                        timezone(
                            'UTC',
                            timezone(
                                'America/New_York',
                                {self.event_time_column}
                            )
                        ) AS {self.event_time_column_utc}
                    FROM read_parquet(?)
                    WHERE {self.event_time_column} >= ?
                    AND {self.event_time_column} < ?
                    ORDER BY {self.event_time_column_utc} ASC
                """
            )
            query_params = [parquet_path, window_start_local, window_end_local]
            con.execute(query, query_params)

            # get count from the temp table (avoid scanning parquet files)
            row_count = con.execute("SELECT count(*) FROM temp_window").fetchone()[0]

            # if the window contains no records, still advance the window
            if row_count == 0:
                self.advance_window(window_end)
                # logger.debug(
                #     "Buffer window | vehicle_type=%s | row_count=0 | window_start_inclusive=%s | window_end_exclusive=%s",
                #     self.vehicle_type,
                #     window_start,
                #     window_end,
                # )
                return 0

            logger.info(
                "Buffer window | vehicle_type=%s | row_count=%d | window_start_inclusive=%s | window_end_exclusive=%s",
                self.vehicle_type,
                row_count,
                window_start,
                window_end,
            )

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

    def delete_expired_rows(self, mimic_current_time: datetime) -> int:
        """
        Delete expired rows from the DuckDB buffer table for this vehicle type.

        A row is considered expired when its normalized UTC event-time value is
        older than `mimic_current_time - self.retention`.

        Args:
            mimic_current_time: Current service time in UTC, supplied by the
                streaming service loop.

        Returns:
            Number of rows deleted from the buffer table.
            Returns 0 if the buffer table does not exist or no rows are expired.
        """
        retention_cutoff = mimic_current_time - self.retention

        with duckdb.connect(str(self.buffer_db_path)) as con:
            if not self._table_exists(con, self.buffer_table_name):
                return 0

            query = f"""
                DELETE FROM {self.buffer_table_name}
                WHERE {self.event_time_column_utc} < ?
                RETURNING 1
            """
            query_params = [retention_cutoff]
            deleted_rows = con.execute(query, query_params).fetchall()

        deleted_count = len(deleted_rows)

        if deleted_count > 0:
            logger.info(
                "Delete expired buffer rows | vehicle_type=%s | deleted_count=%d | retention_cutoff_exclusive=%s",
                self.vehicle_type,
                deleted_count,
                retention_cutoff,
            )

        return deleted_count
