from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from nyc_taxi_data_service.serving.streaming.window_buffer import StreamingWindowBuffer


@pytest.fixture
def mimic_service_start_time() -> datetime:
    """Provide a deterministic mimic service start time for streaming buffer tests."""
    fixed_mimic_start = datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
    return fixed_mimic_start


@pytest.fixture
def resolved_runtime_config(tmp_path: Path) -> dict:
    """Build a minimal resolved runtime config for streaming buffer tests."""
    return {
        "storage": {
            "target_storage_type": "local",
            "target_base_path": str(tmp_path / "data" / "raw"),
        },
        "serving": {
            "service_event_time_column_by_vehicle_type": {
                "yellow": "tpep_dropoff_datetime",
                "green": "lpep_dropoff_datetime",
                "fhv": "dropOff_datetime",
                "fhvhv": "dropoff_datetime",
            },
            "streaming": {
                "buffer": {
                    "window_size": "5m",
                    "db_base_path": str(tmp_path / "data" / "stream_buffer"),
                    "db_table_name": "buffered_data",
                },
            },
        },
    }


def _write_test_parquet(
    base_path: Path,
    vehicle_type: str,
    rows: list[tuple[str, int]],
    event_time_column: str = "tpep_dropoff_datetime",
) -> None:
    """
    Write test parquet data under the expected year=/month= partition layout.

    Args:
        base_path: Root target_base_path from resolved runtime config.
        vehicle_type: Vehicle type folder name.
        rows: List of (timestamp_string, value) tuples.
        event_time_column: Timestamp column name for the vehicle type.
    """
    partition_dir = base_path / vehicle_type / "year=2025" / "month=01"
    partition_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = partition_dir / "part-000.parquet"

    values_sql = ",\n".join(f"(TIMESTAMP '{ts}', {value})" for ts, value in rows)

    with duckdb.connect() as con:
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM (
                    VALUES
                    {values_sql}
                ) AS t({event_time_column}, test_value)
            )
            TO '{parquet_path}' (FORMAT PARQUET)
            """
        )


def test_init_sets_config_derived_attributes_and_creates_fresh_buffer_db(
    resolved_runtime_config: dict, mimic_service_start_time: datetime
) -> None:
    """Initialize config-derived buffer attributes and create a fresh DuckDB file."""
    raw_base_path = resolved_runtime_config["storage"]["target_base_path"]
    buffer_cfg = resolved_runtime_config["serving"]["streaming"]["buffer"]

    buffer = StreamingWindowBuffer(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    assert buffer.vehicle_type == "yellow"
    assert buffer.window_size == timedelta(minutes=5)
    assert buffer.event_time_column == "tpep_dropoff_datetime"
    assert buffer.vehicle_data_path == Path(raw_base_path) / "yellow"
    assert buffer.buffer_table_name == buffer_cfg["db_table_name"]
    assert buffer.buffer_db_path == Path(buffer_cfg["db_base_path"]) / "yellow.duckdb"
    assert buffer._next_window_start == mimic_service_start_time
    assert buffer.buffer_db_path.exists()


def test_init_recreates_buffer_db_from_clean_state(
    resolved_runtime_config: dict, mimic_service_start_time: datetime
) -> None:
    """Recreate the per-vehicle buffer DB from a clean state on initialization."""
    buffer_db_path = (
        Path(resolved_runtime_config["serving"]["streaming"]["buffer"]["db_base_path"])
        / "yellow.duckdb"
    )
    buffer_db_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(buffer_db_path)) as con:
        con.execute("CREATE TABLE stale_table (id INTEGER)")
        con.execute("INSERT INTO stale_table VALUES (1)")

    buffer = StreamingWindowBuffer(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    with duckdb.connect(str(buffer.buffer_db_path)) as con:
        assert StreamingWindowBuffer._table_exists(con, "stale_table") is False


def test_get_current_window_returns_next_start_and_configured_end(
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """Return the current query window based on next start and window size."""

    buffer = StreamingWindowBuffer(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    window_start, window_end = buffer.get_current_window()

    assert window_start == datetime(2025, 1, 15, 9, 0, 0, tzinfo=timezone.utc)
    assert window_end == datetime(2025, 1, 15, 9, 5, 0, tzinfo=timezone.utc)


def test_advance_window_updates_next_window_start(
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """Advance the next query start time to the provided window end."""

    buffer = StreamingWindowBuffer(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    new_window_end = datetime(2025, 1, 15, 9, 5, 0, tzinfo=timezone.utc)
    buffer.advance_window(new_window_end)

    assert buffer._next_window_start == new_window_end


def test_table_exists_returns_false_then_true(tmp_path: Path) -> None:
    """Detect whether a table exists in the current DuckDB connection."""
    db_path = tmp_path / "table_exists.duckdb"

    with duckdb.connect(str(db_path)) as con:
        assert StreamingWindowBuffer._table_exists(con, "buffered_data") is False

        con.execute("CREATE TABLE buffered_data (id INTEGER)")
        assert StreamingWindowBuffer._table_exists(con, "buffered_data") is True


def test_buffer_current_window_creates_buffer_table_and_returns_row_count(
    tmp_path: Path,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """Create the buffer table on first use and buffer matching rows."""

    raw_base_path = tmp_path / "data" / "raw"
    _write_test_parquet(
        base_path=raw_base_path,
        vehicle_type="yellow",
        rows=[
            ("2025-01-15 09:00:00", 1),
            ("2025-01-15 09:02:00", 2),
            ("2025-01-15 09:04:59", 3),
            ("2025-01-15 09:05:00", 4),  # next window; should be excluded
        ],
    )

    buffer = StreamingWindowBuffer(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    row_count = buffer.buffer_current_window()

    assert row_count == 3
    assert buffer._next_window_start == datetime(
        2025, 1, 15, 9, 5, 0, tzinfo=timezone.utc
    )

    with duckdb.connect(str(buffer.buffer_db_path)) as con:
        assert buffer._table_exists(con, buffer.buffer_table_name) is True
        buffered_count = con.execute(
            f"SELECT COUNT(*) FROM {buffer.buffer_table_name}"
        ).fetchone()[0]

    assert buffered_count == 3


def test_buffer_current_window_returns_zero_and_advances_for_empty_window(
    tmp_path: Path,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """Advance past an empty window and return zero buffered rows."""

    raw_base_path = tmp_path / "data" / "raw"
    _write_test_parquet(
        base_path=raw_base_path,
        vehicle_type="yellow",
        rows=[
            ("2025-01-15 09:06:00", 10),
            ("2025-01-15 09:07:00", 20),
        ],
    )

    buffer = StreamingWindowBuffer(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    row_count = buffer.buffer_current_window()

    assert row_count == 0
    assert buffer._next_window_start == datetime(
        2025, 1, 15, 9, 5, 0, tzinfo=timezone.utc
    )

    with duckdb.connect(str(buffer.buffer_db_path)) as con:
        assert buffer._table_exists(con, buffer.buffer_table_name) is False


def test_buffer_current_window_appends_rows_on_later_calls(
    tmp_path: Path,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """Append later-window rows into an existing buffer table."""

    raw_base_path = tmp_path / "data" / "raw"
    _write_test_parquet(
        base_path=raw_base_path,
        vehicle_type="yellow",
        rows=[
            ("2025-01-15 09:00:00", 1),
            ("2025-01-15 09:01:00", 2),
            ("2025-01-15 09:05:00", 3),
            ("2025-01-15 09:06:00", 4),
            ("2025-01-15 09:10:00", 5),  # third window; excluded from first two calls
        ],
    )

    buffer = StreamingWindowBuffer(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    first_count = buffer.buffer_current_window()
    second_count = buffer.buffer_current_window()

    assert first_count == 2
    assert second_count == 2
    assert buffer._next_window_start == datetime(
        2025, 1, 15, 9, 10, 0, tzinfo=timezone.utc
    )

    with duckdb.connect(str(buffer.buffer_db_path)) as con:
        buffered_count = con.execute(
            f"SELECT COUNT(*) FROM {buffer.buffer_table_name}"
        ).fetchone()[0]

    assert buffered_count == 4


def test_init_uses_vehicle_specific_event_time_column(
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """Initialize vehicle-specific event-time column and buffer DB path from config."""
    buffer = StreamingWindowBuffer(
        vehicle_type="green",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    assert buffer.event_time_column == "lpep_dropoff_datetime"
    assert buffer.buffer_db_path == (
        Path(resolved_runtime_config["serving"]["streaming"]["buffer"]["db_base_path"])
        / "green.duckdb"
    )


def test_buffer_current_window_buffers_rows_in_event_time_order(
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """Buffer current-window rows so they can be read back in ascending event-time order."""
    raw_base_path = Path(resolved_runtime_config["storage"]["target_base_path"])

    _write_test_parquet(
        base_path=raw_base_path,
        vehicle_type="yellow",
        rows=[
            ("2025-01-15 09:04:00", 40),
            ("2025-01-15 09:00:00", 10),
            ("2025-01-15 09:02:00", 20),
            ("2025-01-15 09:01:00", 15),
            ("2025-01-15 09:05:00", 50),  # next window; should be excluded
        ],
    )

    buffer = StreamingWindowBuffer(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    row_count = buffer.buffer_current_window()

    assert row_count == 4

    with duckdb.connect(str(buffer.buffer_db_path)) as con:
        rows = con.execute(
            f"""
            SELECT {buffer.event_time_column}, test_value
            FROM {buffer.buffer_table_name}
            ORDER BY {buffer.event_time_column} ASC
            """
        ).fetchall()

    event_times = [row[0] for row in rows]
    test_values = [row[1] for row in rows]

    assert event_times == sorted(event_times)
    assert test_values == [10, 15, 20, 40]
