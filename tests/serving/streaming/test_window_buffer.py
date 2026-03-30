from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pytest

from nyc_taxi_data_service.serving.streaming.window_buffer import StreamingWindowBuffer

NY_TZ = ZoneInfo("America/New_York")


@pytest.fixture
def mimic_service_start_time() -> datetime:
    """Provide deterministic NY-aware mimic service start time for streaming buffer tests."""
    return datetime(2025, 1, 15, 9, 0, 0, tzinfo=NY_TZ)


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
                    "retention": "1h",
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


# -------------------------
# core behavior tests
# -------------------------


def test_init_sets_attributes(resolved_runtime_config, mimic_service_start_time):
    """Initialize buffer with correct config-derived attributes and starting state."""
    buffer = StreamingWindowBuffer(
        "yellow", resolved_runtime_config, mimic_service_start_time
    )

    assert buffer._next_window_start == mimic_service_start_time
    assert buffer.event_time_column == "tpep_dropoff_datetime"
    assert buffer.window_size == timedelta(minutes=5)


def test_get_current_window(resolved_runtime_config, mimic_service_start_time):
    """Return current window start and end based on configured window size."""
    buffer = StreamingWindowBuffer(
        "yellow", resolved_runtime_config, mimic_service_start_time
    )

    start, end = buffer.get_current_window()

    assert start == mimic_service_start_time
    assert end == mimic_service_start_time + timedelta(minutes=5)


def test_advance_window(resolved_runtime_config, mimic_service_start_time):
    """Advance internal window start to the provided window end."""
    buffer = StreamingWindowBuffer(
        "yellow", resolved_runtime_config, mimic_service_start_time
    )

    new_end = mimic_service_start_time + timedelta(minutes=5)
    buffer.advance_window(new_end)

    assert buffer._next_window_start == new_end


# -------------------------
# DuckDB boundary conversion
# -------------------------


def test_to_duckdb_timestamp_value():
    """Convert NY-aware datetime to naive local timestamp for DuckDB comparison."""
    dt = datetime(2025, 1, 15, 9, 0, 0, tzinfo=NY_TZ)

    result = StreamingWindowBuffer._to_duckdb_timestamp_value(dt)

    assert result == datetime(2025, 1, 15, 9, 0, 0)
    assert result.tzinfo is None


# -------------------------
# buffering behavior
# -------------------------


def test_buffer_current_window(
    tmp_path, resolved_runtime_config, mimic_service_start_time
):
    """Buffer rows within current window and exclude boundary-overflow rows."""
    raw_base_path = tmp_path / "data" / "raw"
    _write_test_parquet(
        raw_base_path,
        "yellow",
        [
            ("2025-01-15 09:00:00", 1),
            ("2025-01-15 09:02:00", 2),
            ("2025-01-15 09:04:59", 3),
            ("2025-01-15 09:05:00", 4),
        ],
    )

    buffer = StreamingWindowBuffer(
        "yellow", resolved_runtime_config, mimic_service_start_time
    )

    count = buffer.buffer_current_window()

    assert count == 3

    with duckdb.connect(str(buffer.buffer_db_path)) as con:
        rows = con.execute(
            f"SELECT test_value FROM {buffer.buffer_table_name}"
        ).fetchall()

    assert len(rows) == 3


def test_buffer_appends_across_windows(
    tmp_path, resolved_runtime_config, mimic_service_start_time
):
    """Append rows across consecutive windows without overwriting previous data."""
    raw_base_path = tmp_path / "data" / "raw"
    _write_test_parquet(
        raw_base_path,
        "yellow",
        [
            ("2025-01-15 09:00:00", 1),
            ("2025-01-15 09:05:00", 2),
        ],
    )

    buffer = StreamingWindowBuffer(
        "yellow", resolved_runtime_config, mimic_service_start_time
    )

    assert buffer.buffer_current_window() == 1
    assert buffer.buffer_current_window() == 1

    with duckdb.connect(str(buffer.buffer_db_path)) as con:
        count = con.execute(
            f"SELECT COUNT(*) FROM {buffer.buffer_table_name}"
        ).fetchone()[0]

    assert count == 2


# -------------------------
# delete behavior
# -------------------------


def test_delete_expired_rows(
    tmp_path, resolved_runtime_config, mimic_service_start_time
):
    """Delete rows older than retention cutoff while preserving newer rows."""
    buffer = StreamingWindowBuffer(
        "yellow", resolved_runtime_config, mimic_service_start_time
    )

    with duckdb.connect(str(buffer.buffer_db_path)) as con:
        con.execute(
            f"""
            CREATE TABLE {buffer.buffer_table_name} (
                {buffer.event_time_column} TIMESTAMP
            )
            """
        )
        con.execute(
            f"""
            INSERT INTO {buffer.buffer_table_name}
            VALUES
                (TIMESTAMP '2025-01-15 08:00:00'),
                (TIMESTAMP '2025-01-15 09:00:00')
            """
        )

    now = datetime(2025, 1, 15, 10, 0, 0, tzinfo=NY_TZ)

    deleted = buffer.delete_expired_rows(now)

    assert deleted == 1

    with duckdb.connect(str(buffer.buffer_db_path)) as con:
        remaining = con.execute(
            f"SELECT COUNT(*) FROM {buffer.buffer_table_name}"
        ).fetchone()[0]

    assert remaining == 1
