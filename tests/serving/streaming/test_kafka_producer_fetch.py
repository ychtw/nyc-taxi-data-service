from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from tests.helpers.kafka_producer_test_helpers import (
    build_producer,
    create_buffer_table_and_insert_rows,
)

NY_TZ = ZoneInfo("America/New_York")


@pytest.fixture
def ny_local_times() -> list[datetime]:
    """Provide naive New York-local event-time values for buffer-table test rows."""
    return [
        datetime(2025, 3, 1, 0, 0, 0),
        datetime(2025, 3, 1, 0, 1, 0),
        datetime(2025, 3, 1, 0, 2, 0),
        datetime(2025, 3, 1, 0, 3, 0),
        datetime(2025, 3, 1, 0, 4, 0),
        datetime(2025, 3, 1, 0, 5, 0),
        datetime(2025, 3, 1, 0, 6, 0),
        datetime(2025, 3, 1, 0, 7, 0),
        datetime(2025, 3, 1, 0, 8, 0),
        datetime(2025, 3, 1, 0, 9, 0),
        datetime(2025, 3, 1, 0, 10, 0),
        datetime(2025, 3, 1, 0, 11, 0),
    ]


def test_fetch_eligible_rows_includes_start_time_on_first_fetch(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
    ny_local_times: list[datetime],
) -> None:
    """
    Include rows whose event time equals mimic_service_start_time on the first fetch.

    When _replay_watermark is None, the initial replay window should be
    [mimic_service_start_time, mimic_current_time]. This test verifies that the
    lower bound is inclusive during the first fetch.
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="yellow",
        mimic_service_start_time=mimic_service_start_time,
    )

    create_buffer_table_and_insert_rows(
        producer=producer,
        create_table_sql=f"""
            CREATE TABLE {producer.buffer_table_name} (
                vendor_id INTEGER,
                {producer.event_time_column} TIMESTAMP
            )
            """,
        insert_sql=f"""
            INSERT INTO {producer.buffer_table_name}
            VALUES
                (1, ?),  -- exactly at mimic_service_start_time: should be included
                (2, ?),  -- inside the window: should be included
                (3, ?)   -- after mimic_current_time: should be excluded
            """,
        insert_sql_params=[
            ny_local_times[0],
            ny_local_times[5],
            ny_local_times[11],
        ],
    )

    # keep the initial state explicit so the test clearly targets the
    # first-fetch branch
    assert producer._replay_watermark is None

    # query rows up to 00:10 so the row at 00:11 stays outside the replay window
    rows = producer._fetch_eligible_rows(
        mimic_current_time=datetime(2025, 3, 1, 0, 10, 0, tzinfo=NY_TZ)
    )

    # verify that the first-fetch branch includes the row exactly equal to the
    # service start time and excludes the row above the upper bound
    assert [row["vendor_id"] for row in rows] == [1, 2]


def test_fetch_eligible_rows_excludes_replay_watermark_on_later_fetch(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
    ny_local_times: list[datetime],
) -> None:
    """
    Exclude rows whose event time equals the current replay watermark on later fetches.

    After the first successful publish, the replay window should become
    (replay_watermark, mimic_current_time]. This test verifies that the lower
    bound is exclusive once replay progress exists.
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="yellow",
        mimic_service_start_time=mimic_service_start_time,
    )

    create_buffer_table_and_insert_rows(
        producer=producer,
        create_table_sql=f"""
            CREATE TABLE {producer.buffer_table_name} (
                vendor_id INTEGER,
                {producer.event_time_column} TIMESTAMP
            )
            """,
        insert_sql=f"""
            INSERT INTO {producer.buffer_table_name}
            VALUES
                (1, ?),  -- exactly at replay watermark: should be excluded
                (2, ?),  -- above watermark and within upper bound: included
                (3, ?)   -- exactly at upper bound: included
            """,
        insert_sql_params=[
            ny_local_times[5],
            ny_local_times[6],
            ny_local_times[10],
        ],
    )

    # switch the producer into the later-fetch branch by setting an existing
    # replay watermark
    producer._replay_watermark = datetime(2025, 3, 1, 0, 5, 0, tzinfo=NY_TZ)

    # query a later replay window where the lower bound should now be exclusive
    rows = producer._fetch_eligible_rows(
        mimic_current_time=datetime(2025, 3, 1, 0, 10, 0, tzinfo=NY_TZ)
    )

    # verify that the replay watermark itself is excluded while later rows
    # inside the window are returned
    assert [row["vendor_id"] for row in rows] == [2, 3]


def test_fetch_eligible_rows_excludes_null_event_times(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
    ny_local_times: list[datetime],
) -> None:
    """
    Exclude rows whose event time column is NULL.

    The query explicitly filters with `event_time_column IS NOT NULL`, so rows
    lacking an event time should never be considered replay-eligible.
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="fhv",
        mimic_service_start_time=mimic_service_start_time,
    )

    create_buffer_table_and_insert_rows(
        producer=producer,
        create_table_sql=f"""
            CREATE TABLE {producer.buffer_table_name} (
                dispatching_base_num VARCHAR,
                {producer.event_time_column} TIMESTAMP
            )
            """,
        insert_sql=f"""
            INSERT INTO {producer.buffer_table_name}
            VALUES
                ('B1', NULL),  -- should be excluded by IS NOT NULL
                ('B2', ?)      -- should be included
            """,
        insert_sql_params=[
            ny_local_times[3],
        ],
    )

    rows = producer._fetch_eligible_rows(
        mimic_current_time=datetime(2025, 3, 1, 0, 10, 0, tzinfo=NY_TZ)
    )

    # verify that only the non-null event-time row is returned
    assert [row["dispatching_base_num"] for row in rows] == ["B2"]


def test_fetch_eligible_rows_returns_rows_in_ascending_event_time_order(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
    ny_local_times: list[datetime],
) -> None:
    """
    Return eligible rows sorted by ascending event time.

    The query orders rows by the configured event-time column so later publish
    steps can replay records deterministically within a cycle.
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="yellow",
        mimic_service_start_time=mimic_service_start_time,
    )

    create_buffer_table_and_insert_rows(
        producer=producer,
        create_table_sql=f"""
            CREATE TABLE {producer.buffer_table_name} (
                trip_id INTEGER,
                {producer.event_time_column} TIMESTAMP
            )
            """,
        insert_sql=f"""
            INSERT INTO {producer.buffer_table_name}
            VALUES
                (10, ?),
                (20, ?),
                (30, ?)
            """,
        insert_sql_params=[
            ny_local_times[8],
            ny_local_times[2],
            ny_local_times[5],
        ],
    )

    rows = producer._fetch_eligible_rows(
        mimic_current_time=datetime(2025, 3, 1, 0, 10, 0, tzinfo=NY_TZ)
    )

    # verify that query results are sorted by event time rather than insertion order
    assert [row["trip_id"] for row in rows] == [20, 30, 10]


def test_fetch_eligible_rows_returns_empty_list_when_no_rows_are_eligible(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
    ny_local_times: list[datetime],
) -> None:
    """
    Return an empty list when no buffered rows fall within the current replay window.
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="yellow",
        mimic_service_start_time=mimic_service_start_time,
    )

    create_buffer_table_and_insert_rows(
        producer=producer,
        create_table_sql=f"""
            CREATE TABLE {producer.buffer_table_name} (
                vendor_id INTEGER,
                {producer.event_time_column} TIMESTAMP
            )
            """,
        insert_sql=f"""
            INSERT INTO {producer.buffer_table_name}
            VALUES
                (1, ?),
                (2, ?)
            """,
        insert_sql_params=[
            ny_local_times[8],
            ny_local_times[10],
        ],
    )

    rows = producer._fetch_eligible_rows(
        mimic_current_time=datetime(2025, 3, 1, 0, 5, 0, tzinfo=NY_TZ)
    )

    # verify the query returns no rows when every buffered record is above the upper bound
    assert rows == []


def test_fetch_eligible_rows_returns_empty_list_when_buffer_table_does_not_exist(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """Return an empty list when the buffer table has not been created yet."""
    import duckdb

    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="green",
        mimic_service_start_time=mimic_service_start_time,
    )

    producer.buffer_db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(producer.buffer_db_path):
        pass

    rows = producer._fetch_eligible_rows(
        mimic_current_time=datetime(2025, 3, 1, 0, 10, 0, tzinfo=NY_TZ)
    )

    assert rows == []
