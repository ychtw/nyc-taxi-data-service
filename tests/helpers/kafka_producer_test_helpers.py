from datetime import datetime

import pytest

from nyc_taxi_data_service.serving.streaming.kafka_producer import KafkaReplayProducer


class DummyProducer:
    def __init__(self, config: dict) -> None:
        # store the config passed by KafkaReplayProducer so tests can inspect it
        self.config = config

    def flush(self, timeout: float | None = None) -> int:
        # mimic the real Producer.flush() return shape without doing any network work
        return 0


class CapturingProducer:
    def __init__(
        self,
        config: dict,
        flush_result: int = 0,
    ) -> None:
        # keep the constructor signature compatible with the real Producer,
        # even though this test only cares about flush() behavior
        self.config = config
        self.flush_result = flush_result
        self.flush_calls: list[float | None] = []

    def flush(self, timeout: float | None = None) -> int:
        # record the timeout value passed by KafkaReplayProducer.close()
        # so the test can verify argument forwarding
        self.flush_calls.append(timeout)
        return self.flush_result


class DummyMessage:
    def __init__(self, topic: str, partition: int, offset: int) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class CapturingLogger:
    def __init__(self) -> None:
        self.warning_messages: list[str] = []

    def warning(self, message: str, *args) -> None:
        # mimic stdlib logging behavior for parameterized warning messages
        if args:
            message = message % args
        self.warning_messages.append(message)


def build_producer(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    vehicle_type: str,
    mimic_service_start_time: datetime,
) -> KafkaReplayProducer:
    """
    Build a KafkaReplayProducer for tests with the real Kafka Producer
    dependency replaced by DummyProducer.
    """

    import nyc_taxi_data_service.serving.streaming.kafka_producer as module

    # patch the Kafka Producer dependency so object construction stays isolated
    # from the real confluent_kafka client
    monkeypatch.setattr(module, "Producer", DummyProducer)

    producer = KafkaReplayProducer(
        vehicle_type=vehicle_type,
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    return producer


def create_buffer_table_and_insert_rows(
    producer: KafkaReplayProducer,
    create_table_sql: str,
    insert_sql: str,
    insert_sql_params: list,
) -> None:
    """
    Create the temporary DuckDB buffer table for a fetch test and insert the
    rows needed for that test case.
    """
    import duckdb

    # ensure parent directory exists before DuckDB tries to create the file
    producer.buffer_db_path.parent.mkdir(parents=True, exist_ok=True)

    # open the vehicle-specific DuckDB buffer file, create the test table,
    # and insert the test rows used by the current scenario
    with duckdb.connect(producer.buffer_db_path) as con:
        con.execute(create_table_sql)
        con.execute(insert_sql, insert_sql_params)


# def nyc_local_to_utc_naive(dt_local: datetime) -> datetime:
#     """Convert naive America/New_York datetime to naive UTC datetime."""
#     aware_local = dt_local.replace(tzinfo=ZoneInfo("America/New_York"))
#     aware_utc = aware_local.astimezone(timezone.utc)
#     return aware_utc.replace(tzinfo=None)
