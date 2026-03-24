import types
from datetime import datetime

import pytest

from nyc_taxi_data_service.serving.streaming.kafka_producer import KafkaReplayProducer
from tests.helpers.kafka_producer_test_helpers import (
    CapturingLogger,
    CapturingProducer,
    DummyMessage,
    DummyProducer,
)


def test_close_calls_flush_with_given_timeout(
    kafka_producer_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Call the underlying producer's flush() with the timeout provided to close().

    This verifies that KafkaReplayProducer.close() forwards the caller's shutdown
    timeout directly to the wrapped Producer instance.
    """

    # replace the real Producer dependency with a test double so the test can
    # observe how close() interacts with the wrapped producer object
    monkeypatch.setattr(kafka_producer_module, "Producer", CapturingProducer)

    # construct the object under test; __init__ will create and store a
    # CapturingProducer instance on producer._producer
    producer = KafkaReplayProducer(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    # call close() with a non-default timeout so the test can clearly verify
    # that this specific value is passed through unchanged
    producer.close(timeout=3.5)

    # verify the wrapped producer is the capturing double
    assert isinstance(producer._producer, CapturingProducer)

    # confirm that close() forwarded the timeout to flush()
    assert producer._producer.flush_calls == [3.5]


def test_close_returns_flush_result(
    kafka_producer_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Return the exact value produced by the underlying producer's flush() call.

    This verifies that KafkaReplayProducer.close() preserves the producer API's
    return value, which represents the number of messages still queued after
    the flush timeout expires.
    """
    # patch the module-level Producer symbol used by KafkaReplayProducer so the
    # test remains isolated from the real Kafka client
    monkeypatch.setattr(
        kafka_producer_module,
        "Producer",
        lambda config: CapturingProducer(config=config, flush_result=2),
    )

    # create the replay producer using the patched Producer dependency
    producer = KafkaReplayProducer(
        vehicle_type="green",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    # close() should return exactly what the wrapped producer's flush() returns
    result = producer.close(timeout=10.0)

    # verify that close() preserves both the flush() return value and the
    # forwarded timeout passed to the wrapped producer
    assert result == 2
    assert producer._producer.flush_calls == [10.0]


def test_on_delivery_logs_warning_when_error_is_present(
    kafka_producer_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Log a warning containing topic, partition, offset, and error information
    when the Kafka client reports a delivery failure.
    """

    # replace the real Kafka Producer dependency with a no-op test double
    monkeypatch.setattr(kafka_producer_module, "Producer", DummyProducer)

    # patch the module-level logger with a capturing test double so the test
    # can verify that the failure branch emits exactly one warning message
    logger = CapturingLogger()
    monkeypatch.setattr(kafka_producer_module, "logger", logger)

    # construct the object under test using the patched Producer and logger
    producer = KafkaReplayProducer(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    # invoke the callback directly to verify the failure-handling branch
    producer._on_delivery(
        "broker unavailable",
        DummyMessage("nyc_taxi_yellow", 2, 15),
    )

    # verify the log message includes the key delivery context fields plus the error
    assert logger.warning_messages == [
        "Delivery failed for topic=nyc_taxi_yellow "
        "partition=2 "
        "offset=15 "
        "error=broker unavailable"
    ]


def test_on_delivery_does_nothing_when_error_is_none(
    kafka_producer_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Do nothing when the Kafka client reports successful delivery.

    The current implementation only logs failures, so the success branch should
    not emit any warning log.
    """

    # replace the real Kafka Producer dependency with a no-op test double
    monkeypatch.setattr(kafka_producer_module, "Producer", DummyProducer)

    # patch the module-level logger with a capturing test double so the test
    # can verify that the success branch emits no warning messages
    logger = CapturingLogger()
    monkeypatch.setattr(kafka_producer_module, "logger", logger)

    # construct the object under test using the patched Producer and logger
    producer = KafkaReplayProducer(
        vehicle_type="green",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    # invoke the success branch directly; current behavior should be a no-op
    producer._on_delivery(None, DummyMessage("nyc_taxi_green", 1, 42))

    # verify that no warning log was emitted
    assert logger.warning_messages == []
