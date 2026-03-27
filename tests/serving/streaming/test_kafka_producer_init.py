import types
from datetime import datetime
from pathlib import Path

import pytest

from nyc_taxi_data_service.serving.streaming.kafka_producer import KafkaReplayProducer
from tests.helpers.kafka_producer_test_helpers import (
    CapturingProducer,
    DummyProducer,
    build_producer,
)


def test_init_sets_expected_attributes_for_valid_vehicle_type(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Initialize a replay producer with a valid vehicle type and verify that
    core replay, buffer, topic, and producer-config attributes are resolved
    correctly from runtime config.
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="yellow",
        mimic_service_start_time=mimic_service_start_time,
    )

    # verify basic identity and replay-state initialization
    assert producer.vehicle_type == "yellow"
    assert producer.mimic_service_start_time == mimic_service_start_time
    assert producer._replay_watermark is None

    # verify vehicle-type-specific metadata resolved from config
    assert producer.event_time_column == "tpep_dropoff_datetime"

    # verify buffer source settings were derived correctly for this vehicle type
    assert (
        producer.buffer_db_path
        == Path(
            resolved_runtime_config["serving"]["streaming"]["buffer"]["db_base_path"]
        )
        / "yellow.duckdb"
    )
    assert producer.buffer_table_name == "buffered_data"

    # verify the topic name is derived from prefix + vehicle type
    assert producer.topic_name == "nyc_taxi_yellow"

    # verify YAML-style config keys were normalized into librdkafka keys
    assert producer.producer_config == {
        "bootstrap.servers": "localhost:9092",
        "client.id": "nyc-taxi-data-service",
        "acks": "all",
        "linger.ms": 0,
        "batch.size": 65536,
        "compression.type": "snappy",
    }


def test_init_raises_for_unsupported_vehicle_type(
    kafka_producer_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Raise ValueError when the requested vehicle type is not present in the
    event-time-column mapping.
    """
    # patch the Producer dependency even though this test is expected to fail
    # before producer construction, to keep the test isolated from externals
    monkeypatch.setattr(kafka_producer_module, "Producer", DummyProducer)

    # use an invalid vehicle type to exercise init-time validation logic
    with pytest.raises(ValueError, match="Unsupported vehicle_type"):
        KafkaReplayProducer(
            vehicle_type="taxi_blue",
            resolved_runtime_config=resolved_runtime_config,
            mimic_service_start_time=mimic_service_start_time,
        )


def test_init_constructs_producer_with_normalized_config(
    kafka_producer_module: types.ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Construct the underlying Kafka Producer using the normalized librdkafka
    configuration returned by the producer-config builder.
    """
    # replace the real Producer class with a test double that records inputs
    monkeypatch.setattr(kafka_producer_module, "Producer", CapturingProducer)

    # create the replay producer; during init it should build the config and
    # pass it directly into the Producer constructor
    producer = KafkaReplayProducer(
        vehicle_type="green",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    # verify the exact config handed to the underlying Producer matches the
    # normalized config stored on the KafkaReplayProducer instance
    assert producer._producer.config == producer.producer_config


@pytest.mark.parametrize(
    "client_config",
    [
        {"client_id": "nyc-taxi-data-service"},
        {"bootstrap_servers": ""},
    ],
    ids=[
        "missing-bootstrap-servers",
        "empty-bootstrap-servers",
    ],
)
def test_build_producer_config_raises_for_missing_or_empty_bootstrap_servers(
    client_config: dict,
) -> None:
    """
    Raise ValueError when Kafka client config does not provide a non-empty
    bootstrap_servers value.
    """
    kafka_config = {
        "client": client_config,
        "producer": {
            "acks": "all",
        },
    }

    with pytest.raises(ValueError, match="bootstrap_servers"):
        KafkaReplayProducer._build_producer_config(kafka_config)


def test_build_producer_config_raises_for_unsupported_kafka_key() -> None:
    """
    Raise ValueError when runtime Kafka config contains a key that is not
    explicitly supported by the producer-config key map.
    """
    kafka_config = {
        "client": {
            "bootstrap_servers": "localhost:9092",
            "client_id": "nyc-taxi-data-service",
            # include an unknown key to verify strict config-surface enforcement
            "unknown_setting": "x",
        },
        "producer": {
            "acks": "all",
            "linger_ms": 0,
            "batch_size": 65536,
            "compression_type": "snappy",
        },
    }

    # the helper should reject keys that are not present in the explicit mapping
    with pytest.raises(ValueError, match="Unsupported Kafka producer config key"):
        KafkaReplayProducer._build_producer_config(kafka_config)


def test_build_producer_config_returns_normalized_config() -> None:
    """
    Build producer config from valid runtime config and verify that:

    - client + producer sections are merged
    - snake_case keys are converted to librdkafka dot keys
    - resulting dict is suitable for confluent_kafka.Producer
    """
    kafka_config = {
        "client": {
            "bootstrap_servers": "localhost:9092",
            "client_id": "test-client",
        },
        "producer": {
            "acks": "all",
            "linger_ms": 5,
            "batch_size": 1000,
            "compression_type": "snappy",
        },
    }

    # call the static helper directly
    config = KafkaReplayProducer._build_producer_config(kafka_config)

    # verify all keys are normalized correctly
    assert config == {
        "bootstrap.servers": "localhost:9092",
        "client.id": "test-client",
        "acks": "all",
        "linger.ms": 5,
        "batch.size": 1000,
        "compression.type": "snappy",
    }


def test_build_producer_config_merges_client_and_producer_sections() -> None:
    """
    Verify that values from both the `client` and `producer` sections are
    combined into a single config dict before normalization.
    """
    kafka_config = {
        "client": {
            "bootstrap_servers": "kafka:9092",
        },
        "producer": {
            "acks": "all",
        },
    }

    config = KafkaReplayProducer._build_producer_config(kafka_config)

    # ensure both sections contributed to the final config
    assert config["bootstrap.servers"] == "kafka:9092"
    assert config["acks"] == "all"


def test_build_producer_config_allows_minimal_valid_config() -> None:
    """
    Verify that the helper accepts the smallest valid config:

    - only bootstrap_servers is required
    - producer section may be empty
    """
    kafka_config = {
        "client": {
            "bootstrap_servers": "localhost:9092",
        },
        "producer": {},
    }

    config = KafkaReplayProducer._build_producer_config(kafka_config)

    assert config == {"bootstrap.servers": "localhost:9092"}


def test_build_producer_config_overrides_client_with_producer_values() -> None:
    """
    Verify that if the same key appears in both sections, the `producer`
    section overrides the `client` section due to dict merge order.

    This test documents the intended precedence of config merging.
    """
    kafka_config = {
        "client": {
            "bootstrap_servers": "localhost:9092",
            "acks": "1",
        },
        "producer": {
            "acks": "all",
        },
    }

    config = KafkaReplayProducer._build_producer_config(kafka_config)

    # producer value should override client value
    assert config["acks"] == "all"
