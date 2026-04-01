from __future__ import annotations

import pytest
from confluent_kafka.cimpl import KafkaException

from nyc_taxi_data_service.serving.streaming import kafka_admin
from tests.helpers.kafka_admin_test_helpers import (
    DummyAdminClient,
    DummyFuture,
    build_minimal_resolved_runtime_config,
    patch_admin_client,
)


def test_build_admin_client_config_maps_supported_keys() -> None:
    """Map supported runtime config keys to confluent-kafka admin client keys."""
    kafka_config = {
        "client": {
            "bootstrap_servers": "localhost:9092",
            "client_id": "nyc-taxi-data-service",
        }
    }

    result = kafka_admin.build_admin_client_config(kafka_config)

    assert result == {
        "bootstrap.servers": "localhost:9092",
        "client.id": "nyc-taxi-data-service",
    }


def test_build_admin_client_config_allows_only_bootstrap_servers() -> None:
    """Allow a minimal client config when only bootstrap_servers is provided."""
    kafka_config = {
        "client": {
            "bootstrap_servers": "localhost:9092",
        }
    }

    result = kafka_admin.build_admin_client_config(kafka_config)

    assert result == {
        "bootstrap.servers": "localhost:9092",
    }


@pytest.mark.parametrize("bootstrap_servers", ["", None])
def test_build_admin_client_config_raises_when_bootstrap_servers_is_empty(
    bootstrap_servers: str | None,
) -> None:
    """Raise when bootstrap_servers is missing or empty."""
    kafka_config = {
        "client": {
            "bootstrap_servers": bootstrap_servers,
            "client_id": "nyc-taxi-data-service",
        }
    }

    with pytest.raises(
        ValueError,
        match="Kafka client config must include a non-empty 'bootstrap_servers'",
    ):
        kafka_admin.build_admin_client_config(kafka_config)


def test_build_admin_client_config_raises_for_unsupported_client_key() -> None:
    """Reject unsupported Kafka admin client config keys."""
    kafka_config = {
        "client": {
            "bootstrap_servers": "localhost:9092",
            "client_id": "nyc-taxi-data-service",
            "security_protocol": "PLAINTEXT",
        }
    }

    with pytest.raises(
        ValueError,
        match="Unsupported Kafka admin client config key: security_protocol",
    ):
        kafka_admin.build_admin_client_config(kafka_config)


def test_ensure_topic_exists_raises_when_topic_name_is_empty() -> None:
    """Reject an empty topic name before any Kafka admin work is attempted."""
    resolved_runtime_config = build_minimal_resolved_runtime_config()

    with pytest.raises(ValueError, match="topic_name must be a non-empty string"):
        kafka_admin.ensure_topic_exists(
            topic_name="",
            resolved_runtime_config=resolved_runtime_config,
        )


def test_ensure_topic_exists_returns_early_when_topic_already_exists(
    monkeypatch: pytest.MonkeyPatch,
    dummy_admin_config: dict,
) -> None:
    """
    Skip topic creation when metadata already contains the requested topic.

    This is the fast path during normal service startup after topics have
    already been provisioned.
    """
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    topic_name = "nyc_taxi_yellow"

    dummy_admin_client = DummyAdminClient(
        config=dummy_admin_config,
        existing_topics={topic_name: object()},
    )

    patch_admin_client(
        monkeypatch=monkeypatch,
        kafka_admin_module=kafka_admin,
        dummy_admin_client=dummy_admin_client,
        dummy_admin_config=dummy_admin_config,
    )

    kafka_admin.ensure_topic_exists(
        topic_name=topic_name,
        resolved_runtime_config=resolved_runtime_config,
    )

    # metadata was checked once with the hard-coded timeout
    assert dummy_admin_client.list_topics_called_with == {"timeout": 10}

    # no create request should be sent if the topic already exists
    assert dummy_admin_client.create_topics_called_with is None


def test_ensure_topic_exists_creates_topic_when_missing(
    monkeypatch: pytest.MonkeyPatch,
    dummy_admin_config: dict,
) -> None:
    """Create the topic with values from runtime config when metadata shows it is missing."""
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    topic_name = "nyc_taxi_yellow"

    dummy_future = DummyFuture()
    dummy_admin_client = DummyAdminClient(
        config=dummy_admin_config,
        existing_topics={},
        create_topics_result={topic_name: dummy_future},
    )

    patch_admin_client(
        monkeypatch=monkeypatch,
        kafka_admin_module=kafka_admin,
        dummy_admin_client=dummy_admin_client,
        dummy_admin_config=dummy_admin_config,
        patch_new_topic=True,
    )

    kafka_admin.ensure_topic_exists(
        topic_name=topic_name,
        resolved_runtime_config=resolved_runtime_config,
    )

    assert dummy_admin_client.list_topics_called_with == {"timeout": 10}
    assert dummy_admin_client.create_topics_called_with is not None
    assert len(dummy_admin_client.create_topics_called_with) == 1

    created_topic = dummy_admin_client.create_topics_called_with[0]
    assert created_topic.topic == topic_name
    assert created_topic.num_partitions == 3
    assert created_topic.replication_factor == 1
    assert created_topic.config == {"retention.ms": "86400000"}

    # waiting on the returned future is important so startup fails fast on real errors
    assert dummy_future.result_called is True


@pytest.mark.parametrize(
    ("missing_key", "expected_message"),
    [
        ("num_partitions", "Kafka topic config must include 'num_partitions'"),
        ("replication_factor", "Kafka topic config must include 'replication_factor'"),
        ("retention_ms", "Kafka topic config must include 'retention_ms'"),
    ],
    ids=[
        "missing_num_partitions",
        "missing_replication_factor",
        "missing_retention_ms",
    ],
)
def test_ensure_topic_exists_raises_when_required_topic_setting_is_missing(
    missing_key: str,
    expected_message: str,
    monkeypatch: pytest.MonkeyPatch,
    dummy_admin_config: dict,
) -> None:
    """
    Fail fast when a required topic setting is missing from runtime config.

    The topic must be absent in metadata so the function reaches the creation
    branch where these settings are validated.
    """
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    del resolved_runtime_config["serving"]["streaming"]["kafka"]["topic"][missing_key]

    dummy_admin_client = DummyAdminClient(
        config=dummy_admin_config,
        existing_topics={},
    )

    patch_admin_client(
        monkeypatch=monkeypatch,
        kafka_admin_module=kafka_admin,
        dummy_admin_client=dummy_admin_client,
        dummy_admin_config=dummy_admin_config,
    )

    with pytest.raises(ValueError, match=expected_message):
        kafka_admin.ensure_topic_exists(
            topic_name="nyc_taxi_yellow",
            resolved_runtime_config=resolved_runtime_config,
        )

    # we should have checked metadata before discovering the missing create-time config
    assert dummy_admin_client.list_topics_called_with == {"timeout": 10}
    assert dummy_admin_client.create_topics_called_with is None


def test_ensure_topic_exists_reraises_when_list_topics_fails(
    monkeypatch: pytest.MonkeyPatch,
    dummy_admin_config: dict,
) -> None:
    """Propagate Kafka metadata lookup failures so service startup stops."""
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    kafka_error = KafkaException("metadata lookup failed")

    dummy_admin_client = DummyAdminClient(
        config=dummy_admin_config,
        list_topics_error=kafka_error,
    )

    patch_admin_client(
        monkeypatch=monkeypatch,
        kafka_admin_module=kafka_admin,
        dummy_admin_client=dummy_admin_client,
        dummy_admin_config=dummy_admin_config,
    )

    with pytest.raises(KafkaException):
        kafka_admin.ensure_topic_exists(
            topic_name="nyc_taxi_yellow",
            resolved_runtime_config=resolved_runtime_config,
        )

    assert dummy_admin_client.create_topics_called_with is None


def test_ensure_topic_exists_treats_topic_already_exists_during_create_as_success(
    monkeypatch: pytest.MonkeyPatch,
    dummy_admin_config: dict,
) -> None:
    """
    Treat a concurrent 'topic already exists' creation race as success.

    This covers the case where metadata says the topic is missing, but another
    process creates it before this process's create request completes.
    """
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    topic_name = "nyc_taxi_yellow"

    dummy_future = DummyFuture(error=KafkaException("Topic already exists"))
    dummy_admin_client = DummyAdminClient(
        config=dummy_admin_config,
        existing_topics={},
        create_topics_result={topic_name: dummy_future},
    )

    patch_admin_client(
        monkeypatch=monkeypatch,
        kafka_admin_module=kafka_admin,
        dummy_admin_client=dummy_admin_client,
        dummy_admin_config=dummy_admin_config,
        patch_new_topic=True,
    )

    kafka_admin.ensure_topic_exists(
        topic_name=topic_name,
        resolved_runtime_config=resolved_runtime_config,
    )

    assert dummy_future.result_called is True


def test_ensure_topic_exists_reraises_non_race_create_error(
    monkeypatch: pytest.MonkeyPatch,
    dummy_admin_config: dict,
) -> None:
    """Re-raise topic creation errors that are not the benign already-exists race."""
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    topic_name = "nyc_taxi_yellow"

    dummy_future = DummyFuture(error=KafkaException("broker not available"))
    dummy_admin_client = DummyAdminClient(
        config=dummy_admin_config,
        existing_topics={},
        create_topics_result={topic_name: dummy_future},
    )

    patch_admin_client(
        monkeypatch=monkeypatch,
        kafka_admin_module=kafka_admin,
        dummy_admin_client=dummy_admin_client,
        dummy_admin_config=dummy_admin_config,
        patch_new_topic=True,
    )

    with pytest.raises(KafkaException):
        kafka_admin.ensure_topic_exists(
            topic_name=topic_name,
            resolved_runtime_config=resolved_runtime_config,
        )

    assert dummy_future.result_called is True


def test_ensure_streaming_topics_exist_raises_when_vehicle_types_is_empty() -> None:
    """Reject an empty vehicle type list before attempting any topic orchestration."""
    resolved_runtime_config = build_minimal_resolved_runtime_config()

    with pytest.raises(
        ValueError,
        match=(
            "vehicle_types must contain at least one vehicle type when ensuring "
            "streaming Kafka topics."
        ),
    ):
        kafka_admin.ensure_streaming_topics_exist(
            vehicle_types=[],
            resolved_runtime_config=resolved_runtime_config,
        )


@pytest.mark.parametrize(
    "topic_prefix",
    ["", None],
    ids=["empty_prefix", "missing_prefix"],
)
def test_ensure_streaming_topics_exist_raises_when_topic_prefix_is_empty(
    topic_prefix: str | None,
) -> None:
    """Reject a missing Kafka topic prefix because topic names cannot be derived."""
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    resolved_runtime_config["serving"]["streaming"]["kafka"]["topic"]["prefix"] = (
        topic_prefix
    )

    with pytest.raises(
        ValueError,
        match="serving.streaming.kafka.topic.prefix must be set in runtime config.",
    ):
        kafka_admin.ensure_streaming_topics_exist(
            vehicle_types=["yellow"],
            resolved_runtime_config=resolved_runtime_config,
        )


def test_ensure_streaming_topics_exist_delegates_one_topic_per_vehicle_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Derive one topic name per vehicle type and delegate existence checks in order.

    This test keeps the scope at the orchestration layer by patching
    `ensure_topic_exists(...)` directly instead of exercising Kafka admin details.
    """
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    vehicle_types = ["yellow", "green", "fhvhv"]
    calls: list[dict] = []

    def dummy_ensure_topic_exists(
        topic_name: str, resolved_runtime_config: dict
    ) -> None:
        """Record delegated ensure-topic calls for assertion."""
        calls.append(
            {
                "topic_name": topic_name,
                "resolved_runtime_config": resolved_runtime_config,
            }
        )

    monkeypatch.setattr(
        kafka_admin,
        "ensure_topic_exists",
        dummy_ensure_topic_exists,
    )

    kafka_admin.ensure_streaming_topics_exist(
        vehicle_types=vehicle_types,
        resolved_runtime_config=resolved_runtime_config,
    )

    # one delegated call should be made for each requested vehicle type
    assert [call["topic_name"] for call in calls] == [
        "nyc_taxi_yellow",
        "nyc_taxi_green",
        "nyc_taxi_fhvhv",
    ]

    # the original resolved runtime config should be passed through unchanged
    assert all(
        call["resolved_runtime_config"] is resolved_runtime_config for call in calls
    )


def test_ensure_streaming_topics_exist_preserves_vehicle_type_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preserve input order so startup behavior stays predictable and easy to trace."""
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    delegated_topic_names: list[str] = []

    def dummy_ensure_topic_exists(
        topic_name: str, resolved_runtime_config: dict
    ) -> None:
        """Collect delegated topic names in call order."""
        delegated_topic_names.append(topic_name)

    monkeypatch.setattr(
        kafka_admin,
        "ensure_topic_exists",
        dummy_ensure_topic_exists,
    )

    kafka_admin.ensure_streaming_topics_exist(
        vehicle_types=["fhv", "yellow", "green"],
        resolved_runtime_config=resolved_runtime_config,
    )

    assert delegated_topic_names == [
        "nyc_taxi_fhv",
        "nyc_taxi_yellow",
        "nyc_taxi_green",
    ]


def test_ensure_streaming_topics_exist_reraises_downstream_topic_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Propagate downstream topic-ensure failures so streaming startup stops."""
    resolved_runtime_config = build_minimal_resolved_runtime_config()

    def dummy_ensure_topic_exists(
        topic_name: str, resolved_runtime_config: dict
    ) -> None:
        """Raise a representative downstream Kafka error."""
        raise KafkaException("metadata lookup failed")

    monkeypatch.setattr(
        kafka_admin,
        "ensure_topic_exists",
        dummy_ensure_topic_exists,
    )

    with pytest.raises(KafkaException):
        kafka_admin.ensure_streaming_topics_exist(
            vehicle_types=["yellow", "green"],
            resolved_runtime_config=resolved_runtime_config,
        )


def test_wait_for_kafka_ready_returns_immediately_when_kafka_is_ready(
    monkeypatch: pytest.MonkeyPatch,
    dummy_admin_config: dict,
) -> None:
    """
    Return on the first successful metadata lookup without sleeping.

    This is the fast path during service startup when Kafka is already reachable.
    """
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    admin_client_instances: list[DummyAdminClient] = []
    sleep_calls: list[float] = []

    class DummyAdminClientFactory:
        """Create a fresh dummy admin client for each readiness-check attempt."""

        def __init__(self, config: dict) -> None:
            assert config == dummy_admin_config

            client = DummyAdminClient(
                config=config,
                existing_topics={},
            )
            admin_client_instances.append(client)
            self._client = client

        def list_topics(self, timeout: float) -> object:
            """Delegate metadata lookup to the underlying dummy client."""
            return self._client.list_topics(timeout=timeout)

    monotonic_values = iter([100.0, 101.0])

    monkeypatch.setattr(kafka_admin, "AdminClient", DummyAdminClientFactory)
    monkeypatch.setattr(
        kafka_admin.time,
        "monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(
        kafka_admin.time,
        "sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    kafka_admin.wait_for_kafka_ready(
        resolved_runtime_config=resolved_runtime_config,
        timeout_seconds=60.0,
        retry_interval_seconds=2.0,
        metadata_timeout_seconds=5.0,
    )

    assert len(admin_client_instances) == 1
    assert admin_client_instances[0].list_topics_called_with == {"timeout": 5.0}
    assert sleep_calls == []


def test_wait_for_kafka_ready_retries_until_kafka_becomes_ready(
    monkeypatch: pytest.MonkeyPatch,
    dummy_admin_config: dict,
) -> None:
    """
    Retry readiness checks until a later attempt succeeds.

    This covers the normal container-startup race where Kafka is not ready on
    the first probe but becomes reachable shortly afterward.
    """
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    admin_client_instances: list[DummyAdminClient] = []
    sleep_calls: list[float] = []

    outcomes = iter(
        [
            KafkaException("broker not available"),
            KafkaException("broker not available"),
            None,
        ]
    )

    class DummyAdminClientFactory:
        """Create per-attempt dummy admin clients with staged outcomes."""

        def __init__(self, config: dict) -> None:
            assert config == dummy_admin_config

            list_topics_error = next(outcomes)
            client = DummyAdminClient(
                config=config,
                existing_topics={},
                list_topics_error=list_topics_error,
            )
            admin_client_instances.append(client)
            self._client = client

        def list_topics(self, timeout: float) -> object:
            """Delegate metadata lookup to the underlying dummy client."""
            return self._client.list_topics(timeout=timeout)

    monotonic_values = iter(
        [
            100.0,  # deadline = 110.0
            101.0,  # while check -> attempt 1
            102.0,  # remaining timeout after failure 1
            103.0,  # while check -> attempt 2
            104.0,  # remaining timeout after failure 2
            105.0,  # while check -> attempt 3 succeeds
        ]
    )

    monkeypatch.setattr(kafka_admin, "AdminClient", DummyAdminClientFactory)
    monkeypatch.setattr(
        kafka_admin.time,
        "monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(
        kafka_admin.time,
        "sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    kafka_admin.wait_for_kafka_ready(
        resolved_runtime_config=resolved_runtime_config,
        timeout_seconds=10.0,
        retry_interval_seconds=2.0,
        metadata_timeout_seconds=5.0,
    )

    assert len(admin_client_instances) == 3
    assert [client.list_topics_called_with for client in admin_client_instances] == [
        {"timeout": 5.0},
        {"timeout": 5.0},
        {"timeout": 5.0},
    ]
    assert sleep_calls == [2.0, 2.0]


def test_wait_for_kafka_ready_reraises_last_kafka_exception_after_timeout(
    monkeypatch: pytest.MonkeyPatch,
    dummy_admin_config: dict,
) -> None:
    """
    Re-raise the last KafkaException when readiness never succeeds before timeout.
    """
    resolved_runtime_config = build_minimal_resolved_runtime_config()
    admin_client_instances: list[DummyAdminClient] = []
    sleep_calls: list[float] = []
    kafka_error = KafkaException("broker not available")

    class DummyAdminClientFactory:
        """Create a failing dummy admin client for each retry attempt."""

        def __init__(self, config: dict) -> None:
            assert config == dummy_admin_config

            client = DummyAdminClient(
                config=config,
                existing_topics={},
                list_topics_error=kafka_error,
            )
            admin_client_instances.append(client)
            self._client = client

        def list_topics(self, timeout: float) -> object:
            """Delegate metadata lookup to the underlying dummy client."""
            return self._client.list_topics(timeout=timeout)

    monotonic_values = iter(
        [
            100.0,  # deadline = 105.0
            100.0,  # while check -> attempt 1
            101.0,  # remaining timeout after failure 1
            102.0,  # while check -> attempt 2
            104.0,  # remaining timeout after failure 2
            106.0,  # while check -> exit
        ]
    )

    monkeypatch.setattr(kafka_admin, "AdminClient", DummyAdminClientFactory)
    monkeypatch.setattr(
        kafka_admin.time,
        "monotonic",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(
        kafka_admin.time,
        "sleep",
        lambda seconds: sleep_calls.append(seconds),
    )

    with pytest.raises(KafkaException):
        kafka_admin.wait_for_kafka_ready(
            resolved_runtime_config=resolved_runtime_config,
            timeout_seconds=5.0,
            retry_interval_seconds=2.0,
            metadata_timeout_seconds=5.0,
        )

    assert len(admin_client_instances) == 2
    assert [client.list_topics_called_with for client in admin_client_instances] == [
        {"timeout": 5.0},
        {"timeout": 5.0},
    ]
    assert sleep_calls == [2.0, 2.0]
