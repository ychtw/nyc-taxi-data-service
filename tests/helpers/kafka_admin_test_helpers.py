from __future__ import annotations

from types import SimpleNamespace

import pytest


class DummyFuture:
    """Simple future test double that either succeeds or raises on result()."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.result_called = False

    def result(self) -> None:
        """Return successfully unless configured to raise an error."""
        self.result_called = True
        if self.error is not None:
            raise self.error


class DummyAdminClient:
    """
    Minimal AdminClient test double for `ensure_topic_exists`.

    The dummy lets each test control:
    - existing cluster topics returned by `list_topics`
    - the mapping returned by `create_topics`
    - optional exceptions raised by `list_topics`
    """

    def __init__(
        self,
        config: dict,
        *,
        existing_topics: dict | None = None,
        create_topics_result: dict | None = None,
        list_topics_error: Exception | None = None,
    ) -> None:
        self.config = config
        self.existing_topics = existing_topics or {}
        self.create_topics_result = create_topics_result or {}
        self.list_topics_error = list_topics_error

        self.list_topics_called_with: dict | None = None
        self.create_topics_called_with: list | None = None

    def list_topics(self, timeout: int) -> SimpleNamespace:
        """Return dummy metadata or raise a configured Kafka error."""
        self.list_topics_called_with = {"timeout": timeout}
        if self.list_topics_error is not None:
            raise self.list_topics_error
        return SimpleNamespace(topics=self.existing_topics)

    def create_topics(self, new_topics: list) -> dict:
        """Record the create request and return the configured future mapping."""
        self.create_topics_called_with = new_topics
        return self.create_topics_result


class DummyNewTopic:
    """Capture topic creation arguments without relying on confluent internals."""

    def __init__(
        self,
        topic: str,
        num_partitions: int,
        replication_factor: int,
        config: dict,
    ) -> None:
        self.topic = topic
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.config = config


def build_minimal_resolved_runtime_config() -> dict:
    """
    Build a minimal resolved runtime config for Kafka admin tests.

    This mirrors the relevant shape from runtime.yaml while keeping the test
    fixture small and focused on the keys used by `ensure_topic_exists`.
    """
    return {
        "serving": {
            "streaming": {
                "kafka": {
                    "client": {
                        "bootstrap_servers": "localhost:9092",
                        "client_id": "nyc-taxi-data-service",
                    },
                    "topic": {
                        "prefix": "nyc_taxi",
                        "num_partitions": 3,
                        "replication_factor": 1,
                        "retention_ms": 86400000,
                    },
                }
            }
        }
    }


def patch_admin_client(
    monkeypatch: pytest.MonkeyPatch,
    kafka_admin_module,
    dummy_admin_client,
    dummy_admin_config: dict,
    *,
    patch_new_topic: bool = False,
) -> None:
    """Patch Kafka admin dependencies for unit tests."""
    monkeypatch.setattr(
        kafka_admin_module,
        "build_admin_client_config",
        lambda kafka_config: dummy_admin_config,
    )
    monkeypatch.setattr(
        kafka_admin_module,
        "AdminClient",
        lambda config: dummy_admin_client,
    )
    if patch_new_topic:
        monkeypatch.setattr(kafka_admin_module, "NewTopic", DummyNewTopic)
