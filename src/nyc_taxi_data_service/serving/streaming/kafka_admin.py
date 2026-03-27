from __future__ import annotations

import logging

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.cimpl import KafkaException

logger = logging.getLogger(__name__)

# TODO:
#   This first version assumes the resolved runtime config already contains the
#   required Kafka sections and value shapes (for example: client, topic,
#   bootstrap_servers, prefix, num_partitions, replication_factor, retention_ms).
#   Later, extract shared runtime-config validation into a dedicated validator
#   module so this file can focus only on Kafka admin behavior.


def build_admin_client_config(kafka_config: dict) -> dict:
    # TODO:
    #   extend client-config key mapping when SSL/SASL settings are added
    #   for now, only the keys required by local/plaintext admin access are supported.
    key_map = {
        "bootstrap_servers": "bootstrap.servers",
        "client_id": "client.id",
    }
    raw_client_config = {
        **kafka_config["client"],
    }
    bootstrap_servers = raw_client_config.get("bootstrap_servers")
    if not bootstrap_servers:
        raise ValueError(
            "Kafka client config must include a non-empty 'bootstrap_servers'"
        )

    client_config = {}
    for key, value in raw_client_config.items():
        mapped_key = key_map.get(key)
        if mapped_key is None:
            raise ValueError(f"Unsupported Kafka admin client config key: {key}")
        client_config[mapped_key] = value

    return client_config


def ensure_topic_exists(
    topic_name: str,
    resolved_runtime_config: dict,
) -> None:
    """
    Ensure that a single Kafka topic exists, creating it if necessary.

    Config source:
        resolved_runtime_config["serving"]["streaming"]["kafka"]

    Expected topic defaults:
        - num_partitions
        - replication_factor
        - retention_ms

    Expected client config:
        - bootstrap_servers

    Creation behavior:
    - create a Kafka AdminClient from bootstrap_servers
    - fetch current cluster metadata to check whether topic_name already exists
    - if missing, submit a create-topics request for exactly one topic
    - wait on the returned future so startup fails fast on real errors

    Args:
        topic_name: Fully qualified Kafka topic name, such as
            "nyc_taxi_yellow".
        resolved_runtime_config: Fully resolved runtime config dictionary.

    Raises:
        ValueError:
            - if topic_name is empty
            - if required Kafka client/topic settings are missing
        KafkaException:
            If Kafka metadata retrieval or topic creation fails.
    """
    if not topic_name:
        raise ValueError("topic_name must be a non-empty string.")

    # create admin client
    kafka_config = resolved_runtime_config["serving"]["streaming"]["kafka"]
    client_config = build_admin_client_config(kafka_config=kafka_config)
    admin_client = AdminClient(client_config)

    logger.info("Checking whether Kafka topic exists | topic_name=%s", topic_name)
    try:
        # query cluster metadata first so we can skip creation if the topic already exists.
        # `list_topics()` returns a metadata snapshot that includes a `topics` mapping keyed by topic name
        # TODO: move this metadata timeout to a named constant or config.
        cluster_metadata = admin_client.list_topics(timeout=10)
    except KafkaException:
        logger.exception(
            "Failed to fetch Kafka metadata while checking topic existence | "
            "topic_name=%s",
            topic_name,
        )
        raise

    if topic_name in cluster_metadata.topics:
        logger.info("Kafka topic already exists | topic_name=%s", topic_name)
        return

    # create a kafka topic if it does not exist yet
    topic_config = kafka_config["topic"]
    num_partitions = topic_config.get("num_partitions")
    replication_factor = topic_config.get("replication_factor")
    retention_ms = topic_config.get("retention_ms")
    if num_partitions is None:
        raise ValueError("Kafka topic config must include 'num_partitions'")
    if replication_factor is None:
        raise ValueError("Kafka topic config must include 'replication_factor'")
    if retention_ms is None:
        raise ValueError("Kafka topic config must include 'retention_ms'")
    logger.info(
        "Kafka topic does not exist yet; creating topic | topic_name=%s | "
        "num_partitions=%s | replication_factor=%s | retention_ms=%s",
        topic_name,
        num_partitions,
        replication_factor,
        retention_ms,
    )

    # define the topic to create
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        config={
            "retention.ms": str(retention_ms),
        },
    )

    # wait on the single future so startup can fail fast if topic creation fails
    # `create_topics(...)` returns a mapping: {topic_name: future}
    futures = admin_client.create_topics(new_topics=[new_topic])
    future = futures[topic_name]

    # wait & confirm the result of topic creation
    try:
        future.result()
        logger.info("Successfully created Kafka topic | topic_name=%s", topic_name)
    except KafkaException as exc:
        # topic creation may race with another process
        # in that case, the topic may already exist even though the earlier
        #   metadata snapshot did not include it yet
        # treat that case as success and return
        error_message = str(exc).lower()
        if "topic already exists" in error_message:
            logger.info(
                "Kafka topic already exists after concurrent creation attempt | "
                "topic_name=%s",
                topic_name,
            )
            return

        # otherwise, topic creation failed
        logger.exception(
            "Failed to create Kafka topic | topic_name=%s",
            topic_name,
        )
        raise


def ensure_streaming_topics_exist(
    vehicle_types: list[str],
    resolved_runtime_config: dict,
) -> None:
    """
    Ensure that all Kafka topics required by the streaming service exist.

    This helper is intended to be called once during streaming-service startup,
    before creating the replay producers. It reads Kafka connection settings and
    topic defaults from the resolved runtime config, derives one topic name per
    configured vehicle type, and then delegates the actual existence-check /
    creation logic to `ensure_topic_exists`.

    Topic naming rule:
        <topic_prefix>_<vehicle_type>

    Example:
        topic_prefix = "nyc_taxi"
        vehicle_types = ["yellow", "green"]

        -> topics ensured:
           - nyc_taxi_yellow
           - nyc_taxi_green

    Args:
        vehicle_types: Vehicle types enabled for the current streaming run,
            such as ["yellow", "green"].
        resolved_runtime_config: Fully resolved runtime config dictionary.

    Raises:
        ValueError:
            - if vehicle_types is empty
            - if required Kafka topic config is missing
        KafkaException:
            If Kafka metadata lookup or topic creation fails in a way that
            should stop service startup.
    """
    if not vehicle_types:
        raise ValueError(
            "vehicle_types must contain at least one vehicle type when ensuring "
            "streaming Kafka topics."
        )

    kafka_config = resolved_runtime_config["serving"]["streaming"]["kafka"]
    topic_config = kafka_config["topic"]

    topic_prefix = topic_config.get("prefix")
    if not topic_prefix:
        raise ValueError(
            "serving.streaming.kafka.topic.prefix must be set in runtime config."
        )

    logger.info(
        "Ensuring streaming Kafka topics exist | topic_prefix=%s | vehicle_types=%s",
        topic_prefix,
        vehicle_types,
    )

    # ensure one topic per configured vehicle type.
    # notes: topic creation itself is delegated to `ensure_topic_exists(...)` so this
    # function stays focused on orchestration over the full vehicle-type list
    for vehicle_type in vehicle_types:
        # e.g. prefix="nyc_taxi", vehicle_type="yellow" -> topic_name="nyc_taxi_yellow"
        topic_name = f"{topic_prefix}_{vehicle_type}"

        ensure_topic_exists(
            topic_name=topic_name,
            resolved_runtime_config=resolved_runtime_config,
        )

    logger.info(
        "Finished ensuring streaming Kafka topics exist | ensured_topics=%s",
        [f"{topic_prefix}_{vehicle_type}" for vehicle_type in vehicle_types],
    )
