import json
import logging
import textwrap
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
from confluent_kafka import Producer

logger = logging.getLogger(__name__)  # initialize default logger


# KafkaReplayProducer reads buffered rows for one vehicle type from DuckDB,
# publishes eligible records to a Kafka topic in event-time order,
# and advances an internal replay watermark.
class KafkaReplayProducer:
    """
    Replay buffered rows for one vehicle type into a Kafka topic.

    Replay state is tracked in timezone-aware `America/New_York` datetimes at
    the service layer. Before querying DuckDB, replay bounds are converted to
    naive New York-local datetimes so they align with the raw TLC event-time
    values stored in the buffer table.
    """

    def __init__(
        self,
        vehicle_type: str,
        resolved_runtime_config: dict,
        mimic_service_start_time: datetime,
    ) -> None:
        """
        Initialize a replay producer for one vehicle type.

        The producer reads buffered rows from the vehicle type's DuckDB buffer,
        publishes eligible rows to the corresponding Kafka topic, and tracks
        replay progress using an internal watermark.

        Args:
            vehicle_type: Vehicle type served by this producer, such as
                `yellow` or `green`.
            resolved_runtime_config: Runtime configuration dictionary containing
                serving, buffer, and Kafka settings.
            mimic_service_start_time: Initial replay lower bound as a
                timezone-aware `America/New_York` datetime.
        """
        streaming_config = resolved_runtime_config["serving"]["streaming"]
        kafka_config = streaming_config["kafka"]

        # replay state
        # the watermark tracks the last successfully replayed event time
        # (the greatest event time that has already been successfully replayed)
        self.mimic_service_start_time = mimic_service_start_time
        self._replay_watermark = None

        # vehicle-specific metadata
        self.vehicle_type = vehicle_type
        event_time_map = resolved_runtime_config["serving"][
            "service_event_time_column_by_vehicle_type"
        ]
        if vehicle_type not in event_time_map:
            raise ValueError(f"Unsupported vehicle_type: {vehicle_type}")
        self.event_time_column = event_time_map[vehicle_type]

        # buffer source settings
        # each vehicle type (yellow, green, etc.) has its own physical DuckDB file
        self.buffer_db_path = (
            Path(streaming_config["buffer"]["db_base_path"]) / f"{vehicle_type}.duckdb"
        )
        self.buffer_table_name = streaming_config["buffer"]["db_table_name"]

        # kafka topic naming
        self.topic_name = f"{kafka_config['topic']['prefix']}_{vehicle_type}"

        # kafka producer client config
        self.producer_config = self._build_producer_config(kafka_config)

        # construct kafka producer
        self._producer = Producer(self.producer_config)

    @staticmethod
    def _build_producer_config(kafka_config: dict) -> dict:
        """
        Build a Confluent Kafka producer config dict from runtime config.

        This helper validates required fields and maps supported keys into
        the dot-separated librdkafka names expected by `confluent_kafka.Producer`.

        Args:
            kafka_config: Kafka config dictionary containing `client` and
                `producer` sections.

        Returns:
            A normalized producer config dictionary suitable for constructing
            `confluent_kafka.Producer`.

        Raises:
            ValueError: If required settings are missing or unsupported config
                keys are provided.
        """
        # map internal snake_case config names to the dot-separated string keys
        # required by the confluent-kafka/librdkafka C library
        key_map = {
            "bootstrap_servers": "bootstrap.servers",
            "client_id": "client.id",
            "acks": "acks",
            "linger_ms": "linger.ms",
            "batch_size": "batch.size",
            "compression_type": "compression.type",
            # future examples:
            # "security_protocol": "security.protocol",
            # "sasl_mechanism": "sasl.mechanisms",
            # "sasl_username": "sasl.username",
            # "sasl_password": "sasl.password",
            # "ssl_ca_location": "ssl.ca.location",
        }

        raw_producer_config = {
            **kafka_config["client"],
            **kafka_config["producer"],
        }

        # connectivity check: The producer cannot function without a broker address
        bootstrap_servers = raw_producer_config.get("bootstrap_servers")
        if not bootstrap_servers:
            raise ValueError(
                "Kafka client config must include a non-empty 'bootstrap_servers'"
            )

        producer_config = {}
        for key, value in raw_producer_config.items():
            mapped_key = key_map.get(key)
            if mapped_key is None:
                raise ValueError(f"Unsupported Kafka producer config key: {key}")
            producer_config[mapped_key] = value

        return producer_config

    @staticmethod
    def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        """
        Return whether the given table exists in the current DuckDB database.
        """
        try:
            con.table(table_name)
            return True
        except duckdb.CatalogException:
            return False

    @staticmethod
    def _from_duckdb_timestamp_value(dt: datetime) -> datetime:
        """
        Convert a DuckDB TIMESTAMP value into an aware New York datetime.

        DuckDB returns raw TLC event-time values as naive timestamps. This helper
        attaches the New York timezone so replay watermark state remains explicit
        and consistent in Python service logic.
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=ZoneInfo("America/New_York"))
        return dt.astimezone(ZoneInfo("America/New_York"))

    @staticmethod
    def _to_duckdb_timestamp_value(dt: datetime) -> datetime:
        """
        Convert a service-layer replay boundary into a naive DuckDB timestamp.

        Replay bounds are tracked in Python as timezone-aware New York datetimes.
        The buffer table stores raw TLC event-time values as naive timestamps, so
        this helper drops timezone info while preserving the local wall-clock time.

        Args:
            dt: Datetime value used as a replay query boundary.

        Returns:
            A naive New York-local datetime suitable for DuckDB TIMESTAMP
            comparisons.
        """
        if dt.tzinfo is None:
            return dt
        return dt.replace(tzinfo=None)

    def publish_available(self, mimic_current_time: datetime) -> int:
        """
        Publish all buffered rows currently eligible for replay.

        This method fetches rows whose event time falls within the current replay
        window, publishes them to Kafka, and advances the internal replay watermark
        to the maximum event time successfully published.

        Args:
            mimic_current_time: Current service time used as the inclusive upper
                bound of the replay window.

        Returns:
            Number of rows published in this replay cycle.
        """
        logger.debug(
            "Publish available rows | vehicle_type=%s | watermark=%s | mimic_current_time=%s",
            self.vehicle_type,
            self._replay_watermark,
            mimic_current_time,
        )

        rows = self._fetch_eligible_rows(mimic_current_time)
        logger.debug(
            "Fetched %d eligible buffered rows for publish | vehicle_type=%s",
            len(rows),
            self.vehicle_type,
        )
        if not rows:
            return 0

        # only advance the watermark after the batch has been successfully
        # produced and flushed for this replay cycle
        published_count, max_event_time = self._publish_rows(rows)
        self._replay_watermark = max_event_time

        logger.info(
            "Publish cycle complete | vehicle_type=%s | published_count=%d | new_watermark=%s",
            self.vehicle_type,
            published_count,
            self._replay_watermark,
        )

        return published_count

    def _get_fetch_bounds(
        self,
        mimic_current_time: datetime,
    ) -> tuple[datetime, datetime]:
        """
        Build the lower and upper replay bounds for the current fetch query.

        Replay state is kept as aware New York datetimes in Python, but DuckDB
        compares against naive TIMESTAMP values in the buffer table. This helper
        therefore converts both bounds into naive New York-local datetimes
        before they are used as SQL query parameters.
        """
        if self._replay_watermark is None:
            lower_bound = self._to_duckdb_timestamp_value(self.mimic_service_start_time)
        else:
            lower_bound = self._to_duckdb_timestamp_value(self._replay_watermark)

        upper_bound = self._to_duckdb_timestamp_value(mimic_current_time)

        return lower_bound, upper_bound

    def _fetch_eligible_rows(self, mimic_current_time: datetime) -> list[dict]:
        """
        Fetch buffered rows whose event time falls within the current replay window.

        The replay window is open on the lower bound and closed on the upper bound:
        `(self._replay_watermark, mimic_current_time]`.

        Rows are returned in ascending event-time order so they can be published
        deterministically within a replay cycle.

        Args:
            mimic_current_time: Current service time used as the inclusive upper
                bound for replay eligibility.

        Returns:
            A list of buffered row dictionaries eligible to be published.
        """
        logger.debug(
            "Fetch eligible rows | vehicle_type=%s | branch=%s | watermark=%s | mimic_service_start_time=%s | mimic_current_time=%s",
            self.vehicle_type,
            "initial" if self._replay_watermark is None else "incremental",
            self._replay_watermark,
            self.mimic_service_start_time,
            mimic_current_time,
        )

        # construct parameterized query
        # fetch window:
        # - initial: [mimic_service_start_time, mimic_current_time]
        # - later: (replay_watermark, mimic_current_time]
        if self._replay_watermark is None:
            query = textwrap.dedent(
                f"""
                SELECT *
                FROM {self.buffer_table_name}
                WHERE {self.event_time_column} IS NOT NULL
                    AND {self.event_time_column} >= ?
                    AND {self.event_time_column} <= ?
                ORDER BY {self.event_time_column} ASC
                """
            )
        else:
            query = textwrap.dedent(
                f"""
                SELECT *
                FROM {self.buffer_table_name}
                WHERE {self.event_time_column} IS NOT NULL
                    AND {self.event_time_column} > ?
                    AND {self.event_time_column} <= ?
                ORDER BY {self.event_time_column} ASC
                """
            )
        lower_bound, upper_bound = self._get_fetch_bounds(mimic_current_time)
        query_params = [lower_bound, upper_bound]

        with duckdb.connect(self.buffer_db_path) as con:
            if not self._table_exists(con, self.buffer_table_name):
                logger.debug(
                    "Buffer table does not exist yet | vehicle_type=%s | table_name=%s",
                    self.vehicle_type,
                    self.buffer_table_name,
                )
                return []

            logger.debug(
                "Normalized DuckDB query params | lower=%r (%s) | upper=%r (%s)",
                query_params[0],
                type(query_params[0]).__name__,
                query_params[-1],
                type(query_params[-1]).__name__,
            )

            rows = con.execute(query, query_params).to_arrow_table().to_pylist()
            # if rows were fetched, log a quick summary of the buffer table
            if rows:
                stats_query = textwrap.dedent(
                    f"""
                    SELECT
                        MIN({self.event_time_column}) AS min_event_time,
                        MAX({self.event_time_column}) AS max_event_time,
                        COUNT(*) AS row_count
                    FROM {self.buffer_table_name}
                    WHERE {self.event_time_column} IS NOT NULL
                    """
                )

                stats_row = con.execute(stats_query).fetchone()

                logger.debug(
                    "Buffer table stats | vehicle_type=%s | min_event_time=%s | max_event_time=%s | row_count=%s",
                    self.vehicle_type,
                    stats_row[0],
                    stats_row[1],
                    stats_row[2],
                )

        return rows

    def _publish_rows(self, rows: list[dict]) -> tuple[int, datetime]:
        """
        Publish buffered rows to Kafka and return replay progress information.

        Each row is serialized as one Kafka message. The replay watermark should
        only be advanced by the caller after this method completes successfully.

        Args:
            rows: Buffered row dictionaries to publish.

        Returns:
            A tuple of:
            - number of rows published
            - maximum event time among the published rows
        """
        logger.debug(
            "Publish row batch | vehicle_type=%s | topic=%s | row_count=%d | first_event_time=%s | last_event_time=%s",
            self.vehicle_type,
            self.topic_name,
            len(rows),
            rows[0][self.event_time_column],
            rows[-1][self.event_time_column],
        )

        # since the input rows are sorted by time, the last row represents
        # the new highest watermark for this vehicle
        max_event_time = self._from_duckdb_timestamp_value(
            rows[-1][self.event_time_column]
        )
        published_count = 0

        for row in rows:
            self._producer.produce(
                topic=self.topic_name,
                key=self._build_message_key(row),
                value=self._serialize_record(row),
                on_delivery=self._on_delivery,
            )
            # poll(0) serves delivery callbacks non-blockingly to avoid
            # the producer's internal event queue filling up
            self._producer.poll(0)
            published_count += 1

        # synchronously wait for all messages in this specific batch to be delivered
        # before returning control to the orchestration loop
        self._producer.flush()

        return published_count, max_event_time

    def _serialize_record(self, row: dict) -> bytes:
        """
        Serialize one buffered row into a Kafka message payload.

        Datetime values are converted to ISO 8601 strings so the row can be encoded
        as JSON.

        Args:
            row: One buffered row dictionary.

        Returns:
            UTF-8 encoded JSON bytes representing the row.
        """
        # standard JSON does not support datetime objects
        # transform them to ISO strings to ensure interoperability with consumers
        serialized_row = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=ZoneInfo("America/New_York"))
                serialized_row[key] = value.isoformat()
            else:
                serialized_row[key] = value

        return json.dumps(serialized_row).encode("utf-8")

    def _build_message_key(self, row: dict) -> bytes | None:
        """
        Build the Kafka message key for one buffered row.

        This first implementation returns `None` so records are published
        without a key.

        Args:
            row: One buffered row dictionary.

        Returns:
            Kafka message key bytes, or `None` to publish without a key.
        """
        # returning None publishes without a key and lets the producer choose
        # partitions using its default behavior.
        return None  # publish without a key for now

    def _on_delivery(self, err, msg) -> None:
        """
        Log Kafka delivery failures for produced messages.

        Args:
            err: Delivery error reported by the Kafka client, or None on success.
            msg: Kafka message associated with the delivery result.
        """
        if err is not None:
            logger.warning(
                "Delivery failed | topic=%s | partition=%s | offset=%s | error=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                err,
            )

    def close(self, timeout: float = 10.0) -> int:
        """
        Flush queued Kafka messages before shutdown.

        Args:
            timeout: Maximum number of seconds to wait for queued and in-flight
                messages to be delivered.

        Returns:
            Number of messages still queued after the flush timeout expires.
        """
        # flush blocks until all messages currently in the producer's internal
        # buffer are delivered or the timeout is reached
        return self._producer.flush(timeout)
