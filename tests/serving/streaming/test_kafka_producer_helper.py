import json
from datetime import datetime, timezone

import pytest

from tests.helpers.kafka_producer_test_helpers import build_producer


def test_publish_rows_produces_each_row_and_returns_count_and_max_event_time(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Produce one Kafka message per row and return replay progress information.

    This verifies that _publish_rows():
    - produces one message for each input row
    - uses the configured topic name
    - passes through the key/value built by helper methods
    - attaches the delivery callback
    - returns the published row count
    - returns the maximum event time from the last row
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="yellow",
        mimic_service_start_time=mimic_service_start_time,
    )

    rows = [
        {
            "vendor_id": 1,
            "tpep_dropoff_datetime": datetime(2025, 3, 1, 5, 5, 0),
            "tpep_dropoff_datetime_utc": datetime(
                2025, 3, 1, 0, 5, 0, tzinfo=timezone.utc
            ),
        },
        {
            "vendor_id": 2,
            "tpep_dropoff_datetime": datetime(2025, 3, 1, 5, 10, 0),
            "tpep_dropoff_datetime_utc": datetime(
                2025, 3, 1, 0, 10, 0, tzinfo=timezone.utc
            ),
        },
    ]

    produced_calls: list[dict] = []

    class CapturingKafkaProducer:
        def produce(
            self,
            topic: str,
            key: bytes | None,
            value: bytes,
            on_delivery,
        ) -> None:
            # record each produce() call so the test can verify that every row
            # becomes exactly one Kafka message with the expected arguments
            produced_calls.append(
                {
                    "topic": topic,
                    "key": key,
                    "value": value,
                    "on_delivery": on_delivery,
                }
            )

        def poll(self, timeout: float) -> int:
            # this test checks call count separately, so the method only needs
            # to exist and accept the expected signature
            return 0

        def flush(self, timeout: float | None = None) -> int:
            return 0

    # replace the wrapped producer instance with a capturing double so the test
    # can inspect how _publish_rows() interacts with Kafka
    producer._producer = CapturingKafkaProducer()

    # stub helper methods so this test stays focused on _publish_rows() control
    # flow rather than key-generation or JSON serialization details
    monkeypatch.setattr(
        producer,
        "_build_message_key",
        lambda row: f"key-{row['vendor_id']}".encode("utf-8"),
    )
    monkeypatch.setattr(
        producer,
        "_serialize_record",
        lambda row: f"value-{row['vendor_id']}".encode("utf-8"),
    )

    published_count, max_event_time = producer._publish_rows(rows)

    # verify one Kafka produce() call was made for each input row
    assert len(produced_calls) == 2

    # verify the first produced message used the expected topic, key, value,
    # and delivery callback
    assert produced_calls[0]["topic"] == producer.topic_name
    assert produced_calls[0]["key"] == b"key-1"
    assert produced_calls[0]["value"] == b"value-1"
    assert produced_calls[0]["on_delivery"] == producer._on_delivery

    # verify the second produced message also used the expected arguments
    assert produced_calls[1]["topic"] == producer.topic_name
    assert produced_calls[1]["key"] == b"key-2"
    assert produced_calls[1]["value"] == b"value-2"
    assert produced_calls[1]["on_delivery"] == producer._on_delivery

    # verify the method reports replay progress based on the input rows
    assert published_count == 2
    assert max_event_time == datetime(2025, 3, 1, 0, 10, 0, tzinfo=timezone.utc)


def test_publish_rows_polls_after_each_produce_and_flushes_once(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Call poll(0) after each produced row and flush once after the batch.

    This verifies the intended batching behavior:
    - each produce() call is followed by a non-blocking poll(0)
    - flush() is called once after all rows have been handed to the producer
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="green",
        mimic_service_start_time=mimic_service_start_time,
    )

    rows = [
        {
            "vendor_id": 10,
            "lpep_dropoff_datetime": datetime(2025, 3, 1, 5, 2, 0),
            "lpep_dropoff_datetime_utc": datetime(
                2025, 3, 1, 0, 2, 0, tzinfo=timezone.utc
            ),
        },
        {
            "vendor_id": 20,
            "lpep_dropoff_datetime": datetime(2025, 3, 1, 5, 4, 0),
            "lpep_dropoff_datetime_utc": datetime(
                2025, 3, 1, 0, 4, 0, tzinfo=timezone.utc
            ),
        },
        {
            "vendor_id": 30,
            "lpep_dropoff_datetime": datetime(2025, 3, 1, 5, 6, 0),
            "lpep_dropoff_datetime_utc": datetime(
                2025, 3, 1, 0, 6, 0, tzinfo=timezone.utc
            ),
        },
    ]

    captured = {
        "produce_count": 0,
        "poll_args": [],
        "flush_count": 0,
    }

    class CapturingKafkaProducer:
        def produce(
            self,
            topic: str,
            key: bytes | None,
            value: bytes,
            on_delivery,
        ) -> None:
            # count how many rows were handed to produce()
            captured["produce_count"] += 1

        def poll(self, timeout: float) -> int:
            # record each timeout value so the test can verify poll(0) was used
            # once per row
            captured["poll_args"].append(timeout)
            return 0

        def flush(self, timeout: float | None = None) -> int:
            # count flush() calls to verify that batch flush happens exactly once
            captured["flush_count"] += 1
            return 0

    producer._producer = CapturingKafkaProducer()

    # stub helper methods because this test is about producer interaction order,
    # not key/value generation
    monkeypatch.setattr(producer, "_build_message_key", lambda row: None)
    monkeypatch.setattr(producer, "_serialize_record", lambda row: b"payload")

    producer._publish_rows(rows)

    # verify one produce() call per input row
    assert captured["produce_count"] == 3

    # verify poll(0) was called after each row
    assert captured["poll_args"] == [0, 0, 0]

    # verify flush() was called exactly once after the batch
    assert captured["flush_count"] == 1


def test_publish_rows_passes_rows_to_key_and_serializer_helpers_in_order(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Pass rows to _build_message_key() and _serialize_record() in input order.

    This verifies that helper methods are called once per row and receive the
    same rows, in the same order, that _publish_rows() iterates over.
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="fhv",
        mimic_service_start_time=mimic_service_start_time,
    )

    rows = [
        {
            "dispatching_base_num": "B1",
            "dropOff_datetime": datetime(2025, 3, 1, 5, 1, 0),
            "dropOff_datetime_utc": datetime(2025, 3, 1, 0, 1, 0, tzinfo=timezone.utc),
        },
        {
            "dispatching_base_num": "B2",
            "dropOff_datetime": datetime(2025, 3, 1, 5, 3, 0),
            "dropOff_datetime_utc": datetime(2025, 3, 1, 0, 3, 0, tzinfo=timezone.utc),
        },
    ]

    helper_calls = {
        "key_rows": [],
        "value_rows": [],
    }

    class CapturingKafkaProducer:
        def produce(
            self,
            topic: str,
            key: bytes | None,
            value: bytes,
            on_delivery,
        ) -> None:
            return None

        def poll(self, timeout: float) -> int:
            return 0

        def flush(self, timeout: float | None = None) -> int:
            return 0

    producer._producer = CapturingKafkaProducer()

    def fake_build_message_key(row: dict) -> bytes:
        # record which row is passed into key generation so the test can verify
        # helper call order and one-call-per-row behavior
        helper_calls["key_rows"].append(row)
        return b"key"

    def fake_serialize_record(row: dict) -> bytes:
        # record which row is passed into serialization so the test can verify
        # helper call order and one-call-per-row behavior
        helper_calls["value_rows"].append(row)
        return b"value"

    monkeypatch.setattr(producer, "_build_message_key", fake_build_message_key)
    monkeypatch.setattr(producer, "_serialize_record", fake_serialize_record)

    producer._publish_rows(rows)

    # verify key generation was called once per row, in input order
    assert helper_calls["key_rows"] == rows

    # verify serialization was also called once per row, in input order
    assert helper_calls["value_rows"] == rows


def test_serialize_record_returns_utf8_encoded_json_bytes(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Serialize a buffered row into UTF-8 encoded JSON bytes.

    This verifies the basic contract of _serialize_record(): the method should
    return bytes, and those bytes should decode into valid JSON representing
    the original row values.
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="yellow",
        mimic_service_start_time=mimic_service_start_time,
    )

    row = {
        "vendor_id": 1,
        "trip_distance": 3.5,
        "store_and_fwd_flag": "N",
    }

    serialized = producer._serialize_record(row)

    # verify the method returns bytes rather than a Python dict or string
    assert isinstance(serialized, bytes)

    # decode the payload and parse it back into Python so the test can verify
    # the JSON structure and values precisely
    decoded = json.loads(serialized.decode("utf-8"))

    # non-datetime values should be preserved exactly through serialization
    assert decoded == row


@pytest.mark.parametrize(
    ("vehicle_type", "row", "expected_decoded"),
    [
        (
            "green",
            {
                "vendor_id": 2,
                "lpep_dropoff_datetime": datetime(2025, 3, 1, 0, 5, 30),
            },
            {
                "vendor_id": 2,
                "lpep_dropoff_datetime": datetime(2025, 3, 1, 0, 5, 30).isoformat(),
            },
        ),
        (
            "fhvhv",
            {
                "trip_id": 100,
                "pickup_datetime": datetime(2025, 3, 1, 0, 1, 0),
                "dropoff_datetime": datetime(2025, 3, 1, 0, 8, 0),
            },
            {
                "trip_id": 100,
                "pickup_datetime": datetime(2025, 3, 1, 0, 1, 0).isoformat(),
                "dropoff_datetime": datetime(2025, 3, 1, 0, 8, 0).isoformat(),
            },
        ),
        (
            "fhv",
            {
                "dispatching_base_num": "B02512",
                "shared_ride": False,
                "fare_amount": 18.75,
                "passenger_count": 1,
                "notes": None,
            },
            {
                "dispatching_base_num": "B02512",
                "shared_ride": False,
                "fare_amount": 18.75,
                "passenger_count": 1,
                "notes": None,
            },
        ),
    ],
    ids=[
        "single-datetime",
        "multiple-datetimes",
        "json-compatible-non-datetime-values",
    ],
)
def test_serialize_record_decodes_to_expected_json_content(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
    vehicle_type: str,
    row: dict,
    expected_decoded: dict,
) -> None:
    """
    Serialize a buffered row, decode the JSON payload, and verify that the
    decoded content matches the expected output.

    This parametrized test covers:
    - conversion of a single datetime field to an ISO 8601 string
    - conversion of multiple datetime fields to ISO 8601 strings
    - preservation of JSON-compatible non-datetime values as-is
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type=vehicle_type,
        mimic_service_start_time=mimic_service_start_time,
    )

    # serialize the row and decode it back into Python objects so the test can
    # compare JSON content rather than raw byte formatting
    serialized = producer._serialize_record(row)
    decoded = json.loads(serialized.decode("utf-8"))

    assert decoded == expected_decoded


def test_build_message_key_returns_none(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Return None for every row in the current first-version key strategy.

    This documents that KafkaReplayProducer currently publishes records
    without a message key.
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="yellow",
        mimic_service_start_time=mimic_service_start_time,
    )

    row = {
        "vendor_id": 1,
        "tpep_dropoff_datetime": datetime(2025, 3, 1, 0, 5, 0),
    }

    # verify the current implementation always publishes without a key
    assert producer._build_message_key(row) is None


def test_serialize_record_adds_utc_offset_to_naive_utc_datetime_fields(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """Serialize naive *_utc datetime fields with an explicit +00:00 offset."""
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="yellow",
        mimic_service_start_time=mimic_service_start_time,
    )

    row = {
        "vendor_id": 1,
        "tpep_dropoff_datetime_utc": datetime(2025, 3, 1, 0, 5, 30),
    }

    serialized = producer._serialize_record(row)
    decoded = json.loads(serialized.decode("utf-8"))

    assert decoded == {
        "vendor_id": 1,
        "tpep_dropoff_datetime_utc": "2025-03-01T00:05:30+00:00",
    }
