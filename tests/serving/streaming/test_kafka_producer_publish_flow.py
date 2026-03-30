from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from tests.helpers.kafka_producer_test_helpers import build_producer

NY_TZ = ZoneInfo("America/New_York")


def test_publish_available_returns_zero_when_no_rows(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Return 0 and leave the replay watermark unchanged when no rows are eligible.

    This verifies the early-return branch of publish_available(), where the
    method should stop after fetching rows and must not call _publish_rows().
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="yellow",
        mimic_service_start_time=mimic_service_start_time,
    )

    # set a non-None watermark first so the test can confirm that the value
    # remains unchanged when no eligible rows are returned
    existing_watermark = datetime(2025, 3, 1, 0, 10, 0, tzinfo=NY_TZ)
    producer._replay_watermark = existing_watermark

    # replace _fetch_eligible_rows() with a stub that simulates an empty replay
    # window, so publish_available() should take its early-return branch
    monkeypatch.setattr(producer, "_fetch_eligible_rows", lambda _: [])

    def fail_if_called(rows: list[dict]) -> tuple[int, datetime]:
        # if _publish_rows() is called in this scenario, the orchestration logic
        # is wrong, so fail the test immediately
        raise AssertionError("_publish_rows should not be called when no rows exist")

    # patch _publish_rows() so the test explicitly verifies it is skipped
    monkeypatch.setattr(producer, "_publish_rows", fail_if_called)

    mimic_current_time = datetime(2025, 3, 1, 0, 15, 0, tzinfo=NY_TZ)

    result = producer.publish_available(mimic_current_time)

    # when no rows are available, the method should return 0
    assert result == 0

    # and it should leave replay progress unchanged
    assert producer._replay_watermark == existing_watermark


def test_publish_available_publishes_rows_and_updates_watermark(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Publish eligible rows, return the published count, and advance the replay
    watermark to the maximum event time returned by _publish_rows().
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="green",
        mimic_service_start_time=mimic_service_start_time,
    )

    mimic_current_time = datetime(2025, 3, 1, 0, 20, 0, tzinfo=NY_TZ)
    fetched_rows = [
        {"lpep_dropoff_datetime": datetime(2025, 3, 1, 0, 5, 0)},
        {"lpep_dropoff_datetime": datetime(2025, 3, 1, 0, 10, 0)},
    ]
    max_event_time = datetime(2025, 3, 1, 0, 10, 0, tzinfo=NY_TZ)

    # capture the value passed into _fetch_eligible_rows() so the test can
    # verify that publish_available() forwards mimic_current_time correctly
    captured = {}

    def fake_fetch(current_time: datetime) -> list[dict]:
        captured["fetch_arg"] = current_time
        return fetched_rows

    def fake_publish(rows: list[dict]) -> tuple[int, datetime]:
        # verify that the same rows fetched from the replay window are passed
        # directly into _publish_rows() without being modified
        captured["publish_arg"] = rows
        return 2, max_event_time

    monkeypatch.setattr(producer, "_fetch_eligible_rows", fake_fetch)
    monkeypatch.setattr(producer, "_publish_rows", fake_publish)

    result = producer.publish_available(mimic_current_time)

    # verify the fetch step received the caller-provided replay horizon
    assert captured["fetch_arg"] == mimic_current_time

    # verify the publish step received the exact rows fetched from the buffer
    assert captured["publish_arg"] == fetched_rows

    # verify the method returns the published row count from _publish_rows()
    assert result == 2

    # verify replay progress advances only after a successful publish step
    assert producer._replay_watermark == max_event_time


def test_publish_available_sets_initial_watermark_after_first_successful_publish(
    monkeypatch: pytest.MonkeyPatch,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> None:
    """
    Set the initial replay watermark when the first successful publish occurs.

    KafkaReplayProducer starts with _replay_watermark = None so the first replay
    window can include rows whose event time equals mimic_service_start_time.
    This test verifies that the first successful publish establishes the
    watermark for subsequent replay cycles.
    """
    producer = build_producer(
        monkeypatch=monkeypatch,
        resolved_runtime_config=resolved_runtime_config,
        vehicle_type="fhv",
        mimic_service_start_time=mimic_service_start_time,
    )

    # confirm the test starts from the special initial state used by the class
    assert producer._replay_watermark is None

    fetched_rows = [
        {"dropOff_datetime": datetime(2025, 3, 1, 0, 0, 0)},
    ]
    max_event_time = datetime(2025, 3, 1, 0, 0, 0, tzinfo=NY_TZ)

    monkeypatch.setattr(producer, "_fetch_eligible_rows", lambda _: fetched_rows)
    monkeypatch.setattr(producer, "_publish_rows", lambda rows: (1, max_event_time))

    result = producer.publish_available(datetime(2025, 3, 1, 0, 1, 0, tzinfo=NY_TZ))

    # verify the first successful publish returns the published count
    assert result == 1

    # verify the initial None watermark is replaced by the max published event time
    assert producer._replay_watermark == max_event_time
