from datetime import datetime
from unittest.mock import sentinel
from zoneinfo import ZoneInfo

from nyc_taxi_data_service.serving.streaming.pipeline import (
    _build_streaming_components,
)

NY_TZ = ZoneInfo("America/New_York")


def test_build_streaming_components_creates_window_buffer_and_replay_producer(
    mocker,
) -> None:
    """Create one window buffer and one replay producer for the vehicle type."""

    # patch the class names as they are imported and used inside `pipeline.py`
    # - when _build_streaming_components(...) calls StreamingWindowBuffer(...),
    #   it will call this mock instead of the real class
    # - when it calls KafkaReplayProducer(...),
    #   it will call this mock instead of the real class
    # `autospec=True` makes the mock closer to the real object's interface.
    mock_window_buffer_cls = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.StreamingWindowBuffer",
        autospec=True,
    )
    mock_replay_producer_cls = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.KafkaReplayProducer",
        autospec=True,
    )

    # tell the patched constructors what they should return when called
    mock_window_buffer_cls.return_value = sentinel.window_buffer
    mock_replay_producer_cls.return_value = sentinel.replay_producer

    # minimal shared inputs needed by the helper under test.
    resolved_runtime_config = {"some": "config"}
    mimic_service_start_time = datetime(2025, 1, 15, 12, 0, tzinfo=NY_TZ)

    # call the function under test
    # notes: because both classes were patched above, this call will NOT create real
    # StreamingWindowBuffer / KafkaReplayProducer objects
    # instead, it will call the mocked constructors and receive the sentinel return values
    window_buffer, replay_producer = _build_streaming_components(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    # confirm the helper returns exactly what the mocked constructors returned
    # `is` checks object identity
    assert window_buffer is sentinel.window_buffer
    assert replay_producer is sentinel.replay_producer

    # confirm each constructor was called exactly once with the expected kwargs
    mock_window_buffer_cls.assert_called_once_with(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )
    mock_replay_producer_cls.assert_called_once_with(
        vehicle_type="yellow",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )


def test_build_streaming_components_passes_vehicle_type_through_to_both_components(
    mocker,
) -> None:
    """Pass the requested vehicle type into both component constructors."""

    # patch both constructors so the helper uses mocks instead of real classes
    mock_window_buffer_cls = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.StreamingWindowBuffer",
        autospec=True,
    )
    mock_replay_producer_cls = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.KafkaReplayProducer",
        autospec=True,
    )

    resolved_runtime_config = {"some": "config"}
    mimic_service_start_time = datetime(2025, 1, 15, 12, 0, tzinfo=NY_TZ)

    # use a specific vehicle type and let the helper call the mocked constructors
    _build_streaming_components(
        vehicle_type="green",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    # `call_args` stores the most recent call made to the mock
    # `.kwargs` gives the keyword arguments used in that call
    # here we inspect only the `vehicle_type` field to verify that the value
    # was forwarded into both constructors
    assert mock_window_buffer_cls.call_args.kwargs["vehicle_type"] == "green"
    assert mock_replay_producer_cls.call_args.kwargs["vehicle_type"] == "green"


def test_build_streaming_components_passes_shared_start_time_to_both_components(
    mocker,
) -> None:
    """Pass the same mimic service start time into both component constructors."""

    # patch both constructors again so we can inspect their call arguments
    mock_window_buffer_cls = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.StreamingWindowBuffer",
        autospec=True,
    )
    mock_replay_producer_cls = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.KafkaReplayProducer",
        autospec=True,
    )

    resolved_runtime_config = {"some": "config"}

    # use a distinctive timestamp so it is easy to confirm the exact same value
    # is passed into both constructors
    mimic_service_start_time = datetime(2025, 2, 1, 8, 30, tzinfo=NY_TZ)

    _build_streaming_components(
        vehicle_type="fhv",
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    # confirm that both constructors received the same startup timestamp
    # this matters because buffering and replay should share the same
    # service-time anchor
    assert (
        mock_window_buffer_cls.call_args.kwargs["mimic_service_start_time"]
        == mimic_service_start_time
    )
    assert (
        mock_replay_producer_cls.call_args.kwargs["mimic_service_start_time"]
        == mimic_service_start_time
    )
