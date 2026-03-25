from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, call

from nyc_taxi_data_service.serving.streaming.pipeline import run_streaming
from tests.helpers.streaming_pipeline_test_helpers import (
    build_resolved_runtime_config_for_run_streaming,
)


def test_run_streaming_ensures_topics_builds_components_and_runs_startup_steps(
    mocker,
) -> None:
    """Ensure startup creates topics, builds components, buffers, and publishes."""
    resolved_runtime_config = build_resolved_runtime_config_for_run_streaming()

    stream_end_time = datetime(2025, 2, 1, tzinfo=timezone.utc)
    window_size = timedelta(minutes=30)
    publish_interval = timedelta(seconds=5)

    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._build_stream_exhaustion_boundary",
        return_value=stream_end_time,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._resolve_stream_timing_config",
        return_value=(window_size, publish_interval),
    )

    mock_service_clock = Mock()
    mock_service_clock.mimic_service_start_time = datetime(
        2025, 1, 15, 8, 0, tzinfo=timezone.utc
    )
    # first now() -> initial publish, second now() -> main loop exits on exhaustion
    mock_service_clock.now.side_effect = [
        datetime(2025, 1, 15, 8, 0, tzinfo=timezone.utc),
        stream_end_time,
    ]
    mock_service_clock_cls = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.ServiceClock",
        return_value=mock_service_clock,
    )

    mock_ensure_topics = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.ensure_streaming_topics_exist"
    )
    mock_build_components = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._build_streaming_components",
        side_effect=[
            (Mock(), Mock()),
            (Mock(), Mock()),
        ],
    )
    mock_initial_buffering = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._run_initial_buffering"
    )
    mock_publish_cycle = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._run_publish_cycle"
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.datetime",
    )
    mocker.patch("nyc_taxi_data_service.serving.streaming.pipeline.time.sleep")

    run_streaming(resolved_runtime_config)

    mock_service_clock_cls.assert_called_once_with(mimic_current_date="2025-01-15")
    mock_ensure_topics.assert_called_once_with(
        vehicle_types=["yellow", "green"],
        resolved_runtime_config=resolved_runtime_config,
    )

    assert mock_build_components.call_args_list == [
        call(
            vehicle_type="yellow",
            resolved_runtime_config=resolved_runtime_config,
            mimic_service_start_time=mock_service_clock.mimic_service_start_time,
        ),
        call(
            vehicle_type="green",
            resolved_runtime_config=resolved_runtime_config,
            mimic_service_start_time=mock_service_clock.mimic_service_start_time,
        ),
    ]

    mock_initial_buffering.assert_called_once()
    assert (
        mock_publish_cycle.call_count == 2
    )  # initial publish + final publish on exhaustion


def test_run_streaming_runs_final_publish_when_stream_is_exhausted(
    mocker,
) -> None:
    """Run one final publish cycle before shutdown when mimic time reaches the end."""
    resolved_runtime_config = build_resolved_runtime_config_for_run_streaming()

    stream_end_time = datetime(2025, 2, 1, tzinfo=timezone.utc)
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._build_stream_exhaustion_boundary",
        return_value=stream_end_time,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._resolve_stream_timing_config",
        return_value=(timedelta(minutes=30), timedelta(seconds=5)),
    )

    mock_service_clock = Mock()
    mock_service_clock.mimic_service_start_time = datetime(
        2025, 1, 15, 8, 0, tzinfo=timezone.utc
    )
    initial_publish_time = datetime(2025, 1, 15, 8, 0, tzinfo=timezone.utc)
    final_publish_time = stream_end_time
    mock_service_clock.now.side_effect = [initial_publish_time, final_publish_time]

    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.ServiceClock",
        return_value=mock_service_clock,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.ensure_streaming_topics_exist"
    )
    yellow_buffer = Mock()
    yellow_producer = Mock()
    green_buffer = Mock()
    green_producer = Mock()
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._build_streaming_components",
        side_effect=[
            (yellow_buffer, yellow_producer),
            (green_buffer, green_producer),
        ],
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._run_initial_buffering"
    )
    mock_publish_cycle = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._run_publish_cycle"
    )
    mocker.patch("nyc_taxi_data_service.serving.streaming.pipeline.time.sleep")

    run_streaming(resolved_runtime_config)

    assert mock_publish_cycle.call_args_list == [
        call(
            replay_producers={
                "yellow": yellow_producer,
                "green": green_producer,
            },
            mimic_current_time=initial_publish_time,
        ),
        call(
            replay_producers={
                "yellow": yellow_producer,
                "green": green_producer,
            },
            mimic_current_time=final_publish_time,
        ),
    ]


def test_run_streaming_closes_all_replay_producers_in_finally(
    mocker,
) -> None:
    """Close every replay producer during shutdown."""
    resolved_runtime_config = build_resolved_runtime_config_for_run_streaming()

    stream_end_time = datetime(2025, 2, 1, tzinfo=timezone.utc)
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._build_stream_exhaustion_boundary",
        return_value=stream_end_time,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._resolve_stream_timing_config",
        return_value=(timedelta(minutes=30), timedelta(seconds=5)),
    )

    mock_service_clock = Mock()
    mock_service_clock.mimic_service_start_time = datetime(
        2025, 1, 15, 8, 0, tzinfo=timezone.utc
    )
    mock_service_clock.now.side_effect = [
        datetime(2025, 1, 15, 8, 0, tzinfo=timezone.utc),
        stream_end_time,
    ]
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.ServiceClock",
        return_value=mock_service_clock,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.ensure_streaming_topics_exist"
    )

    yellow_producer = Mock()
    yellow_producer.close.return_value = 0
    green_producer = Mock()
    green_producer.close.return_value = 1

    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._build_streaming_components",
        side_effect=[
            (Mock(), yellow_producer),
            (Mock(), green_producer),
        ],
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._run_initial_buffering"
    )
    mocker.patch("nyc_taxi_data_service.serving.streaming.pipeline._run_publish_cycle")
    mocker.patch("nyc_taxi_data_service.serving.streaming.pipeline.time.sleep")

    run_streaming(resolved_runtime_config)

    yellow_producer.close.assert_called_once_with(timeout=10.0)
    green_producer.close.assert_called_once_with(timeout=10.0)


def test_run_streaming_buffers_additional_window_when_not_half_window_ahead(
    mocker,
) -> None:
    """Buffer another window when the next window start is not far enough ahead."""
    resolved_runtime_config = build_resolved_runtime_config_for_run_streaming()

    window_size = timedelta(minutes=30)
    publish_interval = timedelta(seconds=5)
    stream_end_time = datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)
    first_loop_mimic_time = datetime(2025, 1, 15, 8, 20, tzinfo=timezone.utc)

    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._build_stream_exhaustion_boundary",
        return_value=stream_end_time,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._resolve_stream_timing_config",
        return_value=(window_size, publish_interval),
    )

    mock_service_clock = Mock()
    mock_service_clock.mimic_service_start_time = datetime(
        2025, 1, 15, 8, 0, tzinfo=timezone.utc
    )
    mock_service_clock.now.side_effect = [
        datetime(2025, 1, 15, 8, 0, tzinfo=timezone.utc),  # initial publish
        first_loop_mimic_time,  # main loop iteration
        stream_end_time,  # next loop -> exit
    ]
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.ServiceClock",
        return_value=mock_service_clock,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.ensure_streaming_topics_exist"
    )

    window_buffer = Mock()
    # first loop iteration:
    #   08:30 - 08:20 = 10m <= 15m -> buffer once
    # second check:
    #   09:00 - 08:20 = 40m > 15m -> stop buffering
    window_buffer.get_current_window.side_effect = [
        (
            datetime(2025, 1, 15, 8, 30, tzinfo=timezone.utc),
            datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc),
        ),
        (
            datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc),
            datetime(2025, 1, 15, 9, 30, tzinfo=timezone.utc),
        ),
    ]
    replay_producer = Mock()

    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._build_streaming_components",
        return_value=(window_buffer, replay_producer),
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._run_initial_buffering"
    )
    mocker.patch("nyc_taxi_data_service.serving.streaming.pipeline._run_publish_cycle")
    mock_datetime = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.datetime"
    )
    mock_datetime.now.side_effect = [
        datetime(2025, 1, 15, 8, 0, tzinfo=timezone.utc),  # initial next_publish_at
        datetime(2025, 1, 15, 8, 1, tzinfo=timezone.utc),  # now inside loop
    ]
    mocker.patch("nyc_taxi_data_service.serving.streaming.pipeline.time.sleep")

    resolved_runtime_config["vehicle_types"] = ["yellow"]

    run_streaming(resolved_runtime_config)

    window_buffer.buffer_current_window.assert_called_once_with()


def test_run_streaming_does_not_buffer_when_next_window_is_far_enough_ahead(
    mocker,
) -> None:
    """Skip buffering when the next window start is more than half a window ahead."""
    resolved_runtime_config = build_resolved_runtime_config_for_run_streaming()

    window_size = timedelta(minutes=30)
    publish_interval = timedelta(seconds=5)
    stream_end_time = datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)
    first_loop_mimic_time = datetime(2025, 1, 15, 8, 10, tzinfo=timezone.utc)

    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._build_stream_exhaustion_boundary",
        return_value=stream_end_time,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._resolve_stream_timing_config",
        return_value=(window_size, publish_interval),
    )

    mock_service_clock = Mock()
    mock_service_clock.mimic_service_start_time = datetime(
        2025, 1, 15, 8, 0, tzinfo=timezone.utc
    )
    mock_service_clock.now.side_effect = [
        datetime(2025, 1, 15, 8, 0, tzinfo=timezone.utc),  # initial publish
        first_loop_mimic_time,  # main loop iteration
        stream_end_time,  # next loop -> exit
    ]
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.ServiceClock",
        return_value=mock_service_clock,
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.ensure_streaming_topics_exist"
    )

    window_buffer = Mock()
    # 08:30 - 08:10 = 20m > 15m -> should not buffer
    window_buffer.get_current_window.return_value = (
        datetime(2025, 1, 15, 8, 30, tzinfo=timezone.utc),
        datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc),
    )
    replay_producer = Mock()

    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._build_streaming_components",
        return_value=(window_buffer, replay_producer),
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline._run_initial_buffering"
    )
    mocker.patch("nyc_taxi_data_service.serving.streaming.pipeline._run_publish_cycle")
    mock_datetime = mocker.patch(
        "nyc_taxi_data_service.serving.streaming.pipeline.datetime"
    )
    mock_datetime.now.side_effect = [
        datetime(2025, 1, 15, 8, 0, tzinfo=timezone.utc),
        datetime(2025, 1, 15, 8, 1, tzinfo=timezone.utc),
    ]
    mocker.patch("nyc_taxi_data_service.serving.streaming.pipeline.time.sleep")

    resolved_runtime_config["vehicle_types"] = ["yellow"]

    run_streaming(resolved_runtime_config)

    window_buffer.buffer_current_window.assert_not_called()
