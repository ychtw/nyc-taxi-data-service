import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from nyc_taxi_data_service.serving.common.log_time import format_log_dt
from nyc_taxi_data_service.serving.common.service_clock_loader import (
    _build_service_clock,
)
from nyc_taxi_data_service.serving.common.startup_context_loader import (
    load_serving_runtime_config,
)
from nyc_taxi_data_service.serving.streaming.kafka_admin import (
    ensure_streaming_topics_exist,
    wait_for_kafka_ready,
)
from nyc_taxi_data_service.serving.streaming.kafka_producer import KafkaReplayProducer
from nyc_taxi_data_service.serving.streaming.window_buffer import (
    StreamingWindowBuffer,
    _parse_buffer_duration,
)
from nyc_taxi_data_service.utils.log_util import setup_logging

logger = logging.getLogger(__name__)


def _parse_publish_interval(
    publish_interval: str,
    window_size: timedelta,
) -> timedelta:
    """
    Parse and validate a Kafka publish-interval string into a timedelta.

    The runtime config stores the publish interval as a compact duration string
    such as "1s", "5s", "30s", "1m", or "5m". This helper validates the
    format, converts the value to a timedelta, and enforces project-level
    bounds for Kafka replay frequency.

    Supported format:
    - one or more digits, followed immediately by a unit
    - allowed units:
        - s: seconds
        - m: minutes
        - h: hours
        - d: days

    Examples of valid values:
    - "1s"
    - "5s"
    - "30s"
    - "1m"
    - "5m"

    Examples of invalid values:
    - "0s"
    - "90"
    - "1sec"
    - " 5s"
    - "5 s"
    - "2S"

    Validation rules:
    - publish_interval must be a string in the supported compact format
    - the effective duration must be at least 1 second
    - the effective duration must be no more than the configured window size

    Args:
        publish_interval: Publish-interval string from runtime config, such as
            "5s", "30s", or "1m".
        window_size: Parsed window-size timedelta used as the upper bound
            for publish_interval.

    Returns:
        A timedelta representing the validated publish interval.

    Raises:
        ValueError:
            - if publish_interval is not a string
            - if the format is invalid
            - if the unit is unsupported
            - if the parsed duration is shorter than 1 second
            - if the parsed duration is longer than `window_size`
    """
    if not isinstance(publish_interval, str):
        raise ValueError(
            f"publish_interval must be a string, got {type(publish_interval).__name__}"
        )

    match = re.fullmatch(r"(\d+)([smhd])", publish_interval)
    if match is None:
        raise ValueError(
            "Invalid publish_interval format. Expected a compact duration like "
            "'1s', '5s', '30s', '1m', or '1h'. "
            "Allowed units are: s (seconds), m (minutes), h (hours), d (days)."
        )

    value_str, unit = match.groups()
    value = int(value_str)

    if unit == "s":
        duration = timedelta(seconds=value)
    elif unit == "m":
        duration = timedelta(minutes=value)
    elif unit == "h":
        duration = timedelta(hours=value)
    else:  # unit == "d"
        duration = timedelta(days=value)

    min_publish_interval = timedelta(seconds=1)

    if duration < min_publish_interval:
        raise ValueError("publish_interval is too short. Minimum allowed value is 1s.")

    if duration > window_size:
        raise ValueError("publish_interval must be less than or equal to window_size.")

    return duration


def _resolve_stream_timing_config(
    resolved_runtime_config: dict,
) -> tuple[timedelta, timedelta, timedelta]:
    """
    Resolve window size, retention, and publish interval from the runtime config.

    This helper reads streaming buffer and Kafka timing settings from the
    resolved runtime config, parses them into timedeltas, and validates that
    the publish interval is within the allowed bounds for the configured
    window size.

    Args:
        resolved_runtime_config: Fully resolved runtime config dictionary.

    Returns:
        A tuple of:
            (window_size, retention, publish_interval)
    """

    buffer_config = resolved_runtime_config["serving"]["streaming"]["buffer"]
    kafka_config = resolved_runtime_config["serving"]["streaming"]["kafka"]

    logger.info(
        "Streaming timing config | window_size=%s | retention=%s | publish_interval=%s",
        buffer_config["window_size"],
        buffer_config["retention"],
        kafka_config["publish_interval"],
    )

    window_size = _parse_buffer_duration(
        raw_value=buffer_config["window_size"], field_name="buffer.window_size"
    )
    retention = _parse_buffer_duration(
        raw_value=buffer_config["retention"], field_name="buffer.retention"
    )
    publish_interval = _parse_publish_interval(
        publish_interval=kafka_config["publish_interval"],
        window_size=window_size,
    )

    return window_size, retention, publish_interval


def _build_streaming_components(
    vehicle_type: str,
    resolved_runtime_config: dict,
    mimic_service_start_time: datetime,
) -> tuple[StreamingWindowBuffer, KafkaReplayProducer]:
    """
    Create the streaming buffer and replay producer for one vehicle type.

    This helper initializes the per-vehicle StreamingWindowBuffer and
    KafkaReplayProducer using the shared runtime config and the service
    start time derived from the ServiceClock.

    Args:
        vehicle_type: Vehicle type being served (e.g. "yellow", "green").
        resolved_runtime_config: Fully resolved runtime config dictionary.
        mimic_service_start_time: Mimic timestamp corresponding to service
            startup, used to align buffering and publishing windows.

    Returns:
        A tuple of:
            (window_buffer, replay_producer)
    """
    logger.info("Build streaming components | vehicle_type=%s", vehicle_type)

    window_buffer = StreamingWindowBuffer(
        vehicle_type=vehicle_type,
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    replay_producer = KafkaReplayProducer(
        vehicle_type=vehicle_type,
        resolved_runtime_config=resolved_runtime_config,
        mimic_service_start_time=mimic_service_start_time,
    )

    return window_buffer, replay_producer


def _run_initial_buffering(
    window_buffers: dict[str, StreamingWindowBuffer],
) -> None:
    """
    Perform the initial buffer fill for all vehicle types.

    Each StreamingWindowBuffer loads its first window of data so that the
    replay producers can begin publishing immediately after startup.
    """
    logger.info("Initial buffering")

    for window_buffer in window_buffers.values():
        window_buffer.buffer_current_window()


def _run_publish_cycle(
    replay_producers: dict[str, KafkaReplayProducer],
    mimic_current_time: datetime,
) -> None:
    """
    Publish all buffered rows that are eligible at the current mimic time.

    Each KafkaReplayProducer checks its buffer and sends any rows whose
    event time is less than or equal to the provided mimic timestamp.
    """
    logger.debug(
        "Publish cycle | mimic_current_time=%s | vehicle_types=%s",
        format_log_dt(mimic_current_time),
        list(replay_producers),
    )
    for replay_producer in replay_producers.values():
        replay_producer.publish_available(mimic_current_time=mimic_current_time)


def _build_stream_exhaustion_boundary(
    resolved_runtime_config: dict,
) -> datetime:
    """
    Build the exclusive upper bound for streamable event time.

    The runtime config defines the available source range at month granularity
    via `time_frame.end_year_month` in "YYYY-MM" format. This helper converts
    that month into the first instant of the following month in
    `America/New_York`, which acts as the exclusive upper bound for event
    times that may appear in the stream.

    Example:
        end_year_month = "2025-03"
        -> boundary = 2025-04-01 00:00:00-04:00

    Args:
        resolved_runtime_config: Fully resolved runtime config dictionary.

    Returns:
        A timezone-aware `America/New_York` datetime representing the exclusive
        upper bound of streamable event time.
    """
    end_year_month = resolved_runtime_config["time_frame"]["end_year_month"]

    if not isinstance(end_year_month, str):
        raise ValueError(
            "time_frame.end_year_month must be a string in 'YYYY-MM' format."
        )

    match = re.fullmatch(r"(\d{4})-(\d{2})", end_year_month)
    if match is None:
        raise ValueError(
            "Invalid time_frame.end_year_month format. Expected 'YYYY-MM'."
        )

    year = int(match.group(1))
    month = int(match.group(2))

    if not 1 <= month <= 12:
        raise ValueError(
            "Invalid time_frame.end_year_month value. Month must be between 01 and 12."
        )

    service_tz = ZoneInfo("America/New_York")

    if month == 12:
        return datetime(year + 1, 1, 1, tzinfo=service_tz)

    return datetime(year, month + 1, 1, tzinfo=service_tz)


def run_streaming(resolved_runtime_config: dict) -> None:
    """
    Run the streaming service main loop.

    The service:
    - creates a shared ServiceClock
    - ensures Kafka topics exist
    - builds one window buffer and one replay producer per vehicle type
    - performs initial buffering and publishing
    - keeps buffering ahead and publishing on schedule
    - stops when the configured source time range is exhausted

    On shutdown, all replay producers are closed so queued Kafka messages
    are flushed before exit.
    """
    logger.info("Starting NYC taxi data streaming service")

    # build the upper bound time (exclusive) of streaming service
    stream_end_time = _build_stream_exhaustion_boundary(resolved_runtime_config)
    logger.debug(
        "Streaming upper bound (exclusive): %s", format_log_dt(stream_end_time)
    )

    # get 2 timing controls used by the streaming service:
    # - window_size: how much source data each buffer-fill query stages into DuckDB
    # - publish_interval: how often buffered rows are replayed from DuckDB to Kafka
    window_size, retention, publish_interval = _resolve_stream_timing_config(
        resolved_runtime_config=resolved_runtime_config
    )
    publish_interval_seconds = publish_interval.total_seconds()

    # mimic_current_date anchors the service's mimic timeline
    mimic_current_date = date.fromisoformat(
        resolved_runtime_config["serving"]["mimic_current_date"]
    )
    logger.info("Mimic current date: %s", mimic_current_date)

    # create one shared `ServiceClock` for the whole streaming service
    # & derive mimic_service_start_time
    service_clock = _build_service_clock(resolved_runtime_config)
    mimic_service_start_time = service_clock.mimic_service_start_time
    logger.info(
        "Service clock started | real_service_start_time=%s | mimic_service_start_time=%s",
        format_log_dt(service_clock.real_service_start_time),
        format_log_dt(mimic_service_start_time),
    )

    # configure vehicle types for this streaming run
    vehicle_types = resolved_runtime_config["vehicle_types"]
    logger.info("Configured vehicle types: %s", vehicle_types)

    # create dictionaries to hold per-vehicle runtime objects
    # initialize these before `try` so `finally` can always access them,
    # even if startup fails before all components are created
    window_buffers: dict[str, StreamingWindowBuffer] = {}
    replay_producers: dict[str, KafkaReplayProducer] = {}

    try:
        # wait until Kafka is actually ready to respond before running
        # topic-admin startup steps
        # (docker container start order alone does not guarantee Kafka readiness)
        wait_for_kafka_ready(
            resolved_runtime_config=resolved_runtime_config,
            timeout_seconds=60.0,
            retry_interval_seconds=2.0,
            metadata_timeout_seconds=5.0,
        )

        # ensure all required kafka topics exist before creating replay producers
        ensure_streaming_topics_exist(
            vehicle_types=vehicle_types, resolved_runtime_config=resolved_runtime_config
        )

        # for each vehicle type:
        #   - create a StreamingWindowBuffer
        #   - create a KafkaReplayProducer
        for vehicle_type in vehicle_types:
            window_buffers[vehicle_type], replay_producers[vehicle_type] = (
                _build_streaming_components(
                    vehicle_type=vehicle_type,
                    resolved_runtime_config=resolved_runtime_config,
                    mimic_service_start_time=mimic_service_start_time,
                )
            )

        # perform initial buffer fill & initial publish pass
        # this lets the service publish any buffered rows that are
        # already eligible at the current mimic time
        _run_initial_buffering(window_buffers=window_buffers)
        logger.info("Initial publish")
        _run_publish_cycle(
            replay_producers=replay_producers,
            mimic_current_time=service_clock.now(),
        )

        # schedule next publish attempt using real time
        # note: this scheduler timestamp is independent of mimic event time
        #   - real time decides "when" the loop wakes up to publish
        #   - mimic time decides "which" records are eligible to publish
        next_publish_at = datetime.now(timezone.utc) + publish_interval

        # run main service loop until source data is exhausted or an exception causes shutdown
        while True:
            # sample the current mimic service time, which serves as the anchor for this loop iteration
            mimic_current_time = service_clock.now()

            # stop service loop once mimic time has reached the end boundary
            if mimic_current_time >= stream_end_time:
                logger.info(
                    "Source data time range exhausted; running final publish cycle before shutdown."
                )
                # run one final publish cycle so any rows that became eligible up to
                # this final mimic timestamp get a chance to be sent to kafka
                _run_publish_cycle(
                    replay_producers=replay_producers,
                    mimic_current_time=mimic_current_time,
                )
                break

            # for each vehicle window buffer, decide whether the next window should be buffered
            for window_buffer in window_buffers.values():
                # use while loop to handle the case when we fall behind by > 1 window_size interval
                while True:
                    window_start, _ = window_buffer.get_current_window()

                    # keep the buffer at least half a window ahead of current mimic time
                    if window_start - mimic_current_time > window_size / 2:
                        # next window start is more than half a window in the future, skip buffering
                        break
                    logger.debug(
                        "Buffer-ahead check triggered buffering | vehicle_type=%s | mimic_current_time=%s | next_window_start=%s",
                        window_buffer.vehicle_type,
                        format_log_dt(mimic_current_time),
                        format_log_dt(window_start),
                    )
                    window_buffer.buffer_current_window()
                    # remove rows older than retention to prevent unbounded buffer growth
                    window_buffer.delete_expired_rows(
                        mimic_current_time=mimic_current_time
                    )

            # publish buffered rows on schedule
            now = datetime.now(timezone.utc)
            if now >= next_publish_at:
                logger.debug(
                    "Scheduled publish triggered | now=%s | next_publish_at=%s | mimic_current_time=%s",
                    format_log_dt(now),
                    format_log_dt(next_publish_at),
                    format_log_dt(mimic_current_time),
                )

                # note: one publish cycle already attempts to send all rows eligible up to `mimic_current_time`
                _run_publish_cycle(
                    replay_producers=replay_producers,
                    mimic_current_time=mimic_current_time,
                )
                # set next publish time to be `publish_interval` later
                next_publish_at = now + publish_interval

            # sleep briefly before the next service loop iteration
            # the sleep duration is capped so the loop wakes up often enough to:
            # - notice the next publish deadline
            # - continue buffering ahead in a timely way
            sleep_seconds = max(
                0.1,  # avoids negative or near-zero sleep durations
                min(
                    (next_publish_at - now).total_seconds(),
                    publish_interval_seconds / 2,
                ),
            )
            # logger.debug("Sleep %.3f seconds", sleep_seconds)
            time.sleep(sleep_seconds)

    finally:
        logger.info("Shutting down streaming service.")

        # close each kafka replay producer so queued messages are flushed before exit
        for vehicle_type, replay_producer in replay_producers.items():
            try:
                remaining = replay_producer.close(timeout=10.0)
                logger.info(
                    "Closed replay producer | vehicle_type=%s | remaining_messages=%s",
                    vehicle_type,
                    remaining,
                )
            except Exception:
                logger.exception(
                    "Failed to close replay producer cleanly | vehicle_type=%s",
                    vehicle_type,
                )


def main() -> None:
    """
    Entry point for the streaming service.

    Loads runtime config, configures logging, and starts the streaming loop.
    """
    # load runtime config:
    # - with SERVICE_STARTUP_CONTEXT_PATH env variable -> use shared startup context JSON
    # - otherwise -> resolve runtime.yaml (standalone mode)
    resolved_runtime_config = load_serving_runtime_config()

    # set up logging based on runtime config setting
    log_level = resolved_runtime_config.get("logging", {}).get("level", "INFO")
    setup_logging(level=log_level)

    # run streaming pipeline
    run_streaming(resolved_runtime_config)


if __name__ == "__main__":
    main()
