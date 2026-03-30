from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from nyc_taxi_data_service.serving.streaming.pipeline import (
    _build_stream_exhaustion_boundary,
    _parse_publish_interval,
    _resolve_stream_timing_config,
)
from tests.helpers.streaming_pipeline_test_helpers import (
    build_minimal_resolved_runtime_config,
    build_runtime_config_with_time_frame,
)

NY_TZ = ZoneInfo("America/New_York")


@pytest.mark.parametrize(
    ("publish_interval", "window_size", "expected"),
    [
        ("1s", timedelta(minutes=30), timedelta(seconds=1)),
        ("5s", timedelta(minutes=30), timedelta(seconds=5)),
        ("30s", timedelta(minutes=30), timedelta(seconds=30)),
        ("1m", timedelta(minutes=30), timedelta(minutes=1)),
        ("5m", timedelta(minutes=30), timedelta(minutes=5)),
        ("2h", timedelta(days=1), timedelta(hours=2)),
        ("1d", timedelta(days=1), timedelta(days=1)),
    ],
    ids=[
        "seconds-min-boundary",
        "seconds-small",
        "seconds-larger",
        "minutes-small",
        "minutes-larger",
        "hours",
        "days-equals-window-size",
    ],
)
def test_parse_publish_interval_valid_values(
    publish_interval: str,
    window_size: timedelta,
    expected: timedelta,
) -> None:
    """Parse supported compact duration strings into timedeltas."""
    assert _parse_publish_interval(publish_interval, window_size) == expected


@pytest.mark.parametrize(
    ("publish_interval", "expected_message"),
    [
        (None, "publish_interval must be a string"),
        (5, "publish_interval must be a string"),
        (1.5, "publish_interval must be a string"),
        (["5s"], "publish_interval must be a string"),
    ],
    ids=[
        "none",
        "int",
        "float",
        "list",
    ],
)
def test_parse_publish_interval_raises_for_non_string_input(
    publish_interval: object,
    expected_message: str,
) -> None:
    """Raise ValueError when publish_interval is not a string."""
    with pytest.raises(ValueError, match=expected_message):
        _parse_publish_interval(
            publish_interval=publish_interval,  # type: ignore[arg-type]
            window_size=timedelta(minutes=30),
        )


@pytest.mark.parametrize(
    "publish_interval",
    [
        "90",
        "1sec",
        " 5s",
        "5 s",
        "2S",
        "",
        "s",
        "1w",
        "-5s",
    ],
    ids=[
        "missing-unit",
        "long-unit",
        "leading-space",
        "embedded-space",
        "uppercase-unit",
        "empty-string",
        "missing-value",
        "unsupported-unit",
        "negative-format",
    ],
)
def test_parse_publish_interval_raises_for_invalid_format(
    publish_interval: str,
) -> None:
    """Raise ValueError when the duration string format is invalid."""
    with pytest.raises(ValueError, match="Invalid publish_interval format"):
        _parse_publish_interval(
            publish_interval=publish_interval,
            window_size=timedelta(minutes=30),
        )


def test_parse_publish_interval_raises_when_too_short() -> None:
    """Raise ValueError when parsed duration is shorter than 1 second."""
    with pytest.raises(
        ValueError,
        match="publish_interval is too short. Minimum allowed value is 1s.",
    ):
        _parse_publish_interval(
            publish_interval="0s",
            window_size=timedelta(minutes=30),
        )


@pytest.mark.parametrize(
    ("publish_interval", "window_size"),
    [
        ("31s", timedelta(seconds=30)),
        ("6m", timedelta(minutes=5)),
        ("2h", timedelta(hours=1)),
        ("2d", timedelta(days=1)),
    ],
    ids=[
        "seconds-exceeds-window",
        "minutes-exceeds-window",
        "hours-exceeds-window",
        "days-exceeds-window",
    ],
)
def test_parse_publish_interval_raises_when_greater_than_window_size(
    publish_interval: str,
    window_size: timedelta,
) -> None:
    """Raise ValueError when publish_interval exceeds window_size."""
    with pytest.raises(
        ValueError,
        match="publish_interval must be less than or equal to window_size.",
    ):
        _parse_publish_interval(
            publish_interval=publish_interval,
            window_size=window_size,
        )


@pytest.mark.parametrize(
    (
        "window_size",
        "retention",
        "publish_interval",
        "expected_window",
        "expected_retention",
        "expected_publish",
    ),
    [
        (
            "30m",
            "30m",
            "5s",
            timedelta(minutes=30),
            timedelta(minutes=30),
            timedelta(seconds=5),
        ),
        (
            "1h",
            "1h",
            "30s",
            timedelta(hours=1),
            timedelta(hours=1),
            timedelta(seconds=30),
        ),
        (
            "1d",
            "1d",
            "1h",
            timedelta(days=1),
            timedelta(days=1),
            timedelta(hours=1),
        ),
        (
            "15m",
            "15m",
            "15m",
            timedelta(minutes=15),
            timedelta(minutes=15),
            timedelta(minutes=15),
        ),
        (
            "15m",
            "30m",
            "5s",
            timedelta(minutes=15),
            timedelta(minutes=30),
            timedelta(seconds=5),
        ),
    ],
    ids=[
        "minutes-window-seconds-publish",
        "hours-window-seconds-publish",
        "days-window-hours-publish",
        "equal-window-and-publish",
        "retention-greater-than-window",
    ],
)
def test_resolve_stream_timing_config_parses_window_retention_and_publish_interval(
    window_size: str,
    retention: str,
    publish_interval: str,
    expected_window: timedelta,
    expected_retention: timedelta,
    expected_publish: timedelta,
) -> None:
    """Resolve and parse all streaming timing settings from runtime config."""
    resolved_runtime_config = build_minimal_resolved_runtime_config(
        window_size=window_size,
        retention=retention,
        publish_interval=publish_interval,
    )

    actual_window, actual_retention, actual_publish = _resolve_stream_timing_config(
        resolved_runtime_config
    )

    assert actual_window == expected_window
    assert actual_retention == expected_retention
    assert actual_publish == expected_publish


@pytest.mark.parametrize(
    "window_size",
    [
        "0m",
        "30s",
        "1w",
        "bad",
        "",
    ],
    ids=[
        "too-short",
        "seconds-not-supported",
        "unsupported-unit",
        "invalid-format",
        "empty-string",
    ],
)
def test_resolve_stream_timing_config_raises_for_invalid_window_size(
    window_size: str,
) -> None:
    """Raise ValueError when window_size cannot be parsed or validated."""
    resolved_runtime_config = build_minimal_resolved_runtime_config(
        window_size=window_size,
        retention="30m",
        publish_interval="5s",
    )

    with pytest.raises(ValueError):
        _resolve_stream_timing_config(resolved_runtime_config)


@pytest.mark.parametrize(
    "retention",
    [
        "0m",
        "30s",
        "1w",
        "bad",
        "",
    ],
    ids=[
        "too-short",
        "seconds-not-supported",
        "unsupported-unit",
        "invalid-format",
        "empty-string",
    ],
)
def test_resolve_stream_timing_config_raises_for_invalid_retention(
    retention: str,
) -> None:
    """Raise ValueError when retention cannot be parsed or validated."""
    resolved_runtime_config = build_minimal_resolved_runtime_config(
        window_size="30m",
        retention=retention,
        publish_interval="5s",
    )

    with pytest.raises(ValueError):
        _resolve_stream_timing_config(resolved_runtime_config)


def test_resolve_stream_timing_config_parses_retention_even_when_shorter_than_window_size() -> (
    None
):
    """Parse timing values without enforcing the retention/window_size relationship."""
    resolved_runtime_config = build_minimal_resolved_runtime_config(
        window_size="30m",
        retention="15m",
        publish_interval="5s",
    )

    actual_window, actual_retention, actual_publish = _resolve_stream_timing_config(
        resolved_runtime_config
    )

    assert actual_window == timedelta(minutes=30)
    assert actual_retention == timedelta(minutes=15)
    assert actual_publish == timedelta(seconds=5)


@pytest.mark.parametrize(
    "publish_interval",
    [
        "0s",
        "bad",
        "31m",
        "",
    ],
    ids=[
        "too-short",
        "invalid-format",
        "greater-than-window-size",
        "empty-string",
    ],
)
def test_resolve_stream_timing_config_raises_for_invalid_publish_interval(
    publish_interval: str,
) -> None:
    """Raise ValueError when publish_interval cannot be parsed or validated."""
    resolved_runtime_config = build_minimal_resolved_runtime_config(
        window_size="30m",
        retention="30m",
        publish_interval=publish_interval,
    )

    with pytest.raises(ValueError):
        _resolve_stream_timing_config(resolved_runtime_config)


@pytest.mark.parametrize(
    ("end_year_month", "expected"),
    [
        ("2025-01", datetime(2025, 2, 1, tzinfo=NY_TZ)),
        ("2025-03", datetime(2025, 4, 1, tzinfo=NY_TZ)),
        ("2025-11", datetime(2025, 12, 1, tzinfo=NY_TZ)),
    ],
    ids=[
        "january-rollover",
        "march-rollover",
        "november-rollover",
    ],
)
def test_build_stream_exhaustion_boundary_returns_first_day_of_next_month(
    end_year_month: str,
    expected: datetime,
) -> None:
    """Return the first instant of the following month in America/New_York."""
    resolved_runtime_config = build_runtime_config_with_time_frame(
        end_year_month=end_year_month
    )

    actual = _build_stream_exhaustion_boundary(resolved_runtime_config)

    assert actual == expected


def test_build_stream_exhaustion_boundary_rolls_december_into_next_year() -> None:
    """Return January 1 of the next year when end_year_month is December."""
    resolved_runtime_config = build_runtime_config_with_time_frame(
        end_year_month="2025-12"
    )

    actual = _build_stream_exhaustion_boundary(resolved_runtime_config)

    assert actual == datetime(2026, 1, 1, tzinfo=NY_TZ)


def test_build_stream_exhaustion_boundary_returns_timezone_aware_new_york_datetime() -> (
    None
):
    """Return a timezone-aware datetime in America/New_York."""
    resolved_runtime_config = build_runtime_config_with_time_frame(
        end_year_month="2025-03"
    )

    actual = _build_stream_exhaustion_boundary(resolved_runtime_config)

    assert actual.tzinfo == NY_TZ


@pytest.mark.parametrize(
    "end_year_month",
    [
        None,
        202503,
        2025.03,
        ["2025-03"],
    ],
    ids=[
        "none",
        "int",
        "float",
        "list",
    ],
)
def test_build_stream_exhaustion_boundary_raises_for_non_string_input(
    end_year_month: object,
) -> None:
    """Raise ValueError when end_year_month is not a string."""
    resolved_runtime_config = build_runtime_config_with_time_frame(
        end_year_month=end_year_month
    )

    with pytest.raises(
        ValueError,
        match="time_frame.end_year_month must be a string in 'YYYY-MM' format.",
    ):
        _build_stream_exhaustion_boundary(resolved_runtime_config)


@pytest.mark.parametrize(
    "end_year_month",
    [
        "",
        "2025-3",
        "25-03",
        "2025/03",
        "202503",
        "bad",
    ],
    ids=[
        "empty-string",
        "single-digit-month",
        "short-year",
        "slash-separated",
        "missing-dash",
        "arbitrary-string",
    ],
)
def test_build_stream_exhaustion_boundary_raises_for_invalid_format(
    end_year_month: str,
) -> None:
    """Raise ValueError when end_year_month is not in YYYY-MM format."""
    resolved_runtime_config = build_runtime_config_with_time_frame(
        end_year_month=end_year_month
    )

    with pytest.raises(
        ValueError,
        match="Invalid time_frame.end_year_month format. Expected 'YYYY-MM'.",
    ):
        _build_stream_exhaustion_boundary(resolved_runtime_config)


@pytest.mark.parametrize(
    "end_year_month",
    [
        "2025-00",
        "2025-13",
        "2025-99",
    ],
    ids=[
        "month-zero",
        "month-thirteen",
        "month-ninety-nine",
    ],
)
def test_build_stream_exhaustion_boundary_raises_for_invalid_month_value(
    end_year_month: str,
) -> None:
    """Raise ValueError when the parsed month is outside 01-12."""
    resolved_runtime_config = build_runtime_config_with_time_frame(
        end_year_month=end_year_month
    )

    with pytest.raises(
        ValueError,
        match="Invalid time_frame.end_year_month value. Month must be between 01 and 12.",
    ):
        _build_stream_exhaustion_boundary(resolved_runtime_config)
