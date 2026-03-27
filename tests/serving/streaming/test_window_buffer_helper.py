from datetime import timedelta

import pytest

from nyc_taxi_data_service.serving.streaming.window_buffer import _parse_buffer_duration


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("1m", timedelta(minutes=1)),
        ("30m", timedelta(minutes=30)),
        ("2h", timedelta(hours=2)),
        ("1d", timedelta(days=1)),
    ],
)
def test_parse_buffer_duration_accepts_valid_values(
    raw_value: str,
    expected: timedelta,
) -> None:
    """Parse valid buffer duration strings into timedelta."""
    result = _parse_buffer_duration(
        raw_value=raw_value, field_name="buffer.window_size"
    )

    assert result == expected


@pytest.mark.parametrize(
    "raw_value",
    [
        "90",
        "30s",
        "1hour",
        "30 m",
        " 30m",
        "2D",
    ],
)
def test_parse_buffer_duration_rejects_invalid_format(raw_value: str) -> None:
    """Reject buffer duration strings that do not match the supported format."""
    with pytest.raises(ValueError, match="Invalid buffer.window_size format"):
        _parse_buffer_duration(raw_value=raw_value, field_name="buffer.window_size")


def test_parse_buffer_duration_rejects_too_short_value() -> None:
    """Reject retention values shorter than the minimum allowed duration."""
    with pytest.raises(ValueError, match="buffer.retention.*too short"):
        _parse_buffer_duration(raw_value="0m", field_name="buffer.retention")


@pytest.mark.parametrize(
    "raw_value",
    [
        "2d",
        "25h",
        "1441m",
    ],
)
def test_parse_buffer_duration_rejects_too_long_values(raw_value: str) -> None:
    """Reject buffer duration longer than the maximum allowed duration."""
    with pytest.raises(ValueError, match="too long"):
        _parse_buffer_duration(raw_value=raw_value, field_name="buffer.window_size")


@pytest.mark.parametrize("raw_value", [30, None, 1.5, [], {}])
def test_parse_buffer_duration_rejects_non_string_inputs(raw_value: object) -> None:
    """Reject non-string input values."""
    with pytest.raises(ValueError, match="buffer.retention must be a string"):
        _parse_buffer_duration(raw_value=raw_value, field_name="buffer.retention")
