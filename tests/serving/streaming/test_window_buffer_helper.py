from datetime import timedelta

import pytest

from nyc_taxi_data_service.serving.streaming.window_buffer import _parse_window_size


@pytest.mark.parametrize(
    ("window_size", "expected"),
    [
        ("1m", timedelta(minutes=1)),
        ("30m", timedelta(minutes=30)),
        ("2h", timedelta(hours=2)),
        ("1d", timedelta(days=1)),
    ],
)
def test_parse_window_size_accepts_valid_values(
    window_size: str,
    expected: timedelta,
) -> None:
    """Parse valid window-size strings into timedelta."""
    result = _parse_window_size(window_size)

    assert result == expected


@pytest.mark.parametrize(
    "window_size",
    [
        "90",
        "30s",
        "1hour",
        "30 m",
        " 30m",
        "2D",
    ],
)
def test_parse_window_size_rejects_invalid_format(window_size: str) -> None:
    """Reject window-size strings that do not match the supported format."""
    with pytest.raises(ValueError, match="Invalid window_size format"):
        _parse_window_size(window_size)


def test_parse_window_size_rejects_too_short_value() -> None:
    """Reject window sizes shorter than the minimum allowed duration."""
    with pytest.raises(ValueError, match="too short"):
        _parse_window_size("0m")


@pytest.mark.parametrize(
    "window_size",
    [
        "2d",
        "25h",
        "1441m",
    ],
)
def test_parse_window_size_rejects_too_long_values(window_size: str) -> None:
    """Reject window sizes longer than the maximum allowed duration."""
    with pytest.raises(ValueError, match="too long"):
        _parse_window_size(window_size)


@pytest.mark.parametrize("window_size", [30, None, 1.5, [], {}])
def test_parse_window_size_rejects_non_string_inputs(window_size: object) -> None:
    """Reject non-string input values."""
    with pytest.raises(ValueError, match="window_size must be a string"):
        _parse_window_size(window_size)
