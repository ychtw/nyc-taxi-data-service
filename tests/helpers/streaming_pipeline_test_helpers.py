from datetime import datetime, timezone
from unittest.mock import Mock
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")


def build_minimal_resolved_runtime_config(
    window_size: str = "30m",
    retention: str = "30m",
    publish_interval: str = "5s",
) -> dict:
    """Build a minimal resolved runtime config for stream timing tests."""
    return {
        "serving": {
            "streaming": {
                "buffer": {
                    "window_size": window_size,
                    "retention": retention,
                },
                "kafka": {
                    "publish_interval": publish_interval,
                },
            }
        }
    }


def build_runtime_config_with_time_frame(
    end_year_month: object,
) -> dict:
    """Build minimal resolved config with time_frame settings."""
    return {
        "time_frame": {
            "end_year_month": end_year_month,
        }
    }


def build_resolved_runtime_config_for_run_streaming() -> dict:
    """Build a minimal resolved runtime config for run_streaming tests."""
    return {
        "serving": {
            "mimic_current_date": "2025-01-15",
        },
        "vehicle_types": ["yellow", "green"],
    }


def build_mock_service_clock(now_side_effect: list[datetime]) -> Mock:
    """
    Build a mocked ServiceClock with the attributes used by run_streaming().

    The streaming pipeline now reads both:
    - real_service_start_time
    - mimic_service_start_time
    and calls now() multiple times during startup and the main loop.
    """
    mock_service_clock = Mock()
    mock_service_clock.real_service_start_time = datetime(
        2026, 3, 18, 13, 0, tzinfo=timezone.utc
    )
    mock_service_clock.mimic_service_start_time = datetime(
        2025, 1, 15, 8, 0, tzinfo=NY_TZ
    )
    mock_service_clock.now.side_effect = now_side_effect
    return mock_service_clock
