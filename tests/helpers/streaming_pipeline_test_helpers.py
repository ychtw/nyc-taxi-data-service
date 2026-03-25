def build_minimal_resolved_runtime_config(
    window_size: str = "30m",
    publish_interval: str = "5s",
) -> dict:
    """Build a minimal resolved runtime config for stream timing tests."""
    return {
        "serving": {
            "streaming": {
                "buffer": {
                    "window_size": window_size,
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
