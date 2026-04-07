from typing import Any


def build_service_context_payload(
    resolved_runtime_config: dict[str, Any],
) -> dict[str, Any]:
    serving = resolved_runtime_config["serving"]

    return {
        "time_frame": resolved_runtime_config["time_frame"],
        "vehicle_types": resolved_runtime_config["vehicle_types"],
        "serving": {
            "mimic_current_date": serving["mimic_current_date"],
            "service_clock_anchor": serving.get("service_clock_anchor"),
            "service_event_time_column_by_vehicle_type": (
                serving["service_event_time_column_by_vehicle_type"]
            ),
        },
    }
