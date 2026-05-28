from typing import Any

from nyc_taxi_data_service.serving.common.service_clock import ServiceClock


def build_service_context_payload(
    resolved_runtime_config: dict[str, Any], service_clock: ServiceClock | None = None
) -> dict[str, Any]:
    serving = resolved_runtime_config["serving"]

    payload = {
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

    if service_clock is not None:
        payload["serving"]["mimic_current_datetime"] = service_clock.now().isoformat()

    return payload
