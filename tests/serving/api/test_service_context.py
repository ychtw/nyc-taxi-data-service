from nyc_taxi_data_service.serving.api.service_context import (
    build_service_context_payload,
)


def test_build_service_context_payload_returns_expected_structure() -> None:
    """
    build_service_context_payload() should return the expected API-facing structure
    given a complete resolved runtime config.
    """
    resolved_runtime_config = {
        "time_frame": {
            "start_year_month": "2025-01",
            "end_year_month": "2025-03",
        },
        "vehicle_types": ["yellow", "green", "fhv", "fhvhv"],
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_clock_anchor": {
                "real_service_start_time_utc": "2026-04-04T16:18:38+00:00",
                "mimic_service_start_time": "2025-01-15T12:18:38-05:00",
            },
            "service_event_time_column_by_vehicle_type": {
                "yellow": "tpep_dropoff_datetime",
                "green": "lpep_dropoff_datetime",
                "fhv": "dropOff_datetime",
                "fhvhv": "dropoff_datetime",
            },
        },
    }

    payload = build_service_context_payload(resolved_runtime_config)

    assert payload["time_frame"] == resolved_runtime_config["time_frame"]
    assert payload["vehicle_types"] == resolved_runtime_config["vehicle_types"]

    assert payload["serving"]["mimic_current_date"] == "2025-01-15"
    assert (
        payload["serving"]["service_clock_anchor"]
        == resolved_runtime_config["serving"]["service_clock_anchor"]
    )

    assert (
        payload["serving"]["service_event_time_column_by_vehicle_type"]
        == resolved_runtime_config["serving"][
            "service_event_time_column_by_vehicle_type"
        ]
    )


def test_build_service_context_payload_handles_missing_optional_anchor() -> None:
    """
    build_service_context_payload() should return None for service_clock_anchor
    when it is not present in the config.
    """
    resolved_runtime_config = {
        "time_frame": {
            "start_year_month": "2025-01",
            "end_year_month": "2025-03",
        },
        "vehicle_types": ["yellow"],
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_event_time_column_by_vehicle_type": {
                "yellow": "tpep_dropoff_datetime",
            },
        },
    }

    payload = build_service_context_payload(resolved_runtime_config)

    assert payload["serving"]["service_clock_anchor"] is None
