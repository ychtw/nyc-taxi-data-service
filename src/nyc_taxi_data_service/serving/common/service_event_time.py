def get_service_event_time_column(
    vehicle_type: str,
    serving_config: dict[str],
) -> str:
    """
    Return the configured service event-time column for a vehicle type.

    The service event-time column is the timestamp column used to compare each
    trip record against the mimic current datetime. This comparison determines
    whether a record belongs to the historic/API side or the real-time/streaming
    side of the service.

    Args:
        vehicle_type: Vehicle type such as "yellow", "green", "fhv", or "fhvhv".
        serving_config: Resolved serving config containing
            "service_event_time_column_by_vehicle_type".

    Returns:
        The configured event-time column name for the given vehicle type.

    Raises:
        ValueError: If the serving config does not include a mapping for the
            requested vehicle type.
    """
    mapping = serving_config["service_event_time_column_by_vehicle_type"]

    try:
        return mapping[vehicle_type]
    except KeyError as e:
        raise ValueError(
            f"Missing service event-time column mapping for vehicle_type: {vehicle_type}"
        ) from e
