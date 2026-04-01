import logging
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from nyc_taxi_data_service.serving.common.service_clock import ServiceClock

logger = logging.getLogger(__name__)


NY_TZ = ZoneInfo("America/New_York")


def _build_service_clock(resolved_runtime_config: dict) -> ServiceClock:
    """
    Build the ServiceClock from resolved runtime config.

    Behavior:
    - If serving.service_clock_anchor is present:
        Reconstruct the clock from a shared startup context (e.g. initializer output).
    - Otherwise:
        Start a fresh standalone session using the current system time.
    """

    serving_config = resolved_runtime_config["serving"]

    # parse mimic_current_date from ISO string (YYYY-MM-DD)
    # (this represents the "logical current date" of the service)
    mimic_current_date = date.fromisoformat(serving_config["mimic_current_date"])

    service_clock_anchor = serving_config.get("service_clock_anchor")

    if service_clock_anchor is None:
        # standalone mode: build a new ServiceClock anchored at the current real time
        logger.info("Build ServiceClock from new session")
        return ServiceClock.from_new_session(mimic_current_date=mimic_current_date)

    # coordinated mode (e.g. Docker Compose with initializer):
    # reconstruct the ServiceClock using persisted anchor values

    # parse the real service start time from ISO string
    # (even if the string already contains a UTC offset, explicitly normalize it
    # to timezone.utc to ensure exact tzinfo equality required by validation)
    real_service_start_time = datetime.fromisoformat(
        service_clock_anchor["real_service_start_time_utc"]
    ).astimezone(timezone.utc)

    # parse the mimic service start time from ISO string
    #
    # note:
    # datetime.fromisoformat(...) will produce a datetime with a fixed-offset
    # timezone (e.g. -05:00), NOT ZoneInfo("America/New_York").
    #
    # however, ServiceClock validation requires the tzinfo to be exactly
    # ZoneInfo("America/New_York"), not just an equivalent offset
    #
    # therefore we must normalize it explicitly using astimezone(NY_TZ),
    # which converts the datetime into the correct ZoneInfo-based tzinfo
    mimic_service_start_time = datetime.fromisoformat(
        service_clock_anchor["mimic_service_start_time"]
    ).astimezone(NY_TZ)

    logger.info("Build ServiceClock from existing anchor")

    return ServiceClock.from_existing_anchor(
        mimic_current_date=mimic_current_date,
        real_service_start_time=real_service_start_time,
        mimic_service_start_time=mimic_service_start_time,
    )
