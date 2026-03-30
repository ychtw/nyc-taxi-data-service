from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo


@dataclass
class ServiceClock:
    """
    ServiceClock provides the current service timestamp in mimic New York time.

    The clock models the data service as if it were operating in the
    `America/New_York` timezone, regardless of where the host machine runs.

    At startup, the clock:
    - captures the real service start time in UTC
    - converts that real start time into New York local time
    - combines the configured `mimic_current_date` with that New York local
      wall-clock time to form `mimic_service_start_time`

    After startup, the current mimic service time is computed by adding the
    real elapsed time since startup to the mimic service start time.

    This design keeps elapsed-time tracking stable in UTC while exposing
    service-facing timestamps in New York local time, which matches the
    semantics of NYC TLC event-time columns.
    """

    mimic_current_date: date
    real_service_start_time: datetime = field(init=False)
    mimic_service_start_time: datetime = field(init=False)

    def __post_init__(self) -> None:
        """
        Initialize the real and mimic service start timestamps.

        Raises:
            ValueError: If `mimic_current_date` is not provided.
        """
        if self.mimic_current_date is None:
            raise ValueError("mimic_current_date must be set for ServiceClock")

        # capture the real startup timestamp in UTC so elapsed-time tracking
        # is independent of the host machine's local timezone.
        self.real_service_start_time = datetime.now(timezone.utc)

        # convert the real startup timestamp into New York local time and use
        # its wall-clock time together with the configured mimic date.
        self.mimic_service_start_time = datetime.combine(
            self.mimic_current_date,
            self.real_service_start_time.astimezone(
                ZoneInfo("America/New_York")
            ).timetz(),
        )

    def now(self) -> datetime:
        """
        Return the current service datetime in mimic New York time.

        The returned value is calculated as:

            mimic_service_start_time + (current real UTC time - real_service_start_time)

        Returns:
            A timezone-aware datetime in `America/New_York` representing the
            current mimic service time.
        """
        real_current_utc = datetime.now(timezone.utc)
        elapsed: timedelta = real_current_utc - self.real_service_start_time
        return self.mimic_service_start_time + elapsed
