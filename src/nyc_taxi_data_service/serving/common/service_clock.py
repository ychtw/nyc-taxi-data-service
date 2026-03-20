from dataclasses import dataclass, field
from datetime import date, datetime, timezone


@dataclass
class ServiceClock:
    """
    ServiceClock provides the current service timestamp in mimic time.

    The clock records the real UTC timestamp when the service starts and the
    corresponding mimic service start timestamp. Current service time is then
    computed by adding real elapsed time since startup to the mimic start time.
    """

    mimic_current_date: date
    real_service_start_time: datetime = field(init=False)
    mimic_service_start_time: datetime = field(init=False)

    def __post_init__(self) -> None:
        """
        Initialize the real and mimic service start timestamps.
        """
        if self.mimic_current_date is None:
            raise ValueError("mimic_current_date must be set for ServiceClock")

        self.real_service_start_time = datetime.now(timezone.utc)
        self.mimic_service_start_time = datetime.combine(
            self.mimic_current_date,
            self.real_service_start_time.timetz(),
        )

    def now(self) -> datetime:
        """
        Return the current service datetime in mimic time.

        The returned value is calculated as:

            mimic_service_start_time + (current real UTC time - real_service_start_time)

        Returns:
            The current service datetime in mimic time.
        """
        real_current_utc = datetime.now(timezone.utc)
        elapsed = real_current_utc - self.real_service_start_time
        return self.mimic_service_start_time + elapsed
