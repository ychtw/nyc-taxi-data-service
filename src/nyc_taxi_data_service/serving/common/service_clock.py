from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

NY_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True, init=False)
class ServiceClock:
    """
    ServiceClock provides the current service timestamp in mimic New York time.

    The clock models the data service as if it were operating in the
    `America/New_York` timezone, regardless of where the host machine runs.

    A ServiceClock is defined by:
    - `mimic_current_date`
    - `real_service_start_time` (in UTC)
    - `mimic_service_start_time` (in New York local time)

    The current mimic service time is computed by adding real elapsed time
    since service startup to the mimic service start time.
    """

    mimic_current_date: date
    real_service_start_time: datetime
    mimic_service_start_time: datetime

    def __init__(
        self,
        mimic_current_date: date,
        real_service_start_time: datetime,
        mimic_service_start_time: datetime,
    ) -> None:
        """
        Initialize a ServiceClock from validated anchor values.

        Prefer using:
        - `ServiceClock.from_new_session(...)`
        - `ServiceClock.from_existing_anchor(...)`
        """
        self._validate_inputs(
            mimic_current_date=mimic_current_date,
            real_service_start_time=real_service_start_time,
            mimic_service_start_time=mimic_service_start_time,
        )

        object.__setattr__(self, "mimic_current_date", mimic_current_date)
        object.__setattr__(self, "real_service_start_time", real_service_start_time)
        object.__setattr__(self, "mimic_service_start_time", mimic_service_start_time)

    @classmethod
    def from_new_session(cls, mimic_current_date: date) -> ServiceClock:
        """
        Build a ServiceClock for a newly started service session.

        This constructor captures the real service start time in UTC, converts
        that timestamp into New York local time, and combines the configured
        `mimic_current_date` with that New York wall-clock time to produce
        `mimic_service_start_time`.

        Args:
            mimic_current_date:
                The date that the service should mimic in New York time.

        Returns:
            A ServiceClock initialized for a new service session.

        Raises:
            ValueError:
                If `mimic_current_date` is not provided.
        """
        if mimic_current_date is None:
            raise ValueError("mimic_current_date must be set for ServiceClock")

        # capture the real startup timestamp in UTC so elapsed-time tracking
        # is independent of the host machine's local timezone.
        real_service_start_time = datetime.now(timezone.utc)

        # convert the real startup timestamp into New York local time and use
        # its wall-clock time together with the configured mimic date.
        mimic_service_start_time = datetime.combine(
            mimic_current_date,
            real_service_start_time.astimezone(NY_TZ).timetz(),
        )

        return cls(
            mimic_current_date=mimic_current_date,
            real_service_start_time=real_service_start_time,
            mimic_service_start_time=mimic_service_start_time,
        )

    @classmethod
    def from_existing_anchor(
        cls,
        mimic_current_date: date,
        real_service_start_time: datetime,
        mimic_service_start_time: datetime,
    ) -> ServiceClock:
        """
        Build a ServiceClock from an existing shared service-time anchor.

        This constructor is intended for cases where multiple services must
        stay synchronized to the same session timing, such as streaming and
        API services served together.

        Args:
            mimic_current_date:
                The date that the service should mimic in New York time.
            real_service_start_time:
                The real service start time in UTC.
            mimic_service_start_time:
                The corresponding mimic service start time in New York time.

        Returns:
            A ServiceClock initialized from the provided timing values.
        """
        return cls(
            mimic_current_date=mimic_current_date,
            real_service_start_time=real_service_start_time,
            mimic_service_start_time=mimic_service_start_time,
        )

    @staticmethod
    def _validate_inputs(
        mimic_current_date: date,
        real_service_start_time: datetime,
        mimic_service_start_time: datetime,
    ) -> None:
        """
        Validate ServiceClock initialization inputs.
        """
        if mimic_current_date is None:
            raise ValueError("mimic_current_date must be set for ServiceClock")

        if real_service_start_time is None:
            raise ValueError("real_service_start_time must be provided")

        if mimic_service_start_time is None:
            raise ValueError("mimic_service_start_time must be provided")

        if real_service_start_time.tzinfo is None:
            raise ValueError("real_service_start_time must be timezone-aware")

        if mimic_service_start_time.tzinfo is None:
            raise ValueError("mimic_service_start_time must be timezone-aware")

        if real_service_start_time.tzinfo != timezone.utc:
            raise ValueError("real_service_start_time must be in UTC")

        if mimic_service_start_time.tzinfo != NY_TZ:
            raise ValueError(
                "mimic_service_start_time must be in America/New_York timezone"
            )

        expected_mimic_service_start_time = datetime.combine(
            mimic_current_date,
            real_service_start_time.astimezone(NY_TZ).timetz(),
        )

        if mimic_service_start_time != expected_mimic_service_start_time:
            raise ValueError(
                "mimic_service_start_time must match mimic_current_date combined with "
                "the New York wall-clock time derived from real_service_start_time"
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
