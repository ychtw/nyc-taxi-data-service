from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from nyc_taxi_data_service.serving.common.service_clock import ServiceClock

NY_TZ = ZoneInfo("America/New_York")


def build_matching_mimic_service_start_time(
    mimic_current_date: date,
    real_service_start_time: datetime,
) -> datetime:
    """
    Build a valid mimic_service_start_time that preserves the New York
    wall-clock time derived from real_service_start_time.
    """
    return datetime.combine(
        mimic_current_date,
        real_service_start_time.astimezone(NY_TZ).timetz(),
    )


def test_from_new_session_sets_real_and_mimic_service_start_times(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    from_new_session() should:
    - store real_service_start_time in UTC
    - create mimic_service_start_time in New York timezone using the same
      wall-clock time but replacing the date with mimic_current_date
    """
    fixed_real_start = datetime(2026, 3, 18, 9, 42, 15, 123456, tzinfo=timezone.utc)

    # patch datetime.now(...) inside the service_clock module so clock creation
    # uses a deterministic startup time.
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_real_start

    # monkeypatch.setattr replaces an attribute at runtime
    # here, we patch the datetime object inside the service_clock module
    # notes:
    #   in service_clock.py, the code does: from datetime import datetime
    #   so inside that module, datetime refers to:
    #       nyc_taxi_data_service.serving.common.service_clock.datetime
    monkeypatch.setattr(
        "nyc_taxi_data_service.serving.common.service_clock.datetime",
        FixedDateTime,
    )

    # create the clock with a mimic date, which should replace the
    # date part of the result
    clock = ServiceClock.from_new_session(mimic_current_date=date(2025, 1, 15))

    # real start is unchanged
    assert clock.real_service_start_time == fixed_real_start
    assert clock.mimic_service_start_time == build_matching_mimic_service_start_time(
        mimic_current_date=date(2025, 1, 15),
        real_service_start_time=fixed_real_start,
    )


def test_from_new_session_raises_value_error_when_mimic_current_date_is_none() -> None:
    """
    from_new_session() should raise ValueError if mimic_current_date is None.
    """
    with pytest.raises(ValueError, match="mimic_current_date must be set"):
        ServiceClock.from_new_session(mimic_current_date=None)


def test_from_existing_anchor_sets_all_fields_from_provided_values() -> None:
    """
    from_existing_anchor() should preserve the provided anchor values exactly
    when they satisfy the ServiceClock anchor contract.
    """
    mimic_current_date = date(2025, 1, 15)
    real_service_start_time = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)
    mimic_service_start_time = build_matching_mimic_service_start_time(
        mimic_current_date=mimic_current_date,
        real_service_start_time=real_service_start_time,
    )

    clock = ServiceClock.from_existing_anchor(
        mimic_current_date=mimic_current_date,
        real_service_start_time=real_service_start_time,
        mimic_service_start_time=mimic_service_start_time,
    )

    assert clock.mimic_current_date == mimic_current_date
    assert clock.real_service_start_time == real_service_start_time
    assert clock.mimic_service_start_time == mimic_service_start_time


@pytest.mark.parametrize(
    ("real_service_start_time", "expected_message"),
    [
        (
            None,
            "real_service_start_time must be provided",
        ),
        (
            datetime(2026, 3, 18, 9, 42, 15),
            "real_service_start_time must be timezone-aware",
        ),
        (
            datetime(2026, 3, 18, 9, 42, 15, tzinfo=NY_TZ),
            "real_service_start_time must be in UTC",
        ),
    ],
    ids=[
        "missing_real_service_start_time",
        "naive_real_service_start_time",
        "non_utc_real_service_start_time",
    ],
)
def test_from_existing_anchor_raises_for_invalid_real_service_start_time(
    real_service_start_time: datetime | None,
    expected_message: str,
) -> None:
    """
    from_existing_anchor() should reject missing, naive, or non-UTC
    real_service_start_time values.
    """
    with pytest.raises(ValueError, match=expected_message):
        ServiceClock.from_existing_anchor(
            mimic_current_date=date(2025, 1, 15),
            real_service_start_time=real_service_start_time,
            mimic_service_start_time=datetime(2025, 1, 15, 4, 42, 15, tzinfo=NY_TZ),
        )


@pytest.mark.parametrize(
    ("mimic_service_start_time", "expected_message"),
    [
        (
            None,
            "mimic_service_start_time must be provided",
        ),
        (
            datetime(2025, 1, 15, 4, 42, 15),
            "mimic_service_start_time must be timezone-aware",
        ),
        (
            datetime(2025, 1, 15, 4, 42, 15, tzinfo=timezone.utc),
            "mimic_service_start_time must be in America/New_York timezone",
        ),
    ],
    ids=[
        "missing_mimic_service_start_time",
        "naive_mimic_service_start_time",
        "non_new_york_mimic_service_start_time",
    ],
)
def test_from_existing_anchor_raises_for_invalid_mimic_service_start_time(
    mimic_service_start_time: datetime | None,
    expected_message: str,
) -> None:
    """
    from_existing_anchor() should reject missing, naive, or non-New-York
    mimic_service_start_time values.
    """
    with pytest.raises(ValueError, match=expected_message):
        ServiceClock.from_existing_anchor(
            mimic_current_date=date(2025, 1, 15),
            real_service_start_time=datetime(
                2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc
            ),
            mimic_service_start_time=mimic_service_start_time,
        )


def test_from_existing_anchor_raises_when_mimic_current_date_is_none() -> None:
    """
    from_existing_anchor() should raise ValueError if mimic_current_date is None.
    """
    with pytest.raises(ValueError, match="mimic_current_date must be set"):
        ServiceClock.from_existing_anchor(
            mimic_current_date=None,
            real_service_start_time=datetime(
                2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc
            ),
            mimic_service_start_time=datetime(2025, 1, 15, 4, 42, 15, tzinfo=NY_TZ),
        )


def test_from_existing_anchor_raises_when_mimic_service_start_date_mismatches() -> None:
    """
    from_existing_anchor() should raise ValueError when the New York date of
    mimic_service_start_time does not match mimic_current_date.
    """
    real_service_start_time = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)

    with pytest.raises(
        ValueError,
        match="mimic_service_start_time must match mimic_current_date combined",
    ):
        ServiceClock.from_existing_anchor(
            mimic_current_date=date(2025, 1, 15),
            real_service_start_time=real_service_start_time,
            mimic_service_start_time=build_matching_mimic_service_start_time(
                mimic_current_date=date(2025, 1, 16),
                real_service_start_time=real_service_start_time,
            ),
        )


def test_from_existing_anchor_raises_when_mimic_service_start_time_wall_clock_mismatches() -> (
    None
):
    """
    from_existing_anchor() should raise ValueError when mimic_service_start_time
    does not preserve the New York wall-clock time derived from
    real_service_start_time.
    """
    with pytest.raises(
        ValueError,
        match="mimic_service_start_time must match mimic_current_date combined",
    ):
        ServiceClock.from_existing_anchor(
            mimic_current_date=date(2025, 1, 15),
            real_service_start_time=datetime(
                2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc
            ),
            mimic_service_start_time=datetime(2025, 1, 15, 4, 42, 15, tzinfo=NY_TZ),
        )


def test_direct_init_sets_fields_when_values_are_valid() -> None:
    """
    Direct initialization should remain valid because __init__ performs the
    same input validation used by the classmethod constructors.
    """
    mimic_current_date = date(2025, 1, 15)
    real_service_start_time = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)
    mimic_service_start_time = build_matching_mimic_service_start_time(
        mimic_current_date=mimic_current_date,
        real_service_start_time=real_service_start_time,
    )

    clock = ServiceClock(
        mimic_current_date=mimic_current_date,
        real_service_start_time=real_service_start_time,
        mimic_service_start_time=mimic_service_start_time,
    )

    assert clock.mimic_current_date == mimic_current_date
    assert clock.real_service_start_time == real_service_start_time
    assert clock.mimic_service_start_time == mimic_service_start_time


def test_now_returns_mimic_service_start_time_when_no_time_has_elapsed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    now() should return mimic_service_start_time when no real time has elapsed.
    """
    mimic_current_date = date(2025, 1, 15)
    fixed_real_start = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)
    queued = [fixed_real_start]

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return queued.pop(0)

    monkeypatch.setattr(
        "nyc_taxi_data_service.serving.common.service_clock.datetime",
        FixedDateTime,
    )

    clock = ServiceClock(
        mimic_current_date=mimic_current_date,
        real_service_start_time=fixed_real_start,
        mimic_service_start_time=build_matching_mimic_service_start_time(
            mimic_current_date=mimic_current_date,
            real_service_start_time=fixed_real_start,
        ),
    )

    result = clock.now()

    assert result == clock.mimic_service_start_time


def test_now_advances_by_elapsed_real_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    now() should advance by the same elapsed duration as real time.
    """
    mimic_current_date = date(2025, 1, 15)
    fixed_real_start = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)
    fixed_real_now = fixed_real_start + timedelta(hours=3, minutes=15)

    queued = [fixed_real_now]

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return queued.pop(0)

    monkeypatch.setattr(
        "nyc_taxi_data_service.serving.common.service_clock.datetime",
        FixedDateTime,
    )

    clock = ServiceClock(
        mimic_current_date=mimic_current_date,
        real_service_start_time=fixed_real_start,
        mimic_service_start_time=build_matching_mimic_service_start_time(
            mimic_current_date=mimic_current_date,
            real_service_start_time=fixed_real_start,
        ),
    )

    result = clock.now()

    assert result == clock.mimic_service_start_time + timedelta(hours=3, minutes=15)


def test_now_advances_across_multiple_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    now() should continue advancing across multiple days instead of reusing
    only the original mimic_current_date with a replaced time-of-day.
    """
    mimic_current_date = date(2025, 1, 15)
    fixed_real_start = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)
    fixed_real_now = fixed_real_start + timedelta(days=2, hours=4)

    queued = [fixed_real_now]

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return queued.pop(0)

    monkeypatch.setattr(
        "nyc_taxi_data_service.serving.common.service_clock.datetime",
        FixedDateTime,
    )

    clock = ServiceClock(
        mimic_current_date=mimic_current_date,
        real_service_start_time=fixed_real_start,
        mimic_service_start_time=build_matching_mimic_service_start_time(
            mimic_current_date=mimic_current_date,
            real_service_start_time=fixed_real_start,
        ),
    )

    result = clock.now()

    assert result == clock.mimic_service_start_time + timedelta(days=2, hours=4)
    assert result.date() == date(2025, 1, 17)


def test_now_returns_new_york_timezone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    now() should return a timezone-aware datetime in America/New_York.
    """
    mimic_current_date = date(2025, 1, 15)
    fixed_real_start = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)
    fixed_real_now = fixed_real_start + timedelta(minutes=30)

    queued = [fixed_real_now]

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return queued.pop(0)

    monkeypatch.setattr(
        "nyc_taxi_data_service.serving.common.service_clock.datetime",
        FixedDateTime,
    )

    clock = ServiceClock(
        mimic_current_date=mimic_current_date,
        real_service_start_time=fixed_real_start,
        mimic_service_start_time=build_matching_mimic_service_start_time(
            mimic_current_date=mimic_current_date,
            real_service_start_time=fixed_real_start,
        ),
    )

    result = clock.now()

    assert result.tzinfo == NY_TZ


def test_now_uses_current_time_not_cached_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    now() should use the current UTC time at call time rather than reusing
    the startup time stored during initialization.
    """
    mimic_current_date = date(2025, 1, 15)
    fixed_real_start = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)
    fixed_real_now = fixed_real_start + timedelta(hours=2, minutes=30)

    queued = [fixed_real_now]

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return queued.pop(0)

    monkeypatch.setattr(
        "nyc_taxi_data_service.serving.common.service_clock.datetime",
        FixedDateTime,
    )

    clock = ServiceClock(
        mimic_current_date=mimic_current_date,
        real_service_start_time=fixed_real_start,
        mimic_service_start_time=build_matching_mimic_service_start_time(
            mimic_current_date=mimic_current_date,
            real_service_start_time=fixed_real_start,
        ),
    )

    result = clock.now()

    assert clock.real_service_start_time == fixed_real_start
    assert result == clock.mimic_service_start_time + timedelta(hours=2, minutes=30)
