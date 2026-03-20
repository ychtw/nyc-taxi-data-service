from datetime import date, datetime, timedelta, timezone

import pytest

from nyc_taxi_data_service.serving.common.service_clock import ServiceClock


def test_post_init_sets_real_and_mimic_service_start_times(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    __post_init__ should record the real service start time and the
    corresponding mimic service start time when the clock is created.
    """
    # define the fixed "current time" we want the clock to see
    # use a constant value to make the test deterministic
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
    clock = ServiceClock(mimic_current_date=date(2025, 1, 15))

    assert clock.real_service_start_time == fixed_real_start
    assert clock.mimic_service_start_time == datetime(
        2025, 1, 15, 9, 42, 15, 123456, tzinfo=timezone.utc
    )


def test_post_init_raises_value_error_when_mimic_current_date_is_none() -> None:
    """
    __post_init__ should raise ValueError if mimic_current_date is None.
    """
    with pytest.raises(ValueError, match="mimic_current_date must be set"):
        ServiceClock(mimic_current_date=None)


def test_now_returns_mimic_service_start_time_when_no_time_has_elapsed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    now() should return mimic_service_start_time when the current real UTC
    time is equal to real_service_start_time.
    """
    fixed_real_start = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)

    # datetime.now(...) is called twice:
    # 1. during clock initialization
    # 2. during now()
    queued_now_values = [fixed_real_start, fixed_real_start]

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return queued_now_values.pop(0)

    monkeypatch.setattr(
        "nyc_taxi_data_service.serving.common.service_clock.datetime",
        FixedDateTime,
    )

    clock = ServiceClock(mimic_current_date=date(2025, 1, 15))
    result = clock.now()

    assert result == clock.mimic_service_start_time


def test_now_advances_by_elapsed_real_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    now() should advance the mimic service time by the same elapsed duration
    as the elapsed real time since service startup.
    """
    fixed_real_start = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)
    fixed_real_now = fixed_real_start + timedelta(hours=3, minutes=15)

    queued_now_values = [fixed_real_start, fixed_real_now]

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return queued_now_values.pop(0)

    monkeypatch.setattr(
        "nyc_taxi_data_service.serving.common.service_clock.datetime",
        FixedDateTime,
    )

    clock = ServiceClock(mimic_current_date=date(2025, 1, 15))
    result = clock.now()

    assert result == clock.mimic_service_start_time + timedelta(hours=3, minutes=15)


def test_now_advances_across_multiple_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    now() should continue advancing across multiple days instead of reusing
    only the original mimic_current_date with a replaced time-of-day.
    """
    fixed_real_start = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)
    fixed_real_now = fixed_real_start + timedelta(days=2, hours=4)

    queued_now_values = [fixed_real_start, fixed_real_now]

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return queued_now_values.pop(0)

    monkeypatch.setattr(
        "nyc_taxi_data_service.serving.common.service_clock.datetime",
        FixedDateTime,
    )

    clock = ServiceClock(mimic_current_date=date(2025, 1, 15))
    result = clock.now()

    assert result == clock.mimic_service_start_time + timedelta(days=2, hours=4)
    assert result.date() == date(2025, 1, 17)


def test_now_preserves_utc_timezone_in_returned_datetime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    now() should preserve timezone-aware UTC datetimes when computing the
    current mimic service time.
    """
    fixed_real_start = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)
    fixed_real_now = fixed_real_start + timedelta(minutes=30)

    queued_now_values = [fixed_real_start, fixed_real_now]

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return queued_now_values.pop(0)

    monkeypatch.setattr(
        "nyc_taxi_data_service.serving.common.service_clock.datetime",
        FixedDateTime,
    )

    clock = ServiceClock(mimic_current_date=date(2025, 1, 15))
    result = clock.now()

    assert result.tzinfo == timezone.utc


def test_now_uses_current_system_time_when_called(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    now() should use the current UTC time at call time rather than reusing
    the startup time stored during initialization.
    """
    fixed_real_start = datetime(2026, 3, 18, 9, 42, 15, tzinfo=timezone.utc)
    fixed_real_now = fixed_real_start + timedelta(hours=2, minutes=30)

    # The first value is used in __post_init__, and the second value is used
    # when now() calls datetime.now(timezone.utc).
    queued_now_values = [fixed_real_start, fixed_real_now]

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return queued_now_values.pop(0)

    monkeypatch.setattr(
        "nyc_taxi_data_service.serving.common.service_clock.datetime",
        FixedDateTime,
    )

    clock = ServiceClock(mimic_current_date=date(2025, 1, 15))
    result = clock.now()

    assert clock.real_service_start_time == fixed_real_start
    assert result == datetime(2025, 1, 15, 12, 12, 15, tzinfo=timezone.utc)
