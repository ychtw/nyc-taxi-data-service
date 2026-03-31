import json
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from nyc_taxi_data_service.serving.initializer import (
    build_service_startup_context,
    main,
    write_service_startup_context,
)

NY_TZ = ZoneInfo("America/New_York")


def test_build_service_startup_context_adds_service_clock_anchor(
    mocker,
) -> None:
    """
    build_service_startup_context() should return a new dictionary that
    preserves the resolved runtime config and adds serving.service_clock_anchor.
    """
    resolved_runtime_config = {
        "manifest": {"output_path": "data/manifests"},
        "logging": {"level": "DEBUG"},
        "serving": {
            "mimic_current_date": "2025-01-15",
            "streaming": {"buffer": {"window_size": "1m", "retention": "1m"}},
        },
        "vehicle_types": ["yellow", "green"],
    }

    mock_service_clock = mocker.Mock()
    mock_service_clock.real_service_start_time = datetime(
        2026, 3, 31, 6, 25, 6, 605282, tzinfo=timezone.utc
    )
    mock_service_clock.mimic_service_start_time = datetime(
        2025, 1, 15, 2, 25, 6, 605282, tzinfo=NY_TZ
    )

    mock_from_new_session = mocker.patch(
        "nyc_taxi_data_service.serving.initializer.ServiceClock.from_new_session",
        return_value=mock_service_clock,
    )

    startup_context = build_service_startup_context(
        resolved_runtime_config=resolved_runtime_config
    )

    mock_from_new_session.assert_called_once_with(mimic_current_date=date(2025, 1, 15))

    assert startup_context is not resolved_runtime_config
    assert startup_context["serving"]["mimic_current_date"] == "2025-01-15"
    assert startup_context["serving"]["service_clock_anchor"] == {
        "real_service_start_time_utc": "2026-03-31T06:25:06.605282+00:00",
        "mimic_service_start_time": "2025-01-15T02:25:06.605282-05:00",
    }

    # confirm the input dictionary is not mutated
    assert "service_clock_anchor" not in resolved_runtime_config["serving"]


def test_build_service_startup_context_raises_when_mimic_current_date_is_missing() -> (
    None
):
    """
    build_service_startup_context() should raise ValueError when
    serving.mimic_current_date is missing.
    """
    resolved_runtime_config = {
        "manifest": {"output_path": "data/manifests"},
        "logging": {"level": "DEBUG"},
        "serving": {},
        "vehicle_types": ["yellow", "green"],
    }

    with pytest.raises(
        ValueError,
        match="serving.mimic_current_date must be set in resolved runtime config",
    ):
        build_service_startup_context(resolved_runtime_config=resolved_runtime_config)


def test_build_service_startup_context_raises_when_mimic_current_date_is_invalid(
    mocker,
) -> None:
    """
    build_service_startup_context() should raise ValueError when
    serving.mimic_current_date is not a valid ISO date string.
    """
    resolved_runtime_config = {
        "manifest": {"output_path": "data/manifests"},
        "logging": {"level": "DEBUG"},
        "serving": {"mimic_current_date": "2025/01/15"},
        "vehicle_types": ["yellow", "green"],
    }

    with pytest.raises(ValueError):
        build_service_startup_context(resolved_runtime_config=resolved_runtime_config)


def test_write_service_startup_context_writes_json_file_atomically(
    tmp_path: Path,
) -> None:
    """
    write_service_startup_context() should write the startup context JSON to
    the requested path, creating parent directories as needed.
    """
    startup_context = {
        "manifest": {"output_path": "data/manifests"},
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_clock_anchor": {
                "real_service_start_time_utc": "2026-03-31T06:25:06.605282+00:00",
                "mimic_service_start_time": "2025-01-15T02:25:06.605282-05:00",
            },
        },
    }

    output_path = tmp_path / "nested" / "service_startup_context.json"

    write_service_startup_context(
        startup_context=startup_context,
        output_path=output_path,
    )

    assert output_path.exists()

    with output_path.open("r", encoding="utf-8") as infile:
        written_data = json.load(infile)

    assert written_data == startup_context


def test_write_service_startup_context_replaces_existing_file(
    tmp_path: Path,
) -> None:
    """
    write_service_startup_context() should replace an existing target file with
    the new startup context content.
    """
    output_path = tmp_path / "service_startup_context.json"
    output_path.write_text('{"old": "content"}', encoding="utf-8")

    startup_context = {
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_clock_anchor": {
                "real_service_start_time_utc": "2026-03-31T06:25:06.605282+00:00",
                "mimic_service_start_time": "2025-01-15T02:25:06.605282-05:00",
            },
        }
    }

    write_service_startup_context(
        startup_context=startup_context,
        output_path=output_path,
    )

    with output_path.open("r", encoding="utf-8") as infile:
        written_data = json.load(infile)

    assert written_data == startup_context


def test_write_service_startup_context_cleans_up_temp_file_when_json_dump_fails(
    mocker,
    tmp_path: Path,
) -> None:
    """
    write_service_startup_context() should remove the temporary file if JSON
    serialization fails before the atomic replace step.
    """
    startup_context = {
        "serving": {
            "mimic_current_date": "2025-01-15",
        }
    }
    output_path = tmp_path / "service_startup_context.json"

    mocker.patch(
        "nyc_taxi_data_service.serving.initializer.json.dump",
        side_effect=RuntimeError("json dump failed"),
    )

    with pytest.raises(RuntimeError, match="json dump failed"):
        write_service_startup_context(
            startup_context=startup_context,
            output_path=output_path,
        )

    assert not output_path.exists()
    assert list(tmp_path.iterdir()) == []


def test_main_loads_resolves_builds_and_writes_startup_context(
    mocker,
) -> None:
    """
    main() should load runtime config, resolve it, configure logging, build the
    startup context, and write it to the configured manifest output path.
    """
    runtime_config = {"active_environment": "dev"}
    resolved_runtime_config = {
        "manifest": {"output_path": "data/manifests"},
        "logging": {"level": "DEBUG"},
        "serving": {"mimic_current_date": "2025-01-15"},
    }
    startup_context = {
        "manifest": {"output_path": "data/manifests"},
        "logging": {"level": "DEBUG"},
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_clock_anchor": {
                "real_service_start_time_utc": "2026-03-31T06:25:06.605282+00:00",
                "mimic_service_start_time": "2025-01-15T02:25:06.605282-05:00",
            },
        },
    }

    mock_load_runtime_config = mocker.patch(
        "nyc_taxi_data_service.serving.initializer.load_runtime_config",
        return_value=runtime_config,
    )
    mock_resolve_runtime_config = mocker.patch(
        "nyc_taxi_data_service.serving.initializer.resolve_runtime_config",
        return_value=resolved_runtime_config,
    )
    mock_setup_logging = mocker.patch(
        "nyc_taxi_data_service.serving.initializer.setup_logging"
    )
    mock_build_service_startup_context = mocker.patch(
        "nyc_taxi_data_service.serving.initializer.build_service_startup_context",
        return_value=startup_context,
    )
    mock_write_service_startup_context = mocker.patch(
        "nyc_taxi_data_service.serving.initializer.write_service_startup_context"
    )

    main()

    mock_load_runtime_config.assert_called_once_with()
    mock_resolve_runtime_config.assert_called_once_with(runtime_config)
    mock_setup_logging.assert_called_once_with(level="DEBUG")
    mock_build_service_startup_context.assert_called_once_with(
        resolved_runtime_config=resolved_runtime_config
    )
    mock_write_service_startup_context.assert_called_once_with(
        startup_context=startup_context,
        output_path=Path("data/manifests") / "service_startup_context.json",
    )


def test_main_uses_info_logging_as_default_when_logging_level_is_missing(
    mocker,
) -> None:
    """
    main() should default setup_logging() to INFO when logging.level is missing
    from the resolved runtime config.
    """
    resolved_runtime_config = {
        "manifest": {"output_path": "data/manifests"},
        "serving": {"mimic_current_date": "2025-01-15"},
    }

    mocker.patch(
        "nyc_taxi_data_service.serving.initializer.load_runtime_config",
        return_value={},
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.initializer.resolve_runtime_config",
        return_value=resolved_runtime_config,
    )
    mock_setup_logging = mocker.patch(
        "nyc_taxi_data_service.serving.initializer.setup_logging"
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.initializer.build_service_startup_context",
        return_value={"serving": {}},
    )
    mocker.patch(
        "nyc_taxi_data_service.serving.initializer.write_service_startup_context"
    )

    main()

    mock_setup_logging.assert_called_once_with(level="INFO")
