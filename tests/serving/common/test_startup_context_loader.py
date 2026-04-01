import json
from pathlib import Path

import pytest

from nyc_taxi_data_service.serving.common.startup_context_loader import (
    get_startup_context_path_from_env,
    load_service_startup_context,
    load_serving_runtime_config,
)


def test_load_service_startup_context_reads_valid_json(tmp_path: Path) -> None:
    """
    Successfully load a valid startup context JSON file.
    """
    file_path = tmp_path / "service_startup_context.json"
    data = {"serving": {"mimic_current_date": "2025-01-15"}}

    file_path.write_text(json.dumps(data), encoding="utf-8")

    result = load_service_startup_context(file_path)

    assert result == data


def test_load_service_startup_context_raises_for_invalid_json(tmp_path: Path) -> None:
    """
    Raise ValueError when JSON content is invalid.
    """
    file_path = tmp_path / "service_startup_context.json"
    file_path.write_text("{invalid json", encoding="utf-8")

    with pytest.raises(ValueError):
        load_service_startup_context(file_path)


def test_get_startup_context_path_from_env_returns_none_when_not_set(mocker) -> None:
    """
    Return None when SERVICE_STARTUP_CONTEXT_PATH is not set.
    """
    mocker.patch("os.getenv", return_value=None)

    result = get_startup_context_path_from_env()

    assert result is None


def test_get_startup_context_path_from_env_returns_path(mocker) -> None:
    """
    Return Path when SERVICE_STARTUP_CONTEXT_PATH is set.
    """
    mocker.patch("os.getenv", return_value="/tmp/file.json")

    result = get_startup_context_path_from_env()

    assert isinstance(result, Path)
    assert str(result) == "/tmp/file.json"


def test_load_serving_runtime_config_uses_startup_context_when_env_set(mocker) -> None:
    """
    Use startup context JSON when env var is set.
    """
    mock_path = Path("/tmp/startup.json")
    mock_data = {"serving": {}}

    mocker.patch(
        "nyc_taxi_data_service.serving.common.startup_context_loader.get_startup_context_path_from_env",
        return_value=mock_path,
    )
    mock_load_json = mocker.patch(
        "nyc_taxi_data_service.serving.common.startup_context_loader.load_service_startup_context",
        return_value=mock_data,
    )

    result = load_serving_runtime_config()

    assert result == mock_data
    mock_load_json.assert_called_once_with(mock_path)


def test_load_serving_runtime_config_falls_back_to_runtime_yaml(mocker) -> None:
    """
    Load and resolve runtime.yaml when env var is not set.
    """
    runtime_config = {"active_environment": "dev"}
    resolved_runtime_config = {"serving": {}}

    mocker.patch(
        "nyc_taxi_data_service.serving.common.startup_context_loader.get_startup_context_path_from_env",
        return_value=None,
    )
    mock_load = mocker.patch(
        "nyc_taxi_data_service.serving.common.startup_context_loader.load_runtime_config",
        return_value=runtime_config,
    )
    mock_resolve = mocker.patch(
        "nyc_taxi_data_service.serving.common.startup_context_loader.resolve_runtime_config",
        return_value=resolved_runtime_config,
    )

    result = load_serving_runtime_config()

    assert result == resolved_runtime_config
    mock_load.assert_called_once_with()
    mock_resolve.assert_called_once_with(runtime_config)


def test_load_service_startup_context_preserves_anchor_datetime_strings(
    tmp_path: Path,
) -> None:
    """
    Preserve startup-context anchor datetime strings exactly as stored in JSON.

    Parsing and timezone normalization are handled later by the service clock loader.
    """
    file_path = tmp_path / "service_startup_context.json"
    data = {
        "serving": {
            "mimic_current_date": "2025-01-15",
            "service_clock_anchor": {
                "real_service_start_time_utc": "2026-03-31T12:22:54+00:00",
                "mimic_service_start_time": "2025-01-15T08:22:54-05:00",
            },
        }
    }

    file_path.write_text(json.dumps(data), encoding="utf-8")

    result = load_service_startup_context(file_path)

    assert result == data
