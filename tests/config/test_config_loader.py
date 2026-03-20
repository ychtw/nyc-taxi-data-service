import textwrap
from pathlib import Path

import pytest
import yaml

from nyc_taxi_data_service.config.config_loader import (
    load_runtime_config,
    load_sources_config,
    resolve_runtime_config,
)


def test_load_sources_config_default() -> None:
    """Load the default packaged sources config."""
    config = load_sources_config()

    # assert top-level keys exist
    assert "tlc_website" in config
    assert "vehicle_types" in config

    assert (
        config["tlc_website"]["base_url"]
        == "https://d37ci6vzurychx.cloudfront.net/trip-data"
    )
    assert config["tlc_website"]["file_extension"] == "parquet"

    # assert values for each vehicle type
    assert config["vehicle_types"]["yellow"]["available_from"] == "2009-01"
    assert config["vehicle_types"]["yellow"]["name_pattern"] == "yellow_tripdata"
    assert config["vehicle_types"]["green"]["available_from"] == "2013-08"
    assert config["vehicle_types"]["green"]["name_pattern"] == "green_tripdata"
    assert config["vehicle_types"]["fhv"]["available_from"] == "2015-01"
    assert config["vehicle_types"]["fhv"]["name_pattern"] == "fhv_tripdata"
    assert config["vehicle_types"]["fhvhv"]["available_from"] == "2019-02"
    assert config["vehicle_types"]["fhvhv"]["name_pattern"] == "fhvhv_tripdata"


def test_load_sources_config_custom_path(tmp_path: Path) -> None:
    """Load sources config from a custom YAML file path."""
    # create a mock yaml in the temporary directory
    config_file = tmp_path / "test_sources.yaml"
    mock_data = {
        "tlc_website": {
            "base_url": "https://test-data.com",
            "file_extension": "parquet",
        },
        "vehicle_types": {
            "test_taxi": {"available_from": "2026-01", "name_pattern": "test_tripdata"}
        },
    }
    with open(config_file, "w", encoding="utf-8") as f:
        yaml.dump(mock_data, f)

    # call the function with the custom testing. path
    config = load_sources_config(config_file)

    # assert result values
    assert config["tlc_website"]["base_url"] == "https://test-data.com"
    assert config["tlc_website"]["file_extension"] == "parquet"
    assert config["vehicle_types"]["test_taxi"]["available_from"] == "2026-01"
    assert config["vehicle_types"]["test_taxi"]["name_pattern"] == "test_tripdata"


def test_load_sources_config_raises_file_not_found_for_missing_custom_file(
    tmp_path: Path,
) -> None:
    """Raise FileNotFoundError when the custom config file is missing."""
    missing_file = tmp_path / "does_not_exist.yaml"

    with pytest.raises(FileNotFoundError):
        load_sources_config(missing_file)


def test_load_sources_config_raises_yaml_error_for_invalid_yaml(tmp_path: Path) -> None:
    """Raise yaml.YAMLError when the YAML file content is invalid."""
    # mock invalid yaml
    invalid_yaml_text = textwrap.dedent(
        """
        tlc_website:
          base_url: "https://example.com"
          file_extension: parquet
        vehicle_types:
          yellow:
            available_from: "2009-01"
            name_pattern: "unclosed string
        """
    )

    # create a yaml file with invalid content
    bad_file = tmp_path / "bad_sources.yaml"
    bad_file.write_text(
        invalid_yaml_text.strip(),
        encoding="utf-8",
    )

    # assert yaml error
    with pytest.raises(yaml.YAMLError):
        load_sources_config(bad_file)


def test_load_runtime_config_default() -> None:
    """Load the default packaged runtime config."""
    config = load_runtime_config()

    allowed_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    allowed_target_storage_types = {"local"}

    # assert top-level keys exist
    assert "active_environment" in config
    assert "defaults" in config
    assert "environments" in config

    # assert defaults section
    assert "manifest" in config["defaults"]
    assert "logging" in config["defaults"]
    assert "level" in config["defaults"]["logging"]
    assert config["defaults"]["logging"]["level"] in allowed_log_levels

    assert "serving" in config["defaults"]
    serving_defaults = config["defaults"]["serving"]
    assert "service_event_time_column_by_vehicle_type" in serving_defaults
    event_time_mapping = serving_defaults["service_event_time_column_by_vehicle_type"]
    assert "yellow" in event_time_mapping
    assert "green" in event_time_mapping
    assert "fhv" in event_time_mapping
    assert "fhvhv" in event_time_mapping
    assert all(isinstance(value, str) for value in event_time_mapping.values())

    # assert there's corresponding section in `environments` for the active environment
    active_env = config["active_environment"]
    assert active_env in config["environments"]

    # assert essential settings exist for every environment section
    for env_config in config["environments"].values():
        assert "storage" in env_config
        assert "time_frame" in env_config
        assert "vehicle_types" in env_config
        assert "logging" in env_config
        assert "serving" in env_config

        assert "level" in env_config["logging"]
        assert env_config["logging"]["level"] in allowed_log_levels

        storage_config = env_config["storage"]
        assert "target_storage_type" in storage_config
        assert storage_config["target_storage_type"] in allowed_target_storage_types

        target_storage_type = storage_config["target_storage_type"]
        assert f"{target_storage_type}_settings" in storage_config
        assert "target_base_path" in storage_config[f"{target_storage_type}_settings"]

        serving_config = env_config["serving"]
        assert "mimic_current_date" in serving_config
        assert isinstance(serving_config["mimic_current_date"], str)


def test_load_runtime_config_custom_path(tmp_path: Path) -> None:
    """Load runtime config from a custom YAML file path."""
    # create a mock yaml in the temporary directory
    config_file = tmp_path / "test_runtime.yaml"
    mock_data = {
        "active_environment": "test",
        "defaults": {
            "manifest": {"output_path": "tmp/manifests"},
            "logging": {"level": "WARNING"},
            "serving": {
                "service_event_time_column_by_vehicle_type": {
                    "yellow": "tpep_dropoff_datetime",
                }
            },
        },
        "environments": {
            "test": {
                "logging": {"level": "DEBUG"},
                "storage": {
                    "target_storage_type": "local",
                    "local_settings": {"target_base_path": "tmp/raw"},
                },
                "time_frame": {
                    "start_year_month": "2026-01",
                    "end_year_month": "2026-02",
                },
                "vehicle_types": ["yellow"],
                "serving": {
                    "mimic_current_date": "2026-01-15",
                },
            }
        },
    }
    with config_file.open("w", encoding="utf-8") as f:
        yaml.dump(mock_data, f)

    # call the function with the custom testing path
    config = load_runtime_config(config_file)

    # assert result values
    assert config["active_environment"] == "test"
    assert config["defaults"]["manifest"]["output_path"] == "tmp/manifests"
    assert config["defaults"]["logging"]["level"] == "WARNING"

    test_env = config["environments"]["test"]
    assert test_env["logging"]["level"] == "DEBUG"
    assert test_env["storage"]["target_storage_type"] == "local"
    assert test_env["storage"]["local_settings"]["target_base_path"] == "tmp/raw"
    assert test_env["time_frame"]["start_year_month"] == "2026-01"
    assert test_env["time_frame"]["end_year_month"] == "2026-02"
    assert test_env["vehicle_types"] == ["yellow"]
    assert test_env["serving"]["mimic_current_date"] == "2026-01-15"


def test_load_runtime_config_raises_file_not_found_for_missing_custom_file(
    tmp_path: Path,
) -> None:
    """Raise FileNotFoundError when the custom runtime config file is missing."""
    missing_file = tmp_path / "does_not_exist.yaml"

    with pytest.raises(FileNotFoundError):
        load_runtime_config(missing_file)


def test_load_runtime_config_raises_yaml_error_for_invalid_yaml(
    tmp_path: Path,
) -> None:
    """Raise yaml.YAMLError when the runtime YAML file content is invalid."""
    # mock invalid yaml
    invalid_yaml_text = textwrap.dedent(
        """
        active_environment: dev

        defaults:
          manifest:
            output_path: data/manifests
          logging:
            level: INFO

        environments:
          dev:
            logging:
              level: DEBUG
            storage:
              target_storage_type: local
              local_settings:
                target_base_path: [unclosed
        """
    )

    # create a yaml file with invalid content
    bad_file = tmp_path / "bad_runtime.yaml"
    bad_file.write_text(
        invalid_yaml_text.strip(),
        encoding="utf-8",
    )

    # assert yaml error
    with pytest.raises(yaml.YAMLError):
        load_runtime_config(bad_file)


def test_load_runtime_config_default_active_env_vehicle_types_have_event_time_mapping() -> (
    None
):
    """Ensure each active-environment vehicle type has a service event time mapping."""
    config = load_runtime_config()

    active_env = config["active_environment"]
    active_env_config = config["environments"][active_env]

    vehicle_types = active_env_config["vehicle_types"]
    event_time_mapping = config["defaults"]["serving"][
        "service_event_time_column_by_vehicle_type"
    ]

    for vehicle_type in vehicle_types:
        assert vehicle_type in event_time_mapping


def test_resolve_runtime_config_local_storage_happy_path() -> None:
    """Resolve active environment config and normalize local storage settings."""
    # define mock runtime config
    runtime_config = {
        "active_environment": "dev",
        "defaults": {
            "manifest": {"output_path": "data/manifests"},
            "logging": {"level": "INFO"},
            "serving": {
                "service_event_time_column_by_vehicle_type": {
                    "yellow": "tpep_dropoff_datetime",
                    "green": "lpep_dropoff_datetime",
                    "fhv": "dropOff_datetime",
                    "fhvhv": "dropoff_datetime",
                }
            },
        },
        "environments": {
            "dev": {
                "logging": {"level": "DEBUG"},
                "storage": {
                    "target_storage_type": "local",
                    "local_settings": {"target_base_path": "data/raw"},
                },
                "time_frame": {
                    "start_year_month": "2025-01",
                    "end_year_month": "2025-03",
                },
                "vehicle_types": ["yellow", "green"],
                "serving": {
                    "mimic_current_date": "2025-01-15",
                },
            }
        },
    }

    # call the function with mock runtime config
    resolved = resolve_runtime_config(runtime_config)

    # assert resolved results
    assert resolved["active_environment"] == "dev"
    assert resolved["manifest"]["output_path"] == "data/manifests"
    assert resolved["logging"]["level"] == "DEBUG"
    assert resolved["storage"] == {
        "target_storage_type": "local",
        "target_base_path": "data/raw",
    }  # local_settings should not exist, because the function should flatten it
    assert resolved["time_frame"]["start_year_month"] == "2025-01"
    assert resolved["time_frame"]["end_year_month"] == "2025-03"
    assert resolved["vehicle_types"] == ["yellow", "green"]
    assert resolved["serving"] == {
        "service_event_time_column_by_vehicle_type": {
            "yellow": "tpep_dropoff_datetime",
            "green": "lpep_dropoff_datetime",
            "fhv": "dropOff_datetime",
            "fhvhv": "dropoff_datetime",
        },
        "mimic_current_date": "2025-01-15",
    }


def test_resolve_runtime_config_uses_active_environment_section() -> None:
    """Resolve only the config for the selected active environment."""
    # define mock runtime config
    runtime_config = {
        "active_environment": "prod",
        "defaults": {
            "manifest": {"output_path": "data/manifests"},
            "logging": {"level": "INFO"},
            "serving": {
                "service_event_time_column_by_vehicle_type": {
                    "yellow": "tpep_dropoff_datetime",
                    "green": "lpep_dropoff_datetime",
                }
            },
        },
        "environments": {
            "dev": {
                "logging": {"level": "DEBUG"},
                "storage": {
                    "target_storage_type": "local",
                    "local_settings": {"target_base_path": "data/dev_raw"},
                },
                "time_frame": {
                    "start_year_month": "2025-01",
                    "end_year_month": "2025-03",
                },
                "vehicle_types": ["yellow", "green"],
                "serving": {
                    "mimic_current_date": "2025-01-15",
                },
            },
            "prod": {
                "logging": {"level": "WARNING"},
                "storage": {
                    "target_storage_type": "local",
                    "local_settings": {"target_base_path": "data/prod_raw"},
                },
                "time_frame": {
                    "start_year_month": "2024-01",
                    "end_year_month": "2024-12",
                },
                "vehicle_types": ["fhv"],
                "serving": {
                    "mimic_current_date": "2024-12-15",
                },
            },
        },
    }

    # call the function with mock runtime config
    resolved = resolve_runtime_config(runtime_config)

    # assert resolved results
    assert resolved["active_environment"] == "prod"
    assert resolved["logging"]["level"] == "WARNING"
    assert resolved["storage"] == {
        "target_storage_type": "local",
        "target_base_path": "data/prod_raw",
    }
    assert resolved["time_frame"]["start_year_month"] == "2024-01"
    assert resolved["time_frame"]["end_year_month"] == "2024-12"
    assert resolved["vehicle_types"] == ["fhv"]
    assert resolved["serving"] == {
        "service_event_time_column_by_vehicle_type": {
            "yellow": "tpep_dropoff_datetime",
            "green": "lpep_dropoff_datetime",
        },
        "mimic_current_date": "2024-12-15",
    }


def test_resolve_runtime_config_deep_merge_behavior() -> None:
    """Use a deep merge, so nested dicts are combined instead of replaced."""
    # define mock runtime config
    runtime_config = {
        "active_environment": "dev",
        "defaults": {
            "logging": {
                "level": "INFO",
                "format": "plain_text",
            },
            "manifest": {"output_path": "data/manifests"},
            "serving": {
                "service_event_time_column_by_vehicle_type": {
                    "yellow": "tpep_dropoff_datetime",
                    "green": "lpep_dropoff_datetime",
                    "fhv": "dropOff_datetime",
                    "fhvhv": "dropoff_datetime",
                }
            },
        },
        "environments": {
            "dev": {
                "logging": {"level": "DEBUG"},
                "storage": {
                    "target_storage_type": "local",
                    "local_settings": {"target_base_path": "data/raw"},
                },
                "time_frame": {
                    "start_year_month": "2025-01",
                    "end_year_month": "2025-03",
                },
                "vehicle_types": ["yellow"],
                "serving": {
                    "mimic_current_date": "2025-01-15",
                },
            }
        },
    }

    # call the function with mock runtime config
    resolved = resolve_runtime_config(runtime_config)

    # assert resolved values
    assert resolved["logging"] == {
        "level": "DEBUG",
        "format": "plain_text",
    }
    assert resolved["serving"] == {
        "service_event_time_column_by_vehicle_type": {
            "yellow": "tpep_dropoff_datetime",
            "green": "lpep_dropoff_datetime",
            "fhv": "dropOff_datetime",
            "fhvhv": "dropoff_datetime",
        },
        "mimic_current_date": "2025-01-15",
    }


def test_resolve_runtime_config_raises_value_error_when_storage_type_missing() -> None:
    """Raise ValueError if storage.target_storage_type is missing."""
    runtime_config = {
        "active_environment": "dev",
        "defaults": {},
        "environments": {
            "dev": {
                "storage": {},
                "time_frame": {
                    "start_year_month": "2025-01",
                    "end_year_month": "2025-03",
                },
                "vehicle_types": ["yellow"],
                "serving": {
                    "mimic_current_date": "2025-01-15",
                },
            }
        },
    }

    with pytest.raises(ValueError, match="Missing storage.target_storage_type"):
        resolve_runtime_config(runtime_config)


def test_resolve_runtime_config_raises_value_error_when_local_settings_missing() -> (
    None
):
    """Raise ValueError if local_settings is missing for local storage."""
    runtime_config = {
        "active_environment": "dev",
        "defaults": {},
        "environments": {
            "dev": {
                "storage": {
                    "target_storage_type": "local",
                },
                "time_frame": {
                    "start_year_month": "2025-01",
                    "end_year_month": "2025-03",
                },
                "vehicle_types": ["yellow"],
                "serving": {
                    "mimic_current_date": "2025-01-15",
                },
            }
        },
    }

    with pytest.raises(
        ValueError,
        match="storage.local_settings is required",
    ):
        resolve_runtime_config(runtime_config)


def test_resolve_runtime_config_raises_value_error_when_target_base_path_missing() -> (
    None
):
    """Raise ValueError if local_settings.target_base_path is missing for local storage."""
    runtime_config = {
        "active_environment": "dev",
        "defaults": {},
        "environments": {
            "dev": {
                "storage": {
                    "target_storage_type": "local",
                    "local_settings": {"some_other_key": "value"},
                },
                "time_frame": {
                    "start_year_month": "2025-01",
                    "end_year_month": "2025-03",
                },
                "vehicle_types": ["yellow"],
                "serving": {
                    "mimic_current_date": "2025-01-15",
                },
            }
        },
    }

    with pytest.raises(
        ValueError,
        match="storage.local_settings.target_base_path is required",
    ):
        resolve_runtime_config(runtime_config)


def test_resolve_runtime_config_raises_not_implemented_for_unsupported_storage_type() -> (
    None
):
    """Raise NotImplementedError for unsupported storage backends."""
    runtime_config = {
        "active_environment": "dev",
        "defaults": {},
        "environments": {
            "dev": {
                "storage": {
                    "target_storage_type": "s3",
                    "s3_settings": {
                        "bucket": "my-test-bucket",
                        "prefix": "raw/",
                    },
                },
                "time_frame": {
                    "start_year_month": "2025-01",
                    "end_year_month": "2025-03",
                },
                "vehicle_types": ["yellow"],
                "serving": {
                    "mimic_current_date": "2025-01-15",
                },
            }
        },
    }

    with pytest.raises(
        NotImplementedError,
        match="Unsupported target_storage_type: s3",
    ):
        resolve_runtime_config(runtime_config)
