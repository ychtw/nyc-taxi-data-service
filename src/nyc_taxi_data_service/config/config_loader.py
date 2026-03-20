import logging
from copy import deepcopy
from importlib.resources import files
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)  # initialize default logger


def load_sources_config(config_path: Path | None = None) -> dict:
    """
    Load sources config, which contains:
    - TLC base URL
    - file extension
    - availability start month by vehicle type
    - file naming patterns

    Args:
        config_path: A path to the data sources config yaml file. If None, use the default `config/sources.yaml`

    Returns:
        A dictionary of the input yaml config regarding data sources
    """
    if config_path is None:
        config_file = files("nyc_taxi_data_service.config").joinpath("sources.yaml")
    else:
        config_file = config_path

    logger.info(f"Loading sources config from: {config_file}")
    with config_file.open("r", encoding="utf-8") as f:
        sources_config = yaml.safe_load(f)
    logger.debug(f"Loaded sources config: {sources_config}")

    return sources_config


def load_runtime_config(config_path: Path | None = None) -> dict:
    """
    Load the runtime configuration YAML.

    The runtime config includes:
    - the active environment
    - shared defaults
    - per-environment settings such as logging, storage,
      request time frame, vehicle types, and serving-related config

    Args:
        config_path: A path to the runtime config yaml file.
            If None, use the default `config/runtime.yaml`

    Returns:
        A dictionary of the input yaml config regarding runtime settings
    """
    if config_path is None:
        config_file = files("nyc_taxi_data_service.config").joinpath("runtime.yaml")
    else:
        config_file = Path(config_path)

    logger.info(f"Loading runtime config from: {config_file}")
    with config_file.open("r", encoding="utf-8") as f:
        runtime_config = yaml.safe_load(f)
    logger.debug(f"Loaded runtime config: {runtime_config}")

    return runtime_config


def _deep_merge_dicts(base: dict, override: dict) -> dict:
    """
    Recursively merge two dictionaries.

    Rules:
    - if both values are dicts, merge them recursively
    - otherwise, the override value replaces the base value

    Args:
        base: The default/base dictionary.
        override: The dictionary whose values should override base.

    Returns:
        A new merged dictionary.
    """
    merged = deepcopy(base)

    for key, override_value in override.items():
        base_value = merged.get(key)

        if isinstance(base_value, dict) and isinstance(override_value, dict):
            merged[key] = _deep_merge_dicts(base_value, override_value)
        else:
            merged[key] = deepcopy(override_value)

    return merged


def resolve_runtime_config(runtime_config: dict) -> dict:
    """
    Resolve runtime config based on active_environment (dev / prod)

    This function:
    - selects the active environment from `environments`
    - shallow-merges shared `defaults` with the active environment config
    - normalizes storage settings into a flatter structure
    - returns the resolved runtime settings used by the application,
      including manifest, logging, storage, time_frame, vehicle_types,
      and serving config

    Args:
        runtime_config: A dictionary of runtime settings loaded from YAML.
            It may contain shared defaults and multiple environment sections.

    Returns:
        A dictionary of the resolved runtime settings for the active
        environment.
    """
    # get active environment (dev or prod)
    active_environment = runtime_config["active_environment"]
    logger.info(
        f"Resolving runtime configuration for environment: {active_environment}"
    )

    # get default config
    defaults = runtime_config.get("defaults", {})

    # get config for the active environment
    env_config = runtime_config["environments"][active_environment]

    # deep merge defaults with active environment config
    resolved_config = _deep_merge_dicts(defaults, env_config)
    resolved_config["active_environment"] = active_environment

    # storage normalization & validation
    storage_config = resolved_config.get("storage", {})
    target_storage_type = storage_config.get("target_storage_type")

    if not target_storage_type:
        raise ValueError(
            f"Missing storage.target_storage_type in runtime config for environment: {active_environment}"
        )

    if target_storage_type == "local":
        local_settings = storage_config.get("local_settings")
        if not local_settings:
            raise ValueError(
                "storage.local_settings is required when target_storage_type='local'"
            )

        target_base_path = local_settings.get("target_base_path")
        if not target_base_path:
            raise ValueError(
                "storage.local_settings.target_base_path is required when "
                "target_storage_type='local'"
            )

        # flatten the structure
        resolved_config["storage"] = {
            "target_storage_type": "local",
            "target_base_path": target_base_path,
        }
    else:
        # TODO: update this when more storage types are supported
        logger.error(f"Unsupported storage type detected: {target_storage_type}")
        raise NotImplementedError(
            f"Unsupported target_storage_type: {target_storage_type}. "
            "Currently only 'local' storage is implemented."
        )

    logger.debug(f"Resolved runtime config: {resolved_config}")

    return resolved_config
