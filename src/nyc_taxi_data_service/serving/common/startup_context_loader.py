import json
import os
from pathlib import Path

from nyc_taxi_data_service.config.config_loader import (
    load_runtime_config,
    resolve_runtime_config,
)


def load_service_startup_context(startup_context_path: Path) -> dict:
    """
    Load the shared service startup context from a JSON file.

    Args:
        startup_context_path: Path to service_startup_context.json.

    Returns:
        Parsed startup context dictionary.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file content is not valid JSON.
    """
    try:
        with startup_context_path.open("r", encoding="utf-8") as infile:
            return json.load(infile)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid JSON in startup context file: {startup_context_path}"
        ) from e


def get_startup_context_path_from_env() -> Path | None:
    raw_path = os.getenv("SERVICE_STARTUP_CONTEXT_PATH")
    if raw_path is None:
        return None
    return Path(raw_path)


def load_serving_runtime_config() -> dict:
    """
    Load serving runtime config.

    If SERVICE_STARTUP_CONTEXT_PATH is set, load the startup context JSON.
    Otherwise, load and resolve runtime.yaml.
    """
    startup_context_path = get_startup_context_path_from_env()
    if startup_context_path is not None:
        return load_service_startup_context(startup_context_path)

    runtime_config = load_runtime_config()
    return resolve_runtime_config(runtime_config)
