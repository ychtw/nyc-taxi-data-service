import json
import logging
import os
import tempfile
from copy import deepcopy
from datetime import date
from pathlib import Path

from nyc_taxi_data_service.config.config_loader import (
    load_runtime_config,
    resolve_runtime_config,
)
from nyc_taxi_data_service.serving.common.log_time import format_log_dt
from nyc_taxi_data_service.serving.common.service_clock import ServiceClock
from nyc_taxi_data_service.utils.log_util import setup_logging

logger = logging.getLogger(__name__)


def build_service_startup_context(resolved_runtime_config: dict) -> dict:
    """
    Build the startup context shared by the streaming and API services.

    This helper:
    - reads `serving.mimic_current_date` from the resolved runtime config
    - creates a new shared ServiceClock anchor for this service session
    - returns a copy of the resolved runtime config with
      `serving.service_clock_anchor` added

    The resulting dictionary is intended to be serialized and later read by
    both the streaming service and the API service so they can reconstruct
    identical ServiceClock instances via `ServiceClock.from_existing_anchor(...)`.

    Args:
        resolved_runtime_config: Fully resolved runtime config dictionary.

    Returns:
        A new dictionary containing the resolved runtime config plus the
        `serving.service_clock_anchor` section.

    Raises:
        ValueError:
            If `serving.mimic_current_date` is missing or not a valid ISO date.
    """
    startup_context = deepcopy(resolved_runtime_config)

    raw_mimic_current_date = startup_context.get("serving", {}).get(
        "mimic_current_date"
    )
    if raw_mimic_current_date is None:
        raise ValueError(
            "serving.mimic_current_date must be set in resolved runtime config"
        )

    mimic_current_date = date.fromisoformat(raw_mimic_current_date)

    service_clock = ServiceClock.from_new_session(mimic_current_date=mimic_current_date)

    startup_context["serving"]["service_clock_anchor"] = {
        "real_service_start_time_utc": service_clock.real_service_start_time.isoformat(),
        "mimic_service_start_time": service_clock.mimic_service_start_time.isoformat(),
    }

    logger.info(
        "Built service startup context | mimic_current_date=%s",
        mimic_current_date,
    )
    logger.debug(
        "Service clock anchor | real_service_start_time_utc=%s | mimic_service_start_time=%s",
        format_log_dt(service_clock.real_service_start_time),
        format_log_dt(service_clock.mimic_service_start_time),
    )

    return startup_context


def write_service_startup_context(
    startup_context: dict,
    output_path: Path,
) -> None:
    """
    Write the service startup context to a JSON file atomically.

    The file is first written to a temporary file in the same directory and
    then atomically replaced onto the target path. This prevents readers from
    observing a partially written startup context file.

    Args:
        startup_context: Startup context dictionary returned by
            `build_service_startup_context(...)`.
        output_path: Target JSON file path.

    Returns:
        None
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=output_path.parent,
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as tmp_file:
        temp_path = Path(tmp_file.name)
        try:
            json.dump(startup_context, tmp_file, indent=4)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise

    temp_path.replace(output_path)

    logger.info("Service startup context saved to: %s", output_path)


def main() -> None:
    """
    Build and persist the serving startup context.

    This entry point:
    - loads runtime config
    - resolves environment-specific runtime settings
    - configures logging
    - creates a shared ServiceClock anchor for the current service session
    - writes the combined startup context to JSON for later use by the
      streaming and API services
    """
    # load & resolve runtime config
    runtime_config = load_runtime_config()
    resolved_runtime_config = resolve_runtime_config(runtime_config)

    # set up logging based on runtime config setting
    log_level = resolved_runtime_config.get("logging", {}).get("level", "INFO")
    setup_logging(level=log_level)

    # build service start up context by combining
    # resolved runtime config with anchor service times
    startup_context = build_service_startup_context(
        resolved_runtime_config=resolved_runtime_config
    )

    # write startup context as a json file
    output_path = (
        Path(resolved_runtime_config["manifest"]["output_path"])
        / "service_startup_context.json"
    )
    write_service_startup_context(
        startup_context=startup_context,
        output_path=output_path,
    )


if __name__ == "__main__":
    main()
