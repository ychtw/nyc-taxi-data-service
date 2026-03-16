import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from nyc_taxi_data_service.config.config_loader import (
    load_runtime_config,
    load_sources_config,
    resolve_runtime_config,
)
from nyc_taxi_data_service.utils.log_util import setup_logging

logger = logging.getLogger(__name__)  # initialize default logger


def generate_year_months(start_year_month: str, end_year_month: str) -> list[str]:
    """
    Generate a list of YYYY-MM strings from start_month up to end_month, inclusive.

    Returns:
        A list of year-month (YYYY-MM)
    """
    logger.info(
        f"Generating year-month range from {start_year_month} to {end_year_month}"
    )

    # validate start & end month
    start_dt = datetime.strptime(start_year_month, "%Y-%m")
    end_dt = datetime.strptime(end_year_month, "%Y-%m")

    start_year = start_dt.year
    start_month_num = start_dt.month
    end_year = end_dt.year
    end_month_num = end_dt.month

    if (start_year, start_month_num) > (end_year, end_month_num):
        logger.error(f"Invalid time range: {start_year_month} > {end_year_month}")
        raise ValueError(
            f"start_year_month {start_year_month} cannot be later than end_year_month {end_year_month}"
        )

    # build all year_month from start_year_month to end_year_month
    year_months = []
    current_year = start_year
    current_month = start_month_num

    while (current_year, current_month) <= (end_year, end_month_num):
        year_months.append(f"{current_year:04d}-{current_month:02d}")

        if current_month == 12:
            # if current_month is Dec, increment year by 1 and reset month to 1
            current_year += 1
            current_month = 1
        else:
            current_month += 1

    logger.debug(f"Generated year-months: {year_months}")

    return year_months


def build_manifest_entries(
    sources_config: dict, resolved_runtime_config: dict
) -> list[dict]:
    """
    Build manifest entries based on:
    - source config dictionary
    - runtime config dictionary, including:
        - requested time range
        - requested vehicle types
        - target storage layout

    Returns:
        A list of dictionary, where each entry represent info associated with a single request
        - vehicle_type
        - year-month
        - source_file_name
        - source_url
        - target_relative_path
    """
    # get source config info
    base_url = sources_config["tlc_website"]["base_url"]
    file_extension = sources_config["tlc_website"]["file_extension"]

    # get resolved config info
    start_year_month = resolved_runtime_config["time_frame"]["start_year_month"]
    end_year_month = resolved_runtime_config["time_frame"]["end_year_month"]
    requested_vehicle_types = resolved_runtime_config["vehicle_types"]

    logger.info(
        "Building manifest for %s, from %s to %s",
        requested_vehicle_types,
        start_year_month,
        end_year_month,
    )

    # generate a list of year-month combination based on resolved config
    year_months = generate_year_months(start_year_month, end_year_month)

    # build manifest
    manifest_entries = []
    for vehicle_type in requested_vehicle_types:
        vehicle_entry_count = 0  # for logging purpose
        vehicle_config = sources_config["vehicle_types"][vehicle_type]
        available_from = vehicle_config["available_from"]
        name_pattern = vehicle_config["name_pattern"]

        logger.info(f"Processing '{vehicle_type}' (available_from={available_from})")

        for year_month in year_months:
            if year_month < available_from:
                logger.debug(
                    "Skipping '%s' for %s: data only available from %s",
                    vehicle_type,
                    year_month,
                    available_from,
                )
                continue

            # construct a manifest entry
            year, month = year_month.split("-")
            source_file_name = f"{name_pattern}_{year_month}.{file_extension}"
            source_url = f"{base_url}/{source_file_name}"
            target_relative_path = (
                f"{vehicle_type}/year={year}/month={month}/{source_file_name}"
            )

            # append manifest entry to list
            manifest_entries.append(
                {
                    "vehicle_type": vehicle_type,
                    "year_month": year_month,
                    "source_file_name": source_file_name,
                    "source_url": source_url,
                    "target_relative_path": target_relative_path,
                }
            )
            vehicle_entry_count += 1

        logger.debug(
            f"Built {vehicle_entry_count} manifest entries for '{vehicle_type}'"
        )

    logger.debug(f"Finished building {len(manifest_entries)} manifest entries")

    return manifest_entries


def write_manifest(
    manifest_entries: list[dict],
    resolved_runtime_config: dict,
    output_path: Path | str,
) -> Path:
    """
    Write manifest entries to a JSON file.

    The manifest records:
    - generation timestamp
    - active environment
    - runtime config snapshot
    - manifest entries

    Args:
        manifest_entries: list of manifest entry dictionaries
        resolved_runtime_config: resolved runtime config used to build manifest
        output_path: directory where manifest file should be written

    Returns:
        Path to the written manifest file
    """

    # ensure output directory exist
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # record current utc timestamp
    timestamp = datetime.now(timezone.utc).isoformat()

    # create output file path
    manifest_file = output_dir / "manifest.json"
    logger.info(
        f"Writing manifest file to: {manifest_file}, with {len(manifest_entries)} entries"
    )

    # form manifest content based on above info
    manifest_payload = {
        "generated_at_utc": timestamp,
        "active_environment": resolved_runtime_config.get("active_environment"),
        "time_frame": resolved_runtime_config.get("time_frame"),
        "vehicle_types": resolved_runtime_config.get("vehicle_types"),
        "storage": resolved_runtime_config.get("storage"),
        "entries": manifest_entries,
    }
    logger.debug(f"Manifest payload: {manifest_payload}")

    # write manifest to target destination
    with manifest_file.open("w", encoding="utf-8") as f:
        json.dump(manifest_payload, f, indent=2)

    return manifest_file


def main() -> None:
    try:
        # load config files
        sources_config = load_sources_config()
        runtime_config = load_runtime_config()
        resolved_runtime_config = resolve_runtime_config(runtime_config)

        # set up logging based on config setting
        log_level = resolved_runtime_config.get("logging", {}).get("level", "INFO")
        setup_logging(level=log_level)
        logger.info("Starting NYC taxi data service manifest builder")

        # build manifest based on configs
        manifest_entries = build_manifest_entries(
            sources_config, resolved_runtime_config
        )

        # write manifest to disk
        manifest_output_dir = resolved_runtime_config["manifest"]["output_path"]
        manifest_path = write_manifest(
            manifest_entries=manifest_entries,
            resolved_runtime_config=resolved_runtime_config,
            output_path=manifest_output_dir,
        )
        logger.info("Manifest builder completed successfully")

    except Exception as e:
        logger.error(f"Manifest generation failed: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
