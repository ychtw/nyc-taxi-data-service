import json
from datetime import datetime, timezone
from importlib.resources import files
from pathlib import Path

import yaml


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

    with config_file.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_runtime_config(config_path: Path | None = None) -> dict:
    """
    Load runtime config, which contains:
    - active environment
    - manifest output path
    - storage config
    - default request time frame
    - default vehicle types

    Args:
        config_path: A path to the runtime config yaml file. If None, use the default `config/runtime.yaml`

    Returns:
        A dictionary of the input yaml config regarding runtime settings
    """
    if config_path is None:
        config_file = files("nyc_taxi_data_service.config").joinpath("runtime.yaml")
    else:
        config_file = Path(config_path)

    with config_file.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve_runtime_config(runtime_config: dict) -> dict:
    """
    Resolve runtime config based on active_environment (dev / prod)

    Args:
        runtime_config: a dictionary of the config regarding runtime settings
                        (may contain both settings for dev and prod)

    Returns:
        A dictionary of the config regarding actual runtime settings to be used in the request
    """
    # get active environment (dev or prod)
    active_environment = runtime_config["active_environment"]

    # get default config
    defaults = runtime_config.get("defaults", {})

    # get config for the active environment
    env_config = runtime_config["environments"][active_environment]

    # update default based on settings of the active environment
    resolved = dict(defaults)
    resolved.update(env_config)
    resolved["active_environment"] = (
        active_environment  # record the active environment in result
    )

    return resolved


def generate_year_months(start_year_month: str, end_year_month: str) -> list[str]:
    """
    Generate a list of YYYY-MM strings from start_month up to end_month, inclusive.

    Returns:
        A list of year-month (YYYY-MM)
    """
    # validate start & end month
    start_dt = datetime.strptime(start_year_month, "%Y-%m")
    end_dt = datetime.strptime(end_year_month, "%Y-%m")

    start_year = start_dt.year
    start_month_num = start_dt.month
    end_year = end_dt.year
    end_month_num = end_dt.month

    if (start_year, start_month_num) > (end_year, end_month_num):
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

    # generate a list of year-month combination based on resolved config
    year_months = generate_year_months(start_year_month, end_year_month)

    # build manifest
    manifest_entries = []
    for vehicle_type in requested_vehicle_types:
        vehicle_config = sources_config["vehicle_types"][vehicle_type]
        available_from = vehicle_config["available_from"]
        name_pattern = vehicle_config["name_pattern"]

        for year_month in year_months:
            if year_month < available_from:
                continue

            year, month = year_month.split("-")
            source_file_name = f"{name_pattern}_{year_month}.{file_extension}"
            source_url = f"{base_url}/{source_file_name}"
            target_relative_path = (
                f"{vehicle_type}/year={year}/month={month}/{source_file_name}"
            )

            manifest_entries.append(
                {
                    "vehicle_type": vehicle_type,
                    "year_month": year_month,
                    "source_file_name": source_file_name,
                    "source_url": source_url,
                    "target_relative_path": target_relative_path,
                }
            )

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

    # form manifest content based on above info
    manifest_payload = {
        "generated_at_utc": timestamp,
        "active_environment": resolved_runtime_config.get("active_environment"),
        "time_frame": resolved_runtime_config.get("time_frame"),
        "vehicle_types": resolved_runtime_config.get("vehicle_types"),
        "storage": resolved_runtime_config.get("storage"),
        "entries": manifest_entries,
    }

    # write manifest to target destination
    with manifest_file.open("w", encoding="utf-8") as f:
        json.dump(manifest_payload, f, indent=2)

    return manifest_file


def main() -> None:
    # load config files
    sources_config = load_sources_config()
    runtime_config = load_runtime_config()
    resolved_runtime_config = resolve_runtime_config(runtime_config)

    # build manifest based on configs
    manifest_entries = build_manifest_entries(sources_config, resolved_runtime_config)

    print("=== full sources config ===")
    print(sources_config)

    print("\n=== full runtime config ===")
    print(runtime_config)

    print("\n=== resolved_runtime_config ===")
    print(resolved_runtime_config)

    print("\n=== generated year_months ===")
    print(
        generate_year_months(
            start_year_month=resolved_runtime_config["time_frame"]["start_year_month"],
            end_year_month=resolved_runtime_config["time_frame"]["end_year_month"],
        )
    )

    print("\n=== manifest entries ===")
    for entry in manifest_entries:
        print(entry)

    print("\n=== manifest summary ===")
    print("total entries:", len(manifest_entries))

    # write manifest to disk
    manifest_output_dir = resolved_runtime_config["manifest"]["output_path"]

    print("\n=== writing manifest ===")
    print("output_dir:", manifest_output_dir)

    manifest_path = write_manifest(
        manifest_entries=manifest_entries,
        resolved_runtime_config=resolved_runtime_config,
        output_path=manifest_output_dir,
    )

    print("manifest written to:", manifest_path)


if __name__ == "__main__":
    main()
