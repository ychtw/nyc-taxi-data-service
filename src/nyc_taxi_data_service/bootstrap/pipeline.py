import logging

from nyc_taxi_data_service.bootstrap.downloader import run as run_downloader
from nyc_taxi_data_service.bootstrap.manifest_builder import run as run_manifest_builder
from nyc_taxi_data_service.config.config_loader import (
    load_runtime_config,
    load_sources_config,
    resolve_runtime_config,
)
from nyc_taxi_data_service.utils.log_util import setup_logging

logger = logging.getLogger(__name__)  # initialize default logger


def run_bootstrap(sources_config: dict, resolved_runtime_config: dict) -> dict:
    """
    Run the bootstrap pipeline for source data ingestion.

    The bootstrap pipeline builds a manifest from configuration,
    checks remote metadata, downloads required files to target storage,
    and writes a log describing the download results.

    Args:
        sources_config: Parsed sources configuration. Defines available
            data sources, base URLs, file naming patterns, and
            availability constraints.
        resolved_runtime_config: Fully resolved runtime configuration.
            Contains selected vehicle types, time range, storage settings,
            and logging configuration.

    Returns:
        dict with keys:
            manifest_path: Path to generated manifest file.
            log_path: Path to download log file, or None if downloads
                were skipped or failed before log creation.
    """
    # build manifest
    manifest_path = run_manifest_builder(sources_config, resolved_runtime_config)

    # download data based on manifest
    log_path = run_downloader(manifest_path)

    return {
        "manifest_path": manifest_path,
        "log_path": log_path,
    }


def main() -> None:
    # load config files
    sources_config = load_sources_config()
    runtime_config = load_runtime_config()
    resolved_runtime_config = resolve_runtime_config(runtime_config)

    # set up logging based on runtime config setting
    log_level = resolved_runtime_config.get("logging", {}).get("level", "INFO")
    setup_logging(level=log_level)

    # run bootstrap pipeline to download data
    run_bootstrap(sources_config, resolved_runtime_config)


if __name__ == "__main__":
    main()
