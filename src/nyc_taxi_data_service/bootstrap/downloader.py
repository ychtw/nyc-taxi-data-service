import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

from nyc_taxi_data_service.bootstrap.downloader_io import (
    build_request_session,
    download_file,
    load_manifest,
)
from nyc_taxi_data_service.bootstrap.downloader_report import (
    summarize_download_results,
    write_download_log,
)
from nyc_taxi_data_service.bootstrap.downloader_storage import (
    StorageWriter,
    build_storage_writer,
    get_storage_root,
)
from nyc_taxi_data_service.config.config_loader import (
    load_runtime_config,
    resolve_runtime_config,
)
from nyc_taxi_data_service.utils.log_util import setup_logging

logger = logging.getLogger(__name__)  # initialize default logger


def download_manifest_entries(
    manifest_entries: list[dict],
    storage_writer: StorageWriter,
    skip_existing: bool = True,
    max_workers: int = 4,
) -> list[dict]:
    """
    Download all files described by manifest entries.

    Args:
        manifest_entries: list of manifest entry dictionaries
        storage_writer: destination backend used to persist downloaded files
        skip_existing: whether to skip files that already exist
        max_workers: maximum number of worker threads used for parallel downloads

    Returns:
        List of download result dictionaries
    """

    def _download_single(entry: dict) -> dict:
        session = build_request_session()

        result = download_file(
            session=session,
            source_url=entry["source_url"],
            target_relative_path=entry["target_relative_path"],
            storage_writer=storage_writer,
            skip_existing=skip_existing,
        )

        result.update(
            {
                "vehicle_type": entry["vehicle_type"],
                "year_month": entry["year_month"],
                "source_file_name": entry["source_file_name"],
            }
        )

        return result

    logger.info("Starting download of %d entries", len(manifest_entries))
    download_results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_download_single, entry): entry
            for entry in manifest_entries
        }

        for future in tqdm(as_completed(futures), total=len(futures)):
            download_results.append(future.result())

    return download_results


def run(manifest_path: Path | str) -> Path:
    """
    Run the bootstrap downloader using a manifest file.

    Args:
        manifest_path: path to the manifest JSON file

    Returns:
        Path to the written download log file
    """

    logger.info("NYC Taxi Data Downloader started")

    # load manifest file
    manifest = load_manifest(manifest_path)
    logger.info(
        "Manifest metadata: generated_at_utc=%s, active_environment=%s, time_frame=%s, vehicle_types=%s, storage=%s",
        manifest.get("generated_at_utc"),
        manifest.get("active_environment"),
        manifest.get("time_frame"),
        manifest.get("vehicle_types"),
        manifest.get("storage"),
    )

    # execute download & summarize results
    manifest_entries = manifest.get("entries", [])
    storage_config = manifest["storage"]
    storage_writer = build_storage_writer(storage_config)
    download_results = download_manifest_entries(
        manifest_entries=manifest_entries,
        storage_writer=storage_writer,
    )
    download_results.sort(key=lambda x: (x["vehicle_type"], x["year_month"]))
    summary = summarize_download_results(download_results)

    # save download log
    target_identifier = get_storage_root(storage_config)
    log_path = write_download_log(
        download_results=download_results,
        summary=summary,
        manifest_path=manifest_path,
        target_identifier=target_identifier,
    )
    logger.info("Downloader completed successfully")

    return log_path


def main() -> None:
    """
    Load runtime configuration, configure logging, and run the downloader.
    """
    # load & resolve runtime config file
    runtime_config = load_runtime_config()
    resolved_runtime_config = resolve_runtime_config(runtime_config)

    # set up logging based on runtime config setting
    log_level = resolved_runtime_config.get("logging", {}).get("level", "INFO")
    setup_logging(level=log_level)

    # run downloader
    manifest_path = Path("data/manifests/manifest.json")
    run(manifest_path)


if __name__ == "__main__":
    main()
