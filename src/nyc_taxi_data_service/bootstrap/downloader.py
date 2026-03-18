import json
import logging
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from nyc_taxi_data_service.config.config_loader import (
    load_runtime_config,
    resolve_runtime_config,
)
from nyc_taxi_data_service.utils.log_util import setup_logging

logger = logging.getLogger(__name__)  # initialize default logger


def load_manifest(manifest_path: Path | str) -> dict:
    """
    Load a manifest JSON file.

    Args:
        manifest_path: path to the manifest JSON file

    Returns:
        Parsed manifest as a dictionary
    """
    manifest_file = Path(manifest_path)

    logger.info(f"Loading manifest from {manifest_file}")
    with manifest_file.open("r", encoding="utf-8") as f:
        manifest = json.load(f)
    logger.debug(f"Loaded manifest: {manifest}")

    return manifest


def resolve_target_path(
    target_base_path: Path | str,
    target_relative_path: str,
) -> Path:
    """
    Resolve full target path from base path and relative path.

    Args:
        target_base_path: root data directory
        target_relative_path: relative path under target_base_path

    Returns:
        Full target path where file should be downloaded
    """
    full_target_path = Path(target_base_path) / target_relative_path
    logger.debug(f"Resolved target path: {full_target_path}")

    return full_target_path


def get_remote_file_metadata(source_url: str, timeout: int = 30) -> dict:
    """
    Fetch remote file metadata using an HTTP HEAD request.

    Args:
        source_url: source file URL
        timeout: request timeout in seconds

    Returns:
        A dictionary containing remote file metadata
    """
    logger.debug(f"Fetching remote metadata for {source_url}")

    try:
        response = requests.head(source_url, allow_redirects=True, timeout=timeout)

        content_length_raw = response.headers.get("Content-Length")
        content_length = int(content_length_raw) if content_length_raw else None

        metadata = {
            "source_url": source_url,
            "status_code": response.status_code,
            "ok": response.ok,
            "content_length": content_length,
            "content_type": response.headers.get("Content-Type"),
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
        }
        logger.debug(f"Remote metadata fetched: {metadata}")

        return metadata

    except requests.RequestException as e:
        logger.error(f"Failed to fetch metadata for {source_url}: {str(e)}")
        raise


# TODO: current version assumes manifest_entries and remote_metadata_list follow the same order
def attach_remote_metadata_to_entries(
    manifest_entries: list[dict],
    remote_metadata_list: list[dict],
) -> list[dict]:
    """
    Attach remote metadata to each manifest entry.
    Note: assumes manifest_entries and remote_metadata_list follow the same order

    Args:
        manifest_entries: list of manifest entry dictionaries
        remote_metadata_list: list of remote metadata dictionaries in matching order

    Returns:
        New list of manifest entries enriched with remote metadata
    """
    logger.debug(
        "Attaching %d remote metadata to %d manifest entries",
        len(remote_metadata_list),
        len(manifest_entries),
    )
    enriched_entries = []

    for entry, metadata in zip(manifest_entries, remote_metadata_list, strict=False):
        enriched_entry = dict(entry)
        enriched_entry["remote_metadata"] = metadata
        enriched_entries.append(enriched_entry)

    return enriched_entries


def format_bytes(num_bytes: int | None) -> str:
    """
    Convert byte count into a human-readable string (for logging purpose only).
    """
    if num_bytes is None:
        return "unknown"

    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)

    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024

    return f"{num_bytes} B"


def get_total_required_bytes(remote_metadata_list: list[dict]) -> int:
    """
    Sum content_length across remote source files.

    Args:
        remote_metadata_list: list of remote metadata dictionaries

    Returns:
        Total required bytes
    """
    total_bytes = 0

    for metadata in remote_metadata_list:
        response_ok = metadata.get("ok")
        content_length = metadata.get("content_length")
        if response_ok and content_length is not None:
            total_bytes += content_length

    logger.info(
        "Total download size: %s for %d files",
        format_bytes(total_bytes),
        len(remote_metadata_list),
    )

    return total_bytes


def check_local_free_space(
    target_base_path: Path | str,
    required_bytes: int,
) -> dict:
    """
    Check whether local filesystem has enough free space.
    Only used when the target storage type is local.

    Args:
        target_base_path: local base directory for downloads
        required_bytes: total bytes required for planned downloads

    Returns:
        Dictionary containing disk usage check result
    """
    target_path = Path(target_base_path)
    target_path.mkdir(parents=True, exist_ok=True)

    usage = shutil.disk_usage(target_path)
    free_bytes = usage.free
    enough_space = free_bytes >= required_bytes

    if not enough_space:
        logger.error(
            "Insufficient disk space at %s. (Required: %s. Available: %s)",
            target_path,
            format_bytes(required_bytes),
            format_bytes(free_bytes),
        )
    else:
        logger.info(f"Disk space check passed: {format_bytes(free_bytes)} available")

    return {
        "target_base_path": str(target_path),
        "required_bytes": required_bytes,
        "free_bytes": free_bytes,
        "enough_space": enough_space,
    }


def download_file(
    source_url: str,
    target_path: Path | str,
    timeout: int = 30,
    chunk_size: int = 1024 * 1024,
    skip_existing: bool = True,
    cleanup_partial_on_failure: bool = True,
    expected_size_bytes: int | None = None,
    max_attempts: int = 3,
    retry_delay_seconds: float = 2.0,
) -> dict:
    """
    Download a file from source_url to target_path using streamed HTTP GET.

    The file is first written to a temporary `.part` file and only renamed
    to the final target path after the download completes successfully.

    Args:
        source_url: remote file URL
        target_path: local destination path
        timeout: request timeout in seconds
        chunk_size: bytes per chunk when streaming response
        skip_existing: whether to skip download if target file already exists
        cleanup_partial_on_failure: whether to delete `.part` file on failure
        expected_size_bytes: expected final file size in bytes. If provided,
            validate that the completed download matches this size before
            renaming the temporary file.
        max_attempts: maximum number of total download attempts
        retry_delay_seconds: number of seconds to wait before the next attempt
            after a failure.

    Returns:
        Dictionary describing download result
    """
    target_file = Path(target_path)
    temp_target_file = Path(str(target_file) + ".part")
    target_file.parent.mkdir(parents=True, exist_ok=True)
    logger.debug(
        "Preparing download: source_url=%s, target_path=%s, expected_size=%s, max_attempts=%d",
        source_url,
        target_file,
        format_bytes(expected_size_bytes),
        max_attempts,
    )

    # skip download if file already exist and match the expected size
    if skip_existing and target_file.exists():
        existing_size = target_file.stat().st_size
        if expected_size_bytes is None or existing_size == expected_size_bytes:
            logger.info(f"File exists and size matches, skipping: {target_file}")
            return {
                "source_url": source_url,
                "target_path": str(target_file),
                "temp_target_path": str(temp_target_file),
                "bytes_written": 0,
                "existing_file_size": existing_size,
                "expected_size_bytes": expected_size_bytes,
                "status": "skipped",
                "error": None,
            }

        logger.warning(
            "Size mismatch for %s, redownloading (Local: %s, Remote: %s)",
            target_file,
            format_bytes(existing_size),
            format_bytes(expected_size_bytes),
        )

    last_error = None

    for attempt in range(1, max_attempts + 1):  # use 1-based indexing
        bytes_written = 0

        # clean up stale .part file from previous failed run
        if temp_target_file.exists():
            logger.debug(f"Found stale temp file, removing {temp_target_file}")
            temp_target_file.unlink()

        try:
            # request data & write to target destination by chuncks
            with requests.get(source_url, stream=True, timeout=timeout) as response:
                response.raise_for_status()

                content_length_raw = response.headers.get("Content-Length")
                content_length = int(content_length_raw) if content_length_raw else None
                logger.info(
                    "Downloading %s (size: %s) [Attempt %d/%d]",
                    target_file,
                    format_bytes(content_length),
                    attempt,
                    max_attempts,
                )

                with temp_target_file.open("wb") as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        f.write(chunk)
                        bytes_written += len(chunk)

            # validate if bytes_written match with expected_size_bytes
            # if not, raise ValueError (will retry if max_retries not reached)
            if expected_size_bytes is not None and bytes_written != expected_size_bytes:
                raise ValueError(
                    f"downloaded size mismatch: wrote {bytes_written} bytes, "
                    f"expected {expected_size_bytes} bytes"
                )

            # if download success, move temp file into final target path
            # and return success status
            temp_target_file.replace(target_file)
            logger.info(
                f"Download completed: {target_file} ({format_bytes(bytes_written)})"
            )
            return {
                "source_url": source_url,
                "target_path": str(target_file),
                "temp_target_path": str(temp_target_file),
                "bytes_written": bytes_written,
                "expected_size_bytes": expected_size_bytes,
                "status": "downloaded",
                "error": None,
            }

        except (requests.RequestException, OSError, ValueError) as e:
            last_error = str(e)

            # remove temporary files
            if cleanup_partial_on_failure and temp_target_file.exists():
                temp_target_file.unlink()

            # if reached max_attempts, return failed status
            if attempt >= max_attempts:
                logger.error(
                    "Failed to download %s after %d attempts. Last error: %s",
                    target_file,
                    max_attempts,
                    last_error,
                )
                return {
                    "source_url": source_url,
                    "target_path": str(target_file),
                    "temp_target_path": str(temp_target_file),
                    "bytes_written": bytes_written,
                    "expected_size_bytes": expected_size_bytes,
                    "status": "failed",
                    "error": last_error,
                }

            # if max_attempts not reached, sleep & retry
            logger.warning(
                "Download failed for %s [attempt %d/%d]: %s. Retrying in %d seconds.",
                target_file,
                attempt,
                max_attempts,
                last_error,
                retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)

    # fallback return statement (usually will never be reached)
    logger.error(
        "Reached unexpected fallback return in download_file for %s. Last error: %s",
        target_file,
        last_error,
    )
    return {
        "source_url": source_url,
        "target_path": str(target_file),
        "temp_target_path": str(temp_target_file),
        "bytes_written": 0,
        "expected_size_bytes": expected_size_bytes,
        "status": "failed",
        "error": last_error,
    }


# TODO: allow download in parallel
def download_manifest_entries(
    manifest_entries: list[dict],
    target_base_path: Path | str,
    skip_existing: bool = True,
) -> list[dict]:
    """
    Download all files described by manifest entries.

    Args:
        manifest_entries: list of manifest entry dictionaries
        target_base_path: root data directory
        skip_existing: whether to skip files that already exist locally

    Returns:
        List of download result dictionaries
    """
    logger.info(f"Starting download of {len(manifest_entries)} entries")
    download_results = []

    for index, entry in enumerate(manifest_entries, start=1):
        logger.debug(f"Processing manifest entry {index}/{len(manifest_entries)}")
        full_target_path = resolve_target_path(
            target_base_path=target_base_path,
            target_relative_path=entry["target_relative_path"],
        )

        result = download_file(
            source_url=entry["source_url"],
            target_path=full_target_path,
            skip_existing=skip_existing,
            expected_size_bytes=entry.get("remote_metadata", {}).get("content_length"),
        )

        result.update(
            {
                "vehicle_type": entry["vehicle_type"],
                "year_month": entry["year_month"],
                "source_file_name": entry["source_file_name"],
            }
        )

        download_results.append(result)

    return download_results


def summarize_download_results(download_results: list[dict]) -> dict:
    """
    Summarize download results by status.

    Args:
        download_results: list of per-file download results

    Returns:
        Summary dictionary
    """
    summary = {
        "downloaded": 0,
        "skipped": 0,
        "failed": 0,
        "total_bytes_written": 0,
    }

    for result in download_results:
        status = result["status"]
        if status in summary:
            summary[status] += 1

        summary["total_bytes_written"] += result.get("bytes_written", 0)

    logger.info(
        "Download summary: downloaded=%d, skipped=%d, failed=%d, total_bytes_written=%d",
        summary["downloaded"],
        summary["skipped"],
        summary["failed"],
        summary["total_bytes_written"],
    )

    return summary


def write_download_log(
    download_results: list[dict],
    summary: dict,
    manifest_path: Path | str,
    target_base_path: Path | str,
    output_dir: Path | str = "data/logs",
) -> Path:
    """
    Write download results to a JSON log file.

    Args:
        download_results: list of per-file results
        summary: summary dictionary
        manifest_path: path to manifest used
        target_base_path: base directory for downloads
        output_dir: directory to store log files

    Returns:
        Path to written log file
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)

    timestamp_str = now.strftime("%Y%m%dT%H%M%SZ")
    log_path = output_dir / f"download_log_{timestamp_str}.json"

    payload = {
        "generated_at_utc": now.isoformat(),
        "manifest_path": str(manifest_path),
        "target_base_path": str(target_base_path),
        "summary": summary,
        "results": download_results,
    }

    with log_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    logger.info(f"Download log written to: {log_path}")

    return log_path


def main() -> None:
    # load config files
    runtime_config = load_runtime_config()
    resolved_runtime_config = resolve_runtime_config(runtime_config)

    # set up logging based on config setting
    log_level = resolved_runtime_config.get("logging", {}).get("level", "INFO")
    setup_logging(level=log_level)
    logger.info("NYC Taxi Data Downloader started")

    # load manifest file
    manifest_path = Path("data/manifests/manifest.json")
    manifest = load_manifest(manifest_path)
    logger.info(
        "Manifest metadata: generated_at_utc=%s, active_environment=%s, time_frame=%s, vehicle_types=%s, storage=%s",
        manifest.get("generated_at_utc"),
        manifest.get("active_environment"),
        manifest.get("time_frame"),
        manifest.get("vehicle_types"),
        manifest.get("storage"),
    )

    # get remote file metadata and do space check (if on local)
    manifest_entries = manifest.get("entries", [])
    remote_metadata_list = [
        get_remote_file_metadata(entry["source_url"]) for entry in manifest_entries
    ]
    required_bytes = get_total_required_bytes(remote_metadata_list)

    target_storage_type = manifest["storage"]["target_storage_type"]
    target_base_path = manifest["storage"]["target_base_path"]
    if target_storage_type == "local":
        space_check = check_local_free_space(
            target_base_path=target_base_path,
            required_bytes=required_bytes,
        )
        if not space_check["enough_space"]:
            logger.critical(
                "Aborting download due to insufficient disk space (required: %s. free space: %s)",
                format_bytes(space_check["required_bytes"]),
                format_bytes(space_check["free_bytes"]),
            )
            return
    else:
        logger.info(
            f"Skipping local disk check for storage type: {target_storage_type}"
        )

    # execute download & summarize results
    manifest_entries_with_metadata = attach_remote_metadata_to_entries(
        manifest_entries=manifest_entries,
        remote_metadata_list=remote_metadata_list,
    )
    download_results = download_manifest_entries(
        manifest_entries=manifest_entries_with_metadata,
        target_base_path=target_base_path,
        skip_existing=True,
    )
    summary = summarize_download_results(download_results)

    # save download log
    log_path = write_download_log(
        download_results=download_results,
        summary=summary,
        manifest_path=manifest_path,
        target_base_path=target_base_path,
    )
    logger.info("Downloader completed successfully")


if __name__ == "__main__":
    main()
