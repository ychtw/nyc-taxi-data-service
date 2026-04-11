import json
import logging
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

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

    logger.info("Loading manifest from %s", manifest_file)
    with manifest_file.open("r", encoding="utf-8") as f:
        manifest = json.load(f)
    logger.debug("Loaded manifest: %s", manifest)

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
    logger.debug("Resolved target path: %s", full_target_path)

    return full_target_path


def format_bytes(num_bytes: int | None) -> str:
    """
    Convert a byte count into a human-readable string.

    Args:
        num_bytes: number of bytes to format. If None, return "unknown".

    Returns:
        Human-readable byte string such as "1.25 MB"
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


# TODO: optional lightweight disk safety check
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
        logger.info("Disk space check passed: %s available", format_bytes(free_bytes))

    return {
        "target_base_path": str(target_path),
        "required_bytes": required_bytes,
        "free_bytes": free_bytes,
        "enough_space": enough_space,
    }


def download_file(
    session: requests.Session,
    source_url: str,
    target_path: Path | str,
    timeout: int = 30,
    chunk_size: int = 1024 * 1024,
    skip_existing: bool = True,
    cleanup_partial_on_failure: bool = True,
    max_attempts: int = 2,
) -> dict:
    """
    Download a file from source_url to target_path using streamed HTTP GET.

    The file is first written to a temporary `.part` file and only renamed
    to the final target path after the download completes successfully.

    Args:
        session: HTTP session used to issue the download request
        source_url: remote file URL
        target_path: local destination path
        timeout: request timeout in seconds
        chunk_size: bytes per chunk when streaming response
        skip_existing: whether to skip download if target file already exists
        cleanup_partial_on_failure: whether to delete `.part` file on failure
        max_attempts: maximum number of total download attempts

    Returns:
        Dictionary containing the download result and associated metadata
    """
    start_time = time.time()

    target_file = Path(target_path)
    temp_target_file = Path(str(target_file) + ".part")
    target_file.parent.mkdir(parents=True, exist_ok=True)
    logger.debug(
        "Preparing download: source_url=%s, target_path=%s, max_attempts=%d",
        source_url,
        target_file,
        max_attempts,
    )

    # skip download if file already exist
    if skip_existing and target_file.exists():
        logger.info("File exists, skipping: %s", target_file)
        duration = time.time() - start_time
        return {
            "source_url": source_url,
            "target_path": str(target_file),
            "temp_target_path": str(temp_target_file),
            "bytes_written": 0,
            "status": "skipped",
            "error": None,
            "attempts": 0,
            "duration_seconds": duration,
        }

    last_error = None

    for attempt in range(1, max_attempts + 1):  # use 1-based indexing
        bytes_written = 0

        # clean up stale .part file from previous failed run
        if temp_target_file.exists():
            logger.debug("Found stale temp file, removing %s", temp_target_file)
            temp_target_file.unlink()

        try:
            # request data & write to target destination by chuncks
            with session.get(source_url, stream=True, timeout=timeout) as response:
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
                    last_log_time = time.time()
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue

                        f.write(chunk)
                        bytes_written += len(chunk)

                        # progress logging (every ~1 sec)
                        now = time.time()
                        if content_length and (now - last_log_time > 10):
                            percent = (bytes_written / content_length) * 100

                            logger.info(
                                "[%s] progress: %s / %s (%.1f%%)",
                                target_file.name,
                                format_bytes(bytes_written),
                                format_bytes(content_length),
                                percent,
                            )

                            last_log_time = now

            # if download success, move temp file into final target path
            # and return success status
            temp_target_file.replace(target_file)
            logger.info(
                "Download completed: %s (%s)", target_file, format_bytes(bytes_written)
            )
            duration = time.time() - start_time
            return {
                "source_url": source_url,
                "target_path": str(target_file),
                "temp_target_path": str(temp_target_file),
                "bytes_written": bytes_written,
                "status": "downloaded",
                "error": None,
                "attempts": attempt,
                "duration_seconds": duration,
            }

        except (requests.RequestException, OSError, ValueError) as e:
            last_error = str(e)

            if cleanup_partial_on_failure and temp_target_file.exists():
                temp_target_file.unlink()

            if attempt >= max_attempts:
                logger.error(
                    "Failed to download %s after %d attempts. Last error: %s",
                    target_file,
                    max_attempts,
                    last_error,
                )
                duration = time.time() - start_time
                return {
                    "source_url": source_url,
                    "target_path": str(target_file),
                    "temp_target_path": str(temp_target_file),
                    "bytes_written": bytes_written,
                    "status": "failed",
                    "error": last_error,
                    "attempts": attempt,
                    "duration_seconds": duration,
                }

            if isinstance(e, (OSError, ValueError)):
                # if max_attempts not reached, sleep & retry
                retry_delay = min(2**attempt, 10)
                logger.warning(
                    "Download failed for %s [%d/%d]: %s. Retrying in %.1fs.",
                    target_file,
                    attempt,
                    max_attempts,
                    last_error,
                    retry_delay,
                )
                time.sleep(retry_delay)
            else:
                logger.warning(
                    "HTTP download failed for %s [attempt %d/%d]: %s. "
                    "Retrying with session-level backoff.",
                    target_file,
                    attempt,
                    max_attempts,
                    last_error,
                )
                continue


def build_request_session() -> requests.Session:
    """
    Build a reusable HTTP session configured with retry and backoff behavior.

    This session enables:
    - connection pooling across multiple requests
    - automatic retries for transient HTTP errors (e.g., 429, 5xx)
    - exponential backoff between retries
    - respect for server-provided Retry-After headers

    Returns:
        A configured requests.Session instance for robust HTTP downloads.
    """
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        other=0,
        allowed_methods=frozenset({"GET"}),
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=1.0,
        backoff_jitter=0.5,
        backoff_max=60,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10,
    )

    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


def download_manifest_entries(
    manifest_entries: list[dict],
    target_base_path: Path | str,
    skip_existing: bool = True,
    max_workers: int = 4,
) -> list[dict]:
    """
    Download all files described by manifest entries.

    Args:
        manifest_entries: list of manifest entry dictionaries
        target_base_path: root data directory
        skip_existing: whether to skip files that already exist locally
        max_workers: maximum number of worker threads used for parallel downloads

    Returns:
        List of download result dictionaries
    """

    def _download_single(entry: dict) -> dict:
        session = build_request_session()

        full_target_path = resolve_target_path(
            target_base_path=target_base_path,
            target_relative_path=entry["target_relative_path"],
        )

        result = download_file(
            session=session,
            source_url=entry["source_url"],
            target_path=full_target_path,
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


def group_results_by_status(download_results: list[dict]) -> dict:
    """
    Group download results by status.

    Args:
        download_results: list of per-file download results

    Returns:
        Dictionary mapping each status to a list of file identifiers
    """
    grouped = {
        "downloaded": [],
        "skipped": [],
        "failed": [],
    }

    for result in download_results:
        status = result["status"]

        file_id = f"{result['vehicle_type']}/{result['year_month']}/{result['source_file_name']}"

        if status in grouped:
            grouped[status].append(file_id)

    return grouped


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

    grouped_results = group_results_by_status(download_results)

    payload = {
        "generated_at_utc": now.isoformat(),
        "manifest_path": str(manifest_path),
        "target_base_path": str(target_base_path),
        "summary": summary,
        "by_status": grouped_results,
        "results": download_results,
    }

    with log_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    logger.info("Download log written to: %s", log_path)

    return log_path


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
    target_base_path = manifest["storage"]["target_base_path"]
    download_results = download_manifest_entries(
        manifest_entries=manifest_entries,
        target_base_path=target_base_path,
        skip_existing=True,
    )
    summary = summarize_download_results(download_results)

    # save download log
    download_results.sort(key=lambda x: (x["vehicle_type"], x["year_month"]))
    log_path = write_download_log(
        download_results=download_results,
        summary=summary,
        manifest_path=manifest_path,
        target_base_path=target_base_path,
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
