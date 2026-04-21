import json
import logging
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nyc_taxi_data_service.bootstrap.downloader_storage import StorageWriter

logger = logging.getLogger(__name__)  # initialize default logger


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


def build_request_session() -> requests.Session:
    """
    Build a reusable HTTP session configured with retry and backoff behavior.

    This session is responsible for retrying transient HTTP/network failures
    such as connection issues, 429 responses, and 5xx responses.
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


def download_file(
    session: requests.Session,
    source_url: str,
    target_relative_path: str,
    storage_writer: StorageWriter,
    timeout: int = 30,
    chunk_size: int = 1024 * 1024,
    skip_existing: bool = True,
    cleanup_partial_on_failure: bool = True,
) -> dict:
    """
    Download a file from source_url and persist it using a storage writer.

    HTTP retries are handled by the configured requests session. This function
    performs a single download/persist attempt and does not implement an
    additional outer retry loop.

    Args:
        session: HTTP session used to issue the download request
        source_url: remote file URL
        target_relative_path: relative destination path from the manifest entry
        storage_writer: destination backend used to finalize the file
        timeout: request timeout in seconds
        chunk_size: bytes per chunk when streaming response
        skip_existing: whether to skip download if target already exists
        cleanup_partial_on_failure: whether to delete the temporary file on failure

    Returns:
        Dictionary containing the download result and associated metadata
    """
    start_time = time.time()

    target_identifier = storage_writer.get_target_identifier(target_relative_path)

    temp_dir = Path(".tmp/downloads")
    temp_dir.mkdir(parents=True, exist_ok=True)
    safe_name = target_relative_path.replace("/", "__")
    temp_target_file = temp_dir / f"{safe_name}.part"

    logger.debug(
        "Preparing download: source_url=%s, target=%s",
        source_url,
        target_identifier,
    )

    if skip_existing and storage_writer.exists(target_relative_path):
        logger.info("File exists, skipping: %s", target_identifier)
        duration = time.time() - start_time
        return {
            "source_url": source_url,
            "target_path": target_identifier,
            "temp_target_path": str(temp_target_file),
            "bytes_written": 0,
            "status": "skipped",
            "error": None,
            "attempts": 0,
            "duration_seconds": duration,
        }

    bytes_written = 0

    if temp_target_file.exists():
        logger.debug("Found stale temp file, removing %s", temp_target_file)
        temp_target_file.unlink()

    try:
        with session.get(source_url, stream=True, timeout=timeout) as response:
            response.raise_for_status()

            content_length_raw = response.headers.get("Content-Length")
            content_length = int(content_length_raw) if content_length_raw else None
            logger.info(
                "Downloading %s (size: %s)",
                target_identifier,
                format_bytes(content_length),
            )

            with temp_target_file.open("wb") as f:
                last_log_time = time.time()
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue

                    f.write(chunk)
                    bytes_written += len(chunk)

                    now = time.time()
                    if content_length and (now - last_log_time > 10):
                        percent = (bytes_written / content_length) * 100
                        logger.info(
                            "[%s] progress: %s / %s (%.1f%%)",
                            Path(target_relative_path).name,
                            format_bytes(bytes_written),
                            format_bytes(content_length),
                            percent,
                        )
                        last_log_time = now

        final_target = storage_writer.persist_file(
            temp_file_path=temp_target_file,
            target_relative_path=target_relative_path,
        )

        logger.info(
            "Download completed: %s (%s)",
            final_target,
            format_bytes(bytes_written),
        )
        duration = time.time() - start_time
        return {
            "source_url": source_url,
            "target_path": final_target,
            "temp_target_path": str(temp_target_file),
            "bytes_written": bytes_written,
            "status": "downloaded",
            "error": None,
            "attempts": 1,
            "duration_seconds": duration,
        }

    except (requests.RequestException, OSError, ValueError) as e:
        last_error = str(e)

        if cleanup_partial_on_failure and temp_target_file.exists():
            temp_target_file.unlink()

        logger.error(
            "Failed to download %s. Last error: %s",
            target_identifier,
            last_error,
        )
        duration = time.time() - start_time
        return {
            "source_url": source_url,
            "target_path": target_identifier,
            "temp_target_path": str(temp_target_file),
            "bytes_written": bytes_written,
            "status": "failed",
            "error": last_error,
            "attempts": 1,
            "duration_seconds": duration,
        }
