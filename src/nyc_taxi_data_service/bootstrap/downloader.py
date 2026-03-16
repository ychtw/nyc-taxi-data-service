import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


def load_manifest(manifest_path: Path | str) -> dict:
    """
    Load a manifest JSON file.

    Args:
        manifest_path: path to the manifest JSON file

    Returns:
        Parsed manifest as a dictionary
    """
    manifest_file = Path(manifest_path)

    with manifest_file.open("r", encoding="utf-8") as f:
        return json.load(f)


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
    return Path(target_base_path) / target_relative_path


def get_remote_file_metadata(source_url: str, timeout: int = 30) -> dict:
    """
    Fetch remote file metadata using an HTTP HEAD request.

    Args:
        source_url: source file URL
        timeout: request timeout in seconds

    Returns:
        A dictionary containing remote file metadata
    """
    response = requests.head(source_url, allow_redirects=True, timeout=timeout)

    content_length_raw = response.headers.get("Content-Length")
    content_length = int(content_length_raw) if content_length_raw else None

    return {
        "source_url": source_url,
        "status_code": response.status_code,
        "ok": response.ok,
        "content_length": content_length,
        "content_type": response.headers.get("Content-Type"),
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
    }


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
        content_length = metadata.get("content_length")
        if content_length is not None:
            total_bytes += content_length

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

    # skip download if file already exist and match the expected size
    if skip_existing and target_file.exists():
        existing_size = target_file.stat().st_size
        if expected_size_bytes is None or existing_size == expected_size_bytes:
            print(
                f"target file already exists with expected size, skip download {target_file}"
            )
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

        print(
            f"target file exists but size mismatch, re-downloading {target_file} "
            f"(existing={format_bytes(existing_size)}, "
            f"expected={format_bytes(expected_size_bytes)})"
        )

    last_error = None

    for attempt in range(1, max_attempts + 1):  # use 1-based indexing
        bytes_written = 0

        # clean up stale .part file from previous failed run
        if temp_target_file.exists():
            print(f"stale temp file found, removing {temp_target_file}")
            temp_target_file.unlink()

        try:
            # request data & write to target destination by chuncks
            with requests.get(source_url, stream=True, timeout=timeout) as response:
                response.raise_for_status()

                content_length_raw = response.headers.get("Content-Length")
                content_length = int(content_length_raw) if content_length_raw else None
                print(
                    f"downloading {source_url} "
                    f"(size: {format_bytes(content_length)}) "
                    f"[attempt {attempt}/{max_attempts}]"
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
            print(
                f"download failed (attempt {attempt}/{max_attempts}): {e}. "
                f"Retrying in {retry_delay_seconds}s"
            )
            time.sleep(retry_delay_seconds)

    # fallback return statement (usually will never be reached)
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
    download_results = []

    for entry in manifest_entries:
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

    return log_path


def main() -> None:
    manifest_path = Path("data/manifests/manifest.json")

    print("=== loading manifest ===")
    print("manifest_path:", manifest_path)

    manifest = load_manifest(manifest_path)

    print("\n=== manifest metadata ===")
    print("generated_at_utc:", manifest.get("generated_at_utc"))
    print("active_environment:", manifest.get("active_environment"))
    print("time_frame:", manifest.get("time_frame"))
    print("vehicle_types:", manifest.get("vehicle_types"))
    print("storage:", manifest.get("storage"))

    manifest_entries = manifest.get("entries", [])

    print("\n=== manifest summary ===")
    print("total manifest entries:", len(manifest_entries))

    target_base_path = manifest["storage"]["target_base_path"]

    print("\n=== resolved target paths ===")
    for entry in manifest_entries:
        full_target_path = resolve_target_path(
            target_base_path=target_base_path,
            target_relative_path=entry["target_relative_path"],
        )
        print(
            {
                "vehicle_type": entry["vehicle_type"],
                "year_month": entry["year_month"],
                "source_url": entry["source_url"],
                "full_target_path": str(full_target_path),
            }
        )

    print("\n=== remote file metadata ===")
    remote_metadata_list = []
    for entry in manifest_entries:
        metadata = get_remote_file_metadata(entry["source_url"])
        remote_metadata_list.append(metadata)
        print(
            {
                "vehicle_type": entry["vehicle_type"],
                "year_month": entry["year_month"],
                "status_code": metadata["status_code"],
                "content_length": format_bytes(metadata["content_length"]),
                "last_modified": metadata["last_modified"],
            }
        )

    required_bytes = get_total_required_bytes(remote_metadata_list)

    print("\n=== total required download size ===")
    print("total required space:", format_bytes(required_bytes))

    space_check = check_local_free_space(
        target_base_path=target_base_path,
        required_bytes=required_bytes,
    )

    print("\n=== local disk space check ===")
    print("required_space:", format_bytes(space_check["required_bytes"]))
    print("free_space:", format_bytes(space_check["free_bytes"]))
    print("has enough space:", space_check["enough_space"])

    print("\n=== downloading files from manifest ===")
    manifest_entries_with_metadata = attach_remote_metadata_to_entries(
        manifest_entries=manifest_entries,
        remote_metadata_list=remote_metadata_list,
    )
    download_results = download_manifest_entries(
        manifest_entries=manifest_entries_with_metadata,
        target_base_path=target_base_path,
        skip_existing=True,
    )

    print("\n=== download results ===")
    for result in download_results:
        print(
            {
                "vehicle_type": result["vehicle_type"],
                "year_month": result["year_month"],
                "status": result["status"],
                "bytes_written": format_bytes(result["bytes_written"]),
                "target_path": result["target_path"],
                "error": result["error"],
            }
        )

    summary = summarize_download_results(download_results)

    print("\n=== download summary ===")
    print("downloaded:", summary["downloaded"])
    print("skipped:", summary["skipped"])
    print("failed:", summary["failed"])
    print("total_bytes_written:", format_bytes(summary["total_bytes_written"]))

    print("\n=== writing download log ===")
    log_path = write_download_log(
        download_results=download_results,
        summary=summary,
        manifest_path=manifest_path,
        target_base_path=target_base_path,
    )
    print("log written to:", log_path)


if __name__ == "__main__":
    main()
