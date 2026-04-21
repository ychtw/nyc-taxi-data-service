import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)  # initialize default logger


def write_download_log(
    download_results: list[dict],
    summary: dict,
    manifest_path: Path | str,
    target_identifier: str,
    output_dir: Path | str = "data/logs",
) -> Path:
    """
    Write download results to a JSON log file.

    Args:
        download_results: list of per-file results
        summary: summary dictionary
        manifest_path: path to manifest used
        target_identifier: destination identifier (e.g., local path or S3 URI prefix)
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
        "target_identifier": target_identifier,
        "summary": summary,
        "by_status": grouped_results,
        "results": download_results,
    }

    with log_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    logger.info("Download log written to: %s", log_path)

    return log_path


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
