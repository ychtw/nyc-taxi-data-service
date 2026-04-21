import json
import re
from pathlib import Path

import pytest

from nyc_taxi_data_service.bootstrap.downloader_report import (
    group_results_by_status,
    summarize_download_results,
    write_download_log,
)

# ---------------------------
# Fixtures
# ---------------------------


@pytest.fixture
def sample_download_results():
    """Provide representative download results."""
    return [
        {
            "status": "downloaded",
            "bytes_written": 100,
            "vehicle_type": "yellow",
            "year_month": "2025-01",
            "source_file_name": "yellow.parquet",
        },
        {
            "status": "failed",
            "bytes_written": 0,
            "vehicle_type": "green",
            "year_month": "2025-01",
            "source_file_name": "green.parquet",
        },
        {
            "status": "skipped",
            "bytes_written": 0,
            "vehicle_type": "fhv",
            "year_month": "2025-01",
            "source_file_name": "fhv.parquet",
        },
    ]


@pytest.fixture
def sample_summary():
    """Provide a representative summary payload."""
    return {
        "downloaded": 1,
        "skipped": 1,
        "failed": 1,
        "total_bytes_written": 100,
    }


@pytest.fixture
def manifest_path(tmp_path):
    """Provide a manifest path for log-writing tests."""
    return tmp_path / "manifest.json"


# ---------------------------
# write_download_log tests
# ---------------------------


def test_write_download_log_creates_log_file_and_returns_path(
    tmp_path, manifest_path, sample_download_results, sample_summary
):
    """write_download_log should create a timestamped log file."""
    output_dir = tmp_path / "logs"

    log_path = write_download_log(
        download_results=sample_download_results,
        summary=sample_summary,
        manifest_path=manifest_path,
        target_identifier="s3://test-bucket/raw",
        output_dir=output_dir,
    )

    assert isinstance(log_path, Path)
    assert log_path.exists()
    assert log_path.parent == output_dir
    assert re.match(r"download_log_\d{8}T\d{6}Z\.json", log_path.name)


def test_write_download_log_creates_output_directory(
    tmp_path, manifest_path, sample_summary
):
    """write_download_log should create the output directory if missing."""
    output_dir = tmp_path / "nested" / "logs"

    assert not output_dir.exists()

    log_path = write_download_log(
        download_results=[],
        summary=sample_summary | {"downloaded": 0, "skipped": 0, "failed": 0},
        manifest_path=manifest_path,
        target_identifier=str(tmp_path),
        output_dir=output_dir,
    )

    assert output_dir.exists()
    assert log_path.exists()


def test_write_download_log_uses_default_output_dir(
    tmp_path, monkeypatch, manifest_path
):
    """write_download_log should use the default output directory when omitted."""
    monkeypatch.chdir(tmp_path)

    log_path = write_download_log(
        download_results=[],
        summary={
            "downloaded": 0,
            "skipped": 0,
            "failed": 0,
            "total_bytes_written": 0,
        },
        manifest_path=manifest_path,
        target_identifier=str(tmp_path),
    )

    assert log_path.exists()
    assert log_path.parent == Path("data/logs")


def test_write_download_log_contains_expected_payload(
    tmp_path, manifest_path, sample_download_results, sample_summary
):
    """write_download_log should write the expected payload structure."""
    log_path = write_download_log(
        download_results=sample_download_results,
        summary=sample_summary,
        manifest_path=manifest_path,
        target_identifier="s3://test-bucket/raw",
        output_dir=tmp_path,
    )

    payload = json.loads(log_path.read_text())

    assert payload["manifest_path"] == str(manifest_path)
    assert payload["target_identifier"] == "s3://test-bucket/raw"
    assert payload["summary"] == sample_summary
    assert payload["results"] == sample_download_results
    assert payload["by_status"] == {
        "downloaded": ["yellow/2025-01/yellow.parquet"],
        "skipped": ["fhv/2025-01/fhv.parquet"],
        "failed": ["green/2025-01/green.parquet"],
    }


def test_write_download_log_handles_empty_results(tmp_path, manifest_path):
    """write_download_log should handle empty download results."""
    summary = {
        "downloaded": 0,
        "skipped": 0,
        "failed": 0,
        "total_bytes_written": 0,
    }

    log_path = write_download_log(
        download_results=[],
        summary=summary,
        manifest_path=manifest_path,
        target_identifier="s3://test-bucket/raw",
        output_dir=tmp_path,
    )

    payload = json.loads(log_path.read_text())

    assert payload["by_status"] == {
        "downloaded": [],
        "skipped": [],
        "failed": [],
    }


# ---------------------------
# summarize_download_results tests
# ---------------------------


@pytest.mark.parametrize(
    ("download_results", "expected_summary"),
    [
        (
            [
                {"status": "downloaded", "bytes_written": 100},
                {"status": "downloaded", "bytes_written": 200},
            ],
            {
                "downloaded": 2,
                "skipped": 0,
                "failed": 0,
                "total_bytes_written": 300,
            },
        ),
        (
            [
                {"status": "downloaded", "bytes_written": 100},
                {"status": "failed", "bytes_written": 0},
            ],
            {
                "downloaded": 1,
                "skipped": 0,
                "failed": 1,
                "total_bytes_written": 100,
            },
        ),
        (
            [
                {"status": "downloaded", "bytes_written": 100},
                {"status": "skipped", "bytes_written": 0},
            ],
            {
                "downloaded": 1,
                "skipped": 1,
                "failed": 0,
                "total_bytes_written": 100,
            },
        ),
        (
            [
                {"status": "downloaded", "bytes_written": 100},
                {"status": "failed", "bytes_written": 0},
                {"status": "skipped", "bytes_written": 0},
            ],
            {
                "downloaded": 1,
                "skipped": 1,
                "failed": 1,
                "total_bytes_written": 100,
            },
        ),
        (
            [],
            {
                "downloaded": 0,
                "skipped": 0,
                "failed": 0,
                "total_bytes_written": 0,
            },
        ),
    ],
    ids=[
        "all_downloaded",
        "with_failed",
        "with_skipped",
        "mixed_statuses",
        "empty_input",
    ],
)
def test_summarize_download_results(download_results, expected_summary):
    """summarize_download_results should count statuses and sum bytes."""
    assert summarize_download_results(download_results) == expected_summary


# ---------------------------
# group_results_by_status tests
# ---------------------------


@pytest.mark.parametrize(
    ("download_results", "expected"),
    [
        (
            [
                {
                    "status": "downloaded",
                    "vehicle_type": "yellow",
                    "year_month": "2025-01",
                    "source_file_name": "yellow.parquet",
                },
                {
                    "status": "failed",
                    "vehicle_type": "green",
                    "year_month": "2025-01",
                    "source_file_name": "green.parquet",
                },
                {
                    "status": "skipped",
                    "vehicle_type": "fhv",
                    "year_month": "2025-01",
                    "source_file_name": "fhv.parquet",
                },
            ],
            {
                "downloaded": ["yellow/2025-01/yellow.parquet"],
                "skipped": ["fhv/2025-01/fhv.parquet"],
                "failed": ["green/2025-01/green.parquet"],
            },
        ),
        (
            [],
            {
                "downloaded": [],
                "skipped": [],
                "failed": [],
            },
        ),
        (
            [
                {
                    "status": "downloaded",
                    "vehicle_type": "yellow",
                    "year_month": "2025-01",
                    "source_file_name": "yellow.parquet",
                },
                {
                    "status": "unknown",
                    "vehicle_type": "green",
                    "year_month": "2025-01",
                    "source_file_name": "green.parquet",
                },
            ],
            {
                "downloaded": ["yellow/2025-01/yellow.parquet"],
                "skipped": [],
                "failed": [],
            },
        ),
    ],
    ids=["mixed_results", "empty_input", "ignore_unknown_status"],
)
def test_group_results_by_status(download_results, expected):
    """group_results_by_status should group known statuses only."""
    assert group_results_by_status(download_results) == expected
