from pathlib import Path

import pytest

from nyc_taxi_data_service.bootstrap.downloader import (
    format_bytes,
    resolve_target_path,
    summarize_download_results,
)


@pytest.mark.parametrize(
    ("num_bytes", "expected"),
    [
        (None, "unknown"),
        (0, "0.00 B"),
        (1, "1.00 B"),
        (512, "512.00 B"),
        (1023, "1023.00 B"),
        (1024, "1.00 KB"),
        (1536, "1.50 KB"),
        (1024**2, "1.00 MB"),
        (5 * 1024**2, "5.00 MB"),
        (1024**3, "1.00 GB"),
        (1024**4, "1.00 TB"),
    ],
)
def test_format_bytes(num_bytes, expected):
    """
    Verify that format_bytes converts byte counts into human-readable strings.

    This test checks:
    - None returns "unknown"
    - Values below 1024 stay in bytes
    - 1024-based unit conversion (KB, MB, GB)
    - Formatting keeps two decimal places
    """
    assert format_bytes(num_bytes) == expected


def test_resolve_target_path_with_string_base_dir(tmp_path):
    """
    Verify that resolve_target_path joins a string base_dir
    with the target_relative_path and returns a Path object.
    """
    base_dir = str(tmp_path)
    relative = "file.parquet"

    result = resolve_target_path(base_dir, relative)

    assert isinstance(result, Path)
    assert result == tmp_path / relative


def test_resolve_target_path_with_path_base_dir(tmp_path):
    """
    Verify that resolve_target_path works when base_dir is already a Path.
    """
    base_dir = tmp_path
    relative = "file.parquet"

    result = resolve_target_path(base_dir, relative)

    assert result == tmp_path / relative


def test_resolve_target_path_nested_relative_path(tmp_path):
    """
    Verify that nested relative paths are appended correctly
    without modification.
    """
    base_dir = tmp_path
    relative = "yellow/2020/01/file.parquet"

    result = resolve_target_path(base_dir, relative)

    assert result == tmp_path / "yellow/2020/01/file.parquet"


def test_resolve_target_path_returns_path_object(tmp_path):
    """
    Verify that the function always returns a Path instance,
    regardless of input type.
    """
    result = resolve_target_path(str(tmp_path), "file.parquet")

    assert isinstance(result, Path)


@pytest.mark.parametrize(
    ("download_results", "expected_summary"),
    [
        # scenario 1: all results are successful downloads, so "downloaded"
        # should equal the number of items, while "skipped" and "failed" stay at 0.
        # total_bytes_written should be the sum of all bytes_written values.
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
        # scenario 2: include a failed download and verify that:
        # - "failed" is counted correctly
        # - "downloaded" only counts successful downloads
        # - total_bytes_written still sums all bytes_written values present
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
        # scenario 3: include a skipped file and verify that:
        # - "skipped" is counted separately
        # - it does not increase "downloaded" or "failed"
        # - bytes_written is still aggregated normally
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
        # scenario 4: mix downloaded, failed, and skipped results
        # together to verify the function correctly counts each status
        # and sums bytes_written.
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
        # scenario 5: an empty input list should return a summary
        # with all counters at 0.
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
    """
    Verify that summarize_download_results counts each download status and
    sums bytes_written correctly across common result scenarios.

    This parametrized test covers:
    - all downloaded results
    - results containing failures
    - results containing skipped files
    - mixed status combinations
    - empty input
    """
    assert summarize_download_results(download_results) == expected_summary


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
    ],
    ids=["mixed_results", "empty_input"],
)
def test_group_results_by_status(download_results, expected):
    """
    Verify that group_results_by_status groups results into file identifiers by status.
    """
    from nyc_taxi_data_service.bootstrap.downloader import group_results_by_status

    assert group_results_by_status(download_results) == expected


def test_group_results_by_status_ignores_unknown_status():
    """
    Verify that unknown statuses are ignored.
    """
    from nyc_taxi_data_service.bootstrap.downloader import group_results_by_status

    results = [
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
    ]

    grouped = group_results_by_status(results)

    assert grouped == {
        "downloaded": ["yellow/2025-01/yellow.parquet"],
        "skipped": [],
        "failed": [],
    }
