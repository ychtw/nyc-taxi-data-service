from pathlib import Path

import pytest

from nyc_taxi_data_service.bootstrap.downloader import (
    attach_remote_metadata_to_entries,
    format_bytes,
    get_total_required_bytes,
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


def test_attach_remote_metadata_to_entries_attaches_matching_metadata():
    """
    Verify that each manifest entry is enriched with the matching remote metadata
    at the same list position.

    This test checks the main expected behavior of the function:
    - each output item keeps the original manifest entry fields
    - each output item gains a "remote_metadata" field
    - metadata is attached by positional order
    """
    manifest_entries = [
        {
            "source_url": "https://example.com/yellow_2020-01.parquet",
            "target_relative_path": "yellow/2020-01.parquet",
            "vehicle_type": "yellow",
        },
        {
            "source_url": "https://example.com/green_2020-01.parquet",
            "target_relative_path": "green/2020-01.parquet",
            "vehicle_type": "green",
        },
    ]
    remote_metadata_list = [
        {
            "source_url": "https://example.com/yellow_2020-01.parquet",
            "content_length": 123,
            "status_code": 200,
            "ok": True,
        },
        {
            "source_url": "https://example.com/green_2020-01.parquet",
            "content_length": 456,
            "status_code": 200,
            "ok": True,
        },
    ]

    result = attach_remote_metadata_to_entries(
        manifest_entries=manifest_entries,
        remote_metadata_list=remote_metadata_list,
    )

    assert result == [
        {
            "source_url": "https://example.com/yellow_2020-01.parquet",
            "target_relative_path": "yellow/2020-01.parquet",
            "vehicle_type": "yellow",
            "remote_metadata": remote_metadata_list[0],
        },
        {
            "source_url": "https://example.com/green_2020-01.parquet",
            "target_relative_path": "green/2020-01.parquet",
            "vehicle_type": "green",
            "remote_metadata": remote_metadata_list[1],
        },
    ]


def test_attach_remote_metadata_to_entries_returns_new_entry_dicts():
    """
    Verify that the function returns new dictionaries instead of mutating
    the original manifest entry objects in place.

    This matters because the implementation copies each entry with dict(entry)
    before adding "remote_metadata".
    """
    manifest_entries = [
        {
            "source_url": "https://example.com/yellow_2020-01.parquet",
            "target_relative_path": "yellow/2020-01.parquet",
        }
    ]
    remote_metadata_list = [
        {
            "source_url": "https://example.com/yellow_2020-01.parquet",
            "content_length": 123,
        }
    ]

    result = attach_remote_metadata_to_entries(
        manifest_entries=manifest_entries,
        remote_metadata_list=remote_metadata_list,
    )

    assert result is not manifest_entries
    assert result[0] is not manifest_entries[0]
    assert "remote_metadata" not in manifest_entries[0]
    assert result[0]["remote_metadata"] == remote_metadata_list[0]


def test_attach_remote_metadata_to_entries_truncates_to_shorter_input_list():
    """
    Verify that the function only returns pairs that can be formed from both
    input lists.

    Because the implementation uses zip(..., strict=False), iteration stops at
    the shorter list length rather than raising an error.
    """
    manifest_entries = [
        {
            "source_url": "https://example.com/yellow_2020-01.parquet",
            "target_relative_path": "yellow/2020-01.parquet",
        },
        {
            "source_url": "https://example.com/green_2020-01.parquet",
            "target_relative_path": "green/2020-01.parquet",
        },
    ]
    remote_metadata_list = [
        {
            "source_url": "https://example.com/yellow_2020-01.parquet",
            "content_length": 123,
        }
    ]

    result = attach_remote_metadata_to_entries(
        manifest_entries=manifest_entries,
        remote_metadata_list=remote_metadata_list,
    )

    assert len(result) == 1
    assert result[0]["source_url"] == manifest_entries[0]["source_url"]
    assert result[0]["remote_metadata"] == remote_metadata_list[0]


@pytest.mark.parametrize(
    ("remote_metadata_list", "expected_total"),
    [
        # scenario 1: sum all content_length values when every entry is valid
        # (ok=True and content_length is present).
        (
            [
                {"ok": True, "content_length": 100},
                {"ok": True, "content_length": 200},
            ],
            300,
        ),
        # scenario 2: ignore entries whose remote request was not successful
        # (ok=False), even if content_length is present.
        (
            [
                {"ok": True, "content_length": 100},
                {"ok": False, "content_length": 999},
            ],
            100,
        ),
        # scenario 3: ignore entries whose content_length is None,
        # even if the remote request itself succeeded.
        (
            [
                {"ok": True, "content_length": 100},
                {"ok": True, "content_length": None},
            ],
            100,
        ),
        # scenario 4: return 0 when all entries are invalid for summation,
        # such as failed requests or missing content_length.
        (
            [
                {"ok": False, "content_length": 100},
                {"ok": True, "content_length": None},
            ],
            0,
        ),
        # scenario 5: return 0 for an empty input list.
        (
            [],
            0,
        ),
    ],
    ids=[
        "sums_valid_entries",
        "ignores_not_ok_entries",
        "ignores_none_content_length",
        "all_invalid_returns_zero",
        "empty_list_returns_zero",
    ],
)
def test_get_total_required_bytes(remote_metadata_list, expected_total):
    """
    Verify that get_total_required_bytes sums only valid content_length values.

    This parametrized test covers:
    - summing multiple valid entries
    - ignoring entries with ok=False
    - ignoring entries with content_length=None
    - returning 0 when no entries are valid
    - returning 0 for an empty list
    """
    assert get_total_required_bytes(remote_metadata_list) == expected_total


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
