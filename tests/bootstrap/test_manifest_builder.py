import json
from datetime import datetime
from pathlib import Path

import pytest

from nyc_taxi_data_service.bootstrap.manifest_builder import (
    build_manifest_entries,
    generate_year_months,
    write_manifest,
)


@pytest.mark.parametrize(
    "start, end, expected",
    [
        # standard range
        ("2025-01", "2025-03", ["2025-01", "2025-02", "2025-03"]),
        # single month
        ("2025-05", "2025-05", ["2025-05"]),
        # cross year
        ("2024-11", "2025-02", ["2024-11", "2024-12", "2025-01", "2025-02"]),
        # multi-year span
        (
            "2023-12",
            "2025-01",
            [
                "2023-12",
                "2024-01",
                "2024-02",
                "2024-03",
                "2024-04",
                "2024-05",
                "2024-06",
                "2024-07",
                "2024-08",
                "2024-09",
                "2024-10",
                "2024-11",
                "2024-12",
                "2025-01",
            ],
        ),
    ],
    ids=["standard_range", "single_month", "cross_year", "multi_year_span"],
)
def test_generate_year_months_scenarios(
    start: str, end: str, expected: list[str]
) -> None:
    """Test various valid year-month range generations."""
    assert generate_year_months(start, end) == expected


def test_generate_year_months_raises_value_error_when_start_is_later_than_end() -> None:
    """Ensure ValueError is raised when start is after end."""
    with pytest.raises(
        ValueError,
        match="start_year_month 2025-03 cannot be later than end_year_month 2025-01",
    ):
        generate_year_months("2025-03", "2025-01")


def test_generate_year_months_raises_value_error_for_invalid_format() -> None:
    """Ensure standard datetime parsing errors are raised for bad formats."""
    with pytest.raises(ValueError):
        generate_year_months("2025/01", "2025/03")


def test_build_manifest_entries_logic():
    """
    Test manifest building logic including:
    - Correct number of entries (filtering logic).
    - URL construction.
    - Target path partitioning structure.
    """
    # create mock configs
    mock_sources_config = {
        "tlc_website": {
            "base_url": "https://test-data.com",
            "file_extension": "parquet",
        },
        "vehicle_types": {
            "yellow": {"available_from": "2010-01", "name_pattern": "yellow_tripdata"},
            "green": {"available_from": "2020-01", "name_pattern": "green_tripdata"},
        },
    }
    mock_resolved_runtime_config = {
        "time_frame": {"start_year_month": "2019-12", "end_year_month": "2020-02"},
        "vehicle_types": ["yellow", "green"],
        "storage": {"target_storage_type": "local", "target_base_path": "data/raw"},
    }

    # run the function with mock configs
    entries = build_manifest_entries(mock_sources_config, mock_resolved_runtime_config)

    # calculation based on mock_sources_config & mock_sources_config fixtures:
    # - yellow is available from 2010: 2019-12 to 2020-02 = 3 months.
    # - green is available from 2020: 2019-12 to 2020-02 = 2 months (skips 2019-12).
    assert len(entries) == 5

    # check filtering for 'green'
    green_entries = [e for e in entries if e["vehicle_type"] == "green"]
    assert len(green_entries) == 2
    assert "2019-12" not in [e["year_month"] for e in green_entries]

    # verify a specific entry's structure (yellow 2020-01)
    # including url construction & target path structure
    yellow_sample = next(
        e
        for e in entries
        if e["vehicle_type"] == "yellow" and e["year_month"] == "2020-01"
    )
    assert yellow_sample["source_file_name"] == "yellow_tripdata_2020-01.parquet"
    assert (
        yellow_sample["source_url"]
        == "https://test-data.com/yellow_tripdata_2020-01.parquet"
    )
    assert (
        yellow_sample["target_relative_path"]
        == "yellow/year=2020/month=01/yellow_tripdata_2020-01.parquet"
    )


@pytest.mark.parametrize(
    "sources_config, resolved_runtime_config, expected_summary",
    [
        # scenario 1: single vehicle, single month
        (
            # sources_config
            {
                "tlc_website": {
                    "base_url": "https://example.com/trip-data",
                    "file_extension": "parquet",
                },
                "vehicle_types": {
                    "yellow": {
                        "available_from": "2009-01",
                        "name_pattern": "yellow_tripdata",
                    }
                },
            },
            # resolved_runtime_config
            {
                "time_frame": {
                    "start_year_month": "2025-01",
                    "end_year_month": "2025-01",
                },
                "vehicle_types": ["yellow"],
            },
            # expected summary result
            {
                "len": 1,
                "vehicle_types": ["yellow"],
                "year_months": ["2025-01"],
                "source_file_names": ["yellow_tripdata_2025-01.parquet"],
            },
        ),
        # scenario 2: single vehicle, multiple months
        (
            # sources_config
            {
                "tlc_website": {
                    "base_url": "https://example.com/trip-data",
                    "file_extension": "parquet",
                },
                "vehicle_types": {
                    "yellow": {
                        "available_from": "2009-01",
                        "name_pattern": "yellow_tripdata",
                    }
                },
            },
            # resolved_runtime_config
            {
                "time_frame": {
                    "start_year_month": "2025-01",
                    "end_year_month": "2025-03",
                },
                "vehicle_types": ["yellow"],
            },
            # expected summary result
            {
                "len": 3,
                "vehicle_types": ["yellow", "yellow", "yellow"],
                "year_months": ["2025-01", "2025-02", "2025-03"],
                "source_file_names": [
                    "yellow_tripdata_2025-01.parquet",
                    "yellow_tripdata_2025-02.parquet",
                    "yellow_tripdata_2025-03.parquet",
                ],
            },
        ),
        # scenario 3: multiple vehicle types
        (
            # sources_config
            {
                "tlc_website": {
                    "base_url": "https://example.com/trip-data",
                    "file_extension": "parquet",
                },
                "vehicle_types": {
                    "yellow": {
                        "available_from": "2009-01",
                        "name_pattern": "yellow_tripdata",
                    },
                    "green": {
                        "available_from": "2013-08",
                        "name_pattern": "green_tripdata",
                    },
                },
            },
            # resolved_runtime_config
            {
                "time_frame": {
                    "start_year_month": "2025-01",
                    "end_year_month": "2025-02",
                },
                "vehicle_types": ["yellow", "green"],
            },
            # expected summary result
            {
                "len": 4,
                "vehicle_types": ["yellow", "yellow", "green", "green"],
                "year_months": ["2025-01", "2025-02", "2025-01", "2025-02"],
                "source_file_names": [
                    "yellow_tripdata_2025-01.parquet",
                    "yellow_tripdata_2025-02.parquet",
                    "green_tripdata_2025-01.parquet",
                    "green_tripdata_2025-02.parquet",
                ],
            },
        ),
        # scenario 4: skip months before available_from
        (
            # sources_config
            {
                "tlc_website": {
                    "base_url": "https://example.com/trip-data",
                    "file_extension": "parquet",
                },
                "vehicle_types": {
                    "green": {
                        "available_from": "2013-08",
                        "name_pattern": "green_tripdata",
                    }
                },
            },
            # resolved_runtime_config
            {
                "time_frame": {
                    "start_year_month": "2013-06",
                    "end_year_month": "2013-09",
                },
                "vehicle_types": ["green"],
            },
            # expected summary result
            {
                "len": 2,
                "vehicle_types": ["green", "green"],
                "year_months": ["2013-08", "2013-09"],
                "source_file_names": [
                    "green_tripdata_2013-08.parquet",
                    "green_tripdata_2013-09.parquet",
                ],
            },
        ),
        # scenario 5: when all months are unavailable
        (
            # sources_config
            {
                "tlc_website": {
                    "base_url": "https://example.com/trip-data",
                    "file_extension": "parquet",
                },
                "vehicle_types": {
                    "fhvhv": {
                        "available_from": "2019-02",
                        "name_pattern": "fhvhv_tripdata",
                    }
                },
            },
            # resolved_runtime_config
            {
                "time_frame": {
                    "start_year_month": "2018-11",
                    "end_year_month": "2019-01",
                },
                "vehicle_types": ["fhvhv"],
            },
            # expected summary result
            {
                "len": 0,
                "vehicle_types": [],
                "year_months": [],
                "source_file_names": [],
            },
        ),
    ],
    ids=[
        "single_vehicle_single_month",
        "single_vehicle_multiple_months",
        "multiple_vehicle_types",
        "skip_months_before_available_from",
        "all_months_unavailable",
    ],
)
def test_build_manifest_entries_scenarios(
    sources_config: dict,
    resolved_runtime_config: dict,
    expected_summary: dict,
) -> None:
    entries = build_manifest_entries(sources_config, resolved_runtime_config)

    assert len(entries) == expected_summary["len"]
    assert [entry["vehicle_type"] for entry in entries] == expected_summary[
        "vehicle_types"
    ]
    assert [entry["year_month"] for entry in entries] == expected_summary["year_months"]
    assert [entry["source_file_name"] for entry in entries] == expected_summary[
        "source_file_names"
    ]


def test_write_manifest_writes_expected_json(tmp_path: Path) -> None:
    """Write a manifest JSON file with the expected metadata and entries."""
    # define mock inputs
    manifest_entries = [
        {
            "vehicle_type": "yellow",
            "year_month": "2025-01",
            "source_file_name": "yellow_tripdata_2025-01.parquet",
            "source_url": "https://example.com/trip-data/yellow_tripdata_2025-01.parquet",
            "target_relative_path": "yellow/year=2025/month=01/yellow_tripdata_2025-01.parquet",
        }
    ]
    resolved_runtime_config = {
        "active_environment": "dev",
        "time_frame": {
            "start_year_month": "2025-01",
            "end_year_month": "2025-03",
        },
        "vehicle_types": ["yellow"],
        "storage": {
            "target_storage_type": "local",
            "target_base_path": "data/raw",
        },
    }

    # write manifest into a nested directory to also confirm the returned path is correct
    output_dir = tmp_path / "subdir" / "manifests"
    manifest_path = write_manifest(
        manifest_entries=manifest_entries,
        resolved_runtime_config=resolved_runtime_config,
        output_path=output_dir,
    )

    # assert file path and existence
    assert manifest_path == output_dir / "manifest.json"
    assert manifest_path.exists()

    # load written JSON payload
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    # verify top-level manifest fields
    assert "generated_at_utc" in payload
    assert payload["active_environment"] == "dev"
    assert payload["time_frame"] == resolved_runtime_config["time_frame"]
    assert payload["vehicle_types"] == resolved_runtime_config["vehicle_types"]
    assert payload["storage"] == resolved_runtime_config["storage"]
    assert payload["entries"] == manifest_entries

    # verify generated timestamp is valid iso datetime text
    datetime.fromisoformat(payload["generated_at_utc"])


def test_write_manifest_creates_output_directory_if_missing(tmp_path: Path) -> None:
    """Create the output directory automatically when it does not already exist."""
    manifest_entries = []
    resolved_runtime_config = {
        "active_environment": "dev",
        "time_frame": {
            "start_year_month": "2025-01",
            "end_year_month": "2025-01",
        },
        "vehicle_types": ["yellow"],
        "storage": {
            "target_storage_type": "local",
            "target_base_path": "data/raw",
        },
    }

    # choose a nested path that does not exist yet
    output_dir = tmp_path / "nested" / "manifest_output"
    assert not output_dir.exists()

    manifest_path = write_manifest(
        manifest_entries=manifest_entries,
        resolved_runtime_config=resolved_runtime_config,
        output_path=output_dir,
    )

    # the function should create the directory and write manifest.json inside it
    assert output_dir.exists()
    assert output_dir.is_dir()
    assert manifest_path == output_dir / "manifest.json"
    assert manifest_path.exists()


def test_write_manifest_with_empty_entries_writes_valid_manifest(
    tmp_path: Path,
) -> None:
    """Write a valid manifest JSON file even when there are no entries."""
    manifest_entries = []
    resolved_runtime_config = {
        "active_environment": "dev",
        "time_frame": {
            "start_year_month": "2025-01",
            "end_year_month": "2025-01",
        },
        "vehicle_types": ["yellow"],
        "storage": {
            "target_storage_type": "local",
            "target_base_path": "data/raw",
        },
    }

    manifest_path = write_manifest(
        manifest_entries=manifest_entries,
        resolved_runtime_config=resolved_runtime_config,
        output_path=tmp_path,
    )

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    # entries should be preserved as an empty list rather than omitted or set to null
    assert payload["entries"] == []
