import pytest

from nyc_taxi_data_service.bootstrap.downloader import (
    download_manifest_entries,
    run,
)

# ---------------------------
# Fixtures
# ---------------------------


@pytest.fixture
def manifest_entries():
    """Provide representative manifest entries."""
    return [
        {
            "source_url": "url1",
            "target_relative_path": "yellow/file1.parquet",
            "vehicle_type": "yellow",
            "year_month": "2025-01",
            "source_file_name": "file1.parquet",
        },
        {
            "source_url": "url2",
            "target_relative_path": "green/file2.parquet",
            "vehicle_type": "green",
            "year_month": "2025-01",
            "source_file_name": "file2.parquet",
        },
    ]


@pytest.fixture
def storage_writer():
    """Provide a simple storage writer sentinel."""
    return object()


# ---------------------------
# download_manifest_entries tests
# ---------------------------


@pytest.mark.parametrize(
    "skip_existing",
    [True, False],
    ids=["skip_existing_true", "skip_existing_false"],
)
def test_download_manifest_entries_calls_download_and_enriches(
    monkeypatch,
    manifest_entries,
    storage_writer,
    skip_existing,
):
    """download_manifest_entries should call download_file and enrich results."""
    calls = []

    def mock_download_file(**kwargs):
        calls.append(kwargs)
        return {
            "status": "downloaded",
            "bytes_written": 100,
        }

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.download_file",
        mock_download_file,
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.build_request_session",
        lambda: object(),
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.tqdm",
        lambda x, total: x,
    )

    results = download_manifest_entries(
        manifest_entries=manifest_entries,
        storage_writer=storage_writer,
        skip_existing=skip_existing,
    )

    assert len(results) == len(manifest_entries)
    assert len(calls) == len(manifest_entries)

    for result, entry, call in zip(results, manifest_entries, calls):
        assert result["status"] == "downloaded"
        assert result["vehicle_type"] == entry["vehicle_type"]
        assert result["year_month"] == entry["year_month"]
        assert result["source_file_name"] == entry["source_file_name"]

        assert call["source_url"] == entry["source_url"]
        assert call["target_relative_path"] == entry["target_relative_path"]
        assert call["storage_writer"] is storage_writer
        assert call["skip_existing"] is skip_existing


def test_download_manifest_entries_returns_empty_list_for_empty_input(storage_writer):
    """download_manifest_entries should return an empty list for empty input."""
    results = download_manifest_entries(
        manifest_entries=[],
        storage_writer=storage_writer,
    )

    assert results == []


# ---------------------------
# run tests
# ---------------------------


def test_run_orchestrates_downloader_flow(monkeypatch, tmp_path):
    """run should orchestrate manifest loading, download, summary, and log writing."""
    manifest_path = tmp_path / "manifest.json"
    manifest = {
        "generated_at_utc": "2026-04-21T00:00:00Z",
        "active_environment": "dev",
        "time_frame": {"start": "2025-01", "end": "2025-01"},
        "vehicle_types": ["yellow", "green"],
        "storage": {
            "target_storage_type": "local",
            "target_base_path": "data/raw",
        },
        "entries": [
            {
                "source_url": "url1",
                "target_relative_path": "yellow/file1.parquet",
                "vehicle_type": "yellow",
                "year_month": "2025-02",
                "source_file_name": "file1.parquet",
            },
            {
                "source_url": "url2",
                "target_relative_path": "green/file2.parquet",
                "vehicle_type": "green",
                "year_month": "2025-01",
                "source_file_name": "file2.parquet",
            },
        ],
    }

    storage_writer = object()
    unsorted_results = [
        {
            "status": "downloaded",
            "bytes_written": 200,
            "vehicle_type": "yellow",
            "year_month": "2025-02",
            "source_file_name": "file1.parquet",
        },
        {
            "status": "downloaded",
            "bytes_written": 100,
            "vehicle_type": "green",
            "year_month": "2025-01",
            "source_file_name": "file2.parquet",
        },
    ]
    summary = {
        "downloaded": 2,
        "skipped": 0,
        "failed": 0,
        "total_bytes_written": 300,
    }
    log_path = tmp_path / "download_log.json"

    def mock_load_manifest(path):
        return manifest

    def mock_build_storage_writer(config):
        return storage_writer

    def mock_get_storage_root(config):
        return "data/raw"

    captured_download_manifest_entries = {}

    def mock_download_manifest_entries(manifest_entries, storage_writer):
        captured_download_manifest_entries["manifest_entries"] = manifest_entries
        captured_download_manifest_entries["storage_writer"] = storage_writer
        return list(unsorted_results)

    captured_summary_input = {}

    def mock_summarize_download_results(download_results):
        captured_summary_input["download_results"] = list(download_results)
        return summary

    captured_write_download_log = {}

    def mock_write_download_log(
        download_results, summary, manifest_path, target_identifier
    ):
        captured_write_download_log["download_results"] = list(download_results)
        captured_write_download_log["summary"] = summary
        captured_write_download_log["manifest_path"] = manifest_path
        captured_write_download_log["target_identifier"] = target_identifier
        return log_path

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.load_manifest",
        mock_load_manifest,
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.build_storage_writer",
        mock_build_storage_writer,
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.download_manifest_entries",
        mock_download_manifest_entries,
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.summarize_download_results",
        mock_summarize_download_results,
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.get_storage_root",
        mock_get_storage_root,
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.write_download_log",
        mock_write_download_log,
    )

    result = run(manifest_path)

    expected_sorted_results = [
        {
            "status": "downloaded",
            "bytes_written": 100,
            "vehicle_type": "green",
            "year_month": "2025-01",
            "source_file_name": "file2.parquet",
        },
        {
            "status": "downloaded",
            "bytes_written": 200,
            "vehicle_type": "yellow",
            "year_month": "2025-02",
            "source_file_name": "file1.parquet",
        },
    ]

    assert result == log_path
    assert captured_download_manifest_entries == {
        "manifest_entries": manifest["entries"],
        "storage_writer": storage_writer,
    }
    assert captured_summary_input["download_results"] == expected_sorted_results
    assert captured_write_download_log == {
        "download_results": expected_sorted_results,
        "summary": summary,
        "manifest_path": manifest_path,
        "target_identifier": "data/raw",
    }


def test_run_handles_empty_manifest_entries(monkeypatch, tmp_path):
    """run should handle an empty manifest entry list."""
    manifest_path = tmp_path / "manifest.json"
    manifest = {
        "generated_at_utc": "2026-04-21T00:00:00Z",
        "active_environment": "dev",
        "time_frame": {"start": "2025-01", "end": "2025-01"},
        "vehicle_types": [],
        "storage": {
            "target_storage_type": "local",
            "target_base_path": "data/raw",
        },
        "entries": [],
    }

    log_path = tmp_path / "download_log.json"

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.load_manifest",
        lambda path: manifest,
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.build_storage_writer",
        lambda config: object(),
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.download_manifest_entries",
        lambda manifest_entries, storage_writer: [],
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.summarize_download_results",
        lambda download_results: {
            "downloaded": 0,
            "skipped": 0,
            "failed": 0,
            "total_bytes_written": 0,
        },
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.get_storage_root",
        lambda config: "data/raw",
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.write_download_log",
        lambda download_results, summary, manifest_path, target_identifier: log_path,
    )

    result = run(manifest_path)

    assert result == log_path
