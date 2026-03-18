from pathlib import Path

from nyc_taxi_data_service.bootstrap.downloader import download_manifest_entries


def test_download_manifest_entries_calls_dependencies_and_enriches_results(
    monkeypatch, tmp_path
):
    """
    Verify that download_manifest_entries resolves each target path, calls
    download_file for each manifest entry, and enriches each returned result
    with manifest-specific fields.

    This test checks:
    - resolve_target_path is called once per entry
    - download_file is called once per entry
    - expected_size_bytes is taken from remote_metadata.content_length
    - vehicle_type, year_month, and source_file_name are added to each result
    - output order matches input order
    """
    manifest_entries = [
        {
            "source_url": "https://example.com/yellow_2020-01.parquet",
            "target_relative_path": "yellow/2020/01.parquet",
            "vehicle_type": "yellow",
            "year_month": "2020-01",
            "source_file_name": "yellow_2020-01.parquet",
            "remote_metadata": {"content_length": 100},
        },
        {
            "source_url": "https://example.com/green_2020-01.parquet",
            "target_relative_path": "green/2020/01.parquet",
            "vehicle_type": "green",
            "year_month": "2020-01",
            "source_file_name": "green_2020-01.parquet",
            "remote_metadata": {"content_length": 200},
        },
    ]

    resolve_calls = []
    download_calls = []

    def mock_resolve_target_path(target_base_path, target_relative_path):
        resolve_calls.append((target_base_path, target_relative_path))
        return Path(target_base_path) / target_relative_path

    def mock_download_file(source_url, target_path, skip_existing, expected_size_bytes):
        download_calls.append(
            {
                "source_url": source_url,
                "target_path": target_path,
                "skip_existing": skip_existing,
                "expected_size_bytes": expected_size_bytes,
            }
        )
        return {
            "status": "downloaded",
            "target_path": str(target_path),
            "bytes_written": expected_size_bytes,
            "error": None,
        }

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.resolve_target_path",
        mock_resolve_target_path,
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.download_file", mock_download_file
    )

    result = download_manifest_entries(
        manifest_entries=manifest_entries,
        target_base_path=tmp_path,
        skip_existing=True,
    )

    assert resolve_calls == [
        (tmp_path, "yellow/2020/01.parquet"),
        (tmp_path, "green/2020/01.parquet"),
    ]

    assert download_calls == [
        {
            "source_url": "https://example.com/yellow_2020-01.parquet",
            "target_path": tmp_path / "yellow/2020/01.parquet",
            "skip_existing": True,
            "expected_size_bytes": 100,
        },
        {
            "source_url": "https://example.com/green_2020-01.parquet",
            "target_path": tmp_path / "green/2020/01.parquet",
            "skip_existing": True,
            "expected_size_bytes": 200,
        },
    ]

    assert result == [
        {
            "status": "downloaded",
            "target_path": str(tmp_path / "yellow/2020/01.parquet"),
            "bytes_written": 100,
            "error": None,
            "vehicle_type": "yellow",
            "year_month": "2020-01",
            "source_file_name": "yellow_2020-01.parquet",
        },
        {
            "status": "downloaded",
            "target_path": str(tmp_path / "green/2020/01.parquet"),
            "bytes_written": 200,
            "error": None,
            "vehicle_type": "green",
            "year_month": "2020-01",
            "source_file_name": "green_2020-01.parquet",
        },
    ]


def test_download_manifest_entries_passes_skip_existing_false(monkeypatch, tmp_path):
    """
    Verify that download_manifest_entries passes the caller-provided
    skip_existing value through to download_file.

    This test checks the non-default branch where skip_existing=False.
    """
    manifest_entries = [
        {
            "source_url": "https://example.com/fhv_2020-01.parquet",
            "target_relative_path": "fhv/2020/01.parquet",
            "vehicle_type": "fhv",
            "year_month": "2020-01",
            "source_file_name": "fhv_2020-01.parquet",
            "remote_metadata": {"content_length": 300},
        }
    ]

    captured = {}

    def mock_resolve_target_path(target_base_path, target_relative_path):
        return Path(target_base_path) / target_relative_path

    def mock_download_file(source_url, target_path, skip_existing, expected_size_bytes):
        captured["skip_existing"] = skip_existing
        return {
            "status": "downloaded",
            "target_path": str(target_path),
            "bytes_written": expected_size_bytes,
            "error": None,
        }

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.resolve_target_path",
        mock_resolve_target_path,
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.download_file", mock_download_file
    )

    result = download_manifest_entries(
        manifest_entries=manifest_entries,
        target_base_path=tmp_path,
        skip_existing=False,
    )

    assert captured["skip_existing"] is False
    assert result[0]["vehicle_type"] == "fhv"
    assert result[0]["year_month"] == "2020-01"
    assert result[0]["source_file_name"] == "fhv_2020-01.parquet"


def test_download_manifest_entries_uses_none_when_remote_metadata_missing(
    monkeypatch, tmp_path
):
    """
    Verify that download_manifest_entries passes expected_size_bytes=None
    when a manifest entry does not contain remote_metadata.content_length.

    This test checks the fallback behavior of:
    entry.get("remote_metadata", {}).get("content_length")
    """
    manifest_entries = [
        {
            "source_url": "https://example.com/fhvhv_2020-01.parquet",
            "target_relative_path": "fhvhv/2020/01.parquet",
            "vehicle_type": "fhvhv",
            "year_month": "2020-01",
            "source_file_name": "fhvhv_2020-01.parquet",
        }
    ]

    captured = {}

    def mock_resolve_target_path(target_base_path, target_relative_path):
        return Path(target_base_path) / target_relative_path

    def mock_download_file(source_url, target_path, skip_existing, expected_size_bytes):
        captured["expected_size_bytes"] = expected_size_bytes
        return {
            "status": "downloaded",
            "target_path": str(target_path),
            "bytes_written": 123,
            "error": None,
        }

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.resolve_target_path",
        mock_resolve_target_path,
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.download_file", mock_download_file
    )

    result = download_manifest_entries(
        manifest_entries=manifest_entries,
        target_base_path=tmp_path,
    )

    assert captured["expected_size_bytes"] is None
    assert result == [
        {
            "status": "downloaded",
            "target_path": str(tmp_path / "fhvhv/2020/01.parquet"),
            "bytes_written": 123,
            "error": None,
            "vehicle_type": "fhvhv",
            "year_month": "2020-01",
            "source_file_name": "fhvhv_2020-01.parquet",
        }
    ]


def test_download_manifest_entries_returns_empty_list_for_empty_input(
    monkeypatch, tmp_path
):
    """
    Verify that download_manifest_entries returns an empty list when given
    no manifest entries.

    This test also checks that no dependency calls are made for empty input.
    """
    resolve_called = False
    download_called = False

    def mock_resolve_target_path(*args, **kwargs):
        nonlocal resolve_called
        resolve_called = True

    def mock_download_file(*args, **kwargs):
        nonlocal download_called
        download_called = True

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.resolve_target_path",
        mock_resolve_target_path,
    )
    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.downloader.download_file", mock_download_file
    )

    result = download_manifest_entries(
        manifest_entries=[],
        target_base_path=tmp_path,
    )

    assert result == []
    assert resolve_called is False
    assert download_called is False
