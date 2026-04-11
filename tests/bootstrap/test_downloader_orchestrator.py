from nyc_taxi_data_service.bootstrap.downloader import download_manifest_entries


def test_download_manifest_entries_calls_download_and_enriches(monkeypatch, tmp_path):
    """
    Verify that download_manifest_entries calls download_file for each entry
    and enriches the result with metadata fields.
    """

    # --- arrange ---
    manifest_entries = [
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

    # --- act ---
    results = download_manifest_entries(
        manifest_entries=manifest_entries,
        target_base_path=tmp_path,
    )

    # --- assert ---
    assert len(results) == 2

    for result, entry in zip(results, manifest_entries):
        assert result["status"] == "downloaded"
        assert result["vehicle_type"] == entry["vehicle_type"]
        assert result["year_month"] == entry["year_month"]
        assert result["source_file_name"] == entry["source_file_name"]

    assert len(calls) == 2


def test_download_manifest_entries_resolves_target_path(monkeypatch, tmp_path):
    """
    Verify that target paths passed to download_file are correctly resolved.
    """

    captured_paths = []

    def mock_download_file(session, source_url, target_path, **kwargs):
        captured_paths.append(target_path)
        return {"status": "downloaded", "bytes_written": 1}

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

    entries = [
        {
            "source_url": "url",
            "target_relative_path": "yellow/2025/file.parquet",
            "vehicle_type": "yellow",
            "year_month": "2025-01",
            "source_file_name": "file.parquet",
        }
    ]

    download_manifest_entries(entries, target_base_path=tmp_path)

    assert captured_paths[0] == tmp_path / "yellow/2025/file.parquet"


def test_download_manifest_entries_returns_empty_list_for_empty_input(tmp_path):
    """
    Verify that empty manifest_entries returns an empty result list.
    """
    results = download_manifest_entries(
        manifest_entries=[],
        target_base_path=tmp_path,
    )

    assert results == []


def test_download_manifest_entries_passes_skip_existing(monkeypatch, tmp_path):
    """
    Verify skip_existing flag is passed to download_file.
    """

    captured_flags = []

    def mock_download_file(session, source_url, target_path, skip_existing, **kwargs):
        captured_flags.append(skip_existing)
        return {"status": "downloaded", "bytes_written": 1}

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

    entries = [
        {
            "source_url": "url",
            "target_relative_path": "file.parquet",
            "vehicle_type": "yellow",
            "year_month": "2025-01",
            "source_file_name": "file.parquet",
        }
    ]

    download_manifest_entries(entries, tmp_path, skip_existing=False)

    assert captured_flags == [False]
