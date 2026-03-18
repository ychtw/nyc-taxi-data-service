import json
import re
from pathlib import Path

import pytest

from nyc_taxi_data_service.bootstrap.downloader import load_manifest, write_download_log


def test_load_manifest_returns_parsed_manifest_dict(tmp_path):
    """
    Verify that load_manifest reads a JSON manifest file and returns
    its parsed content as a dictionary.

    This test checks the normal success path:
    - a manifest file is created under a temporary directory
    - the file contains valid JSON
    - load_manifest returns the same dictionary content
    """
    manifest_data = {
        "generated_at_utc": "2026-03-17T00:00:00Z",
        "storage": {
            "target_storage_type": "local",
            "target_base_path": "data/raw",
        },
        "entries": [
            {
                "source_url": "https://example.com/yellow_2020-01.parquet",
                "target_relative_path": "yellow/2020-01.parquet",
            }
        ],
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_data), encoding="utf-8")

    result = load_manifest(manifest_path)

    assert result == manifest_data


def test_load_manifest_raises_json_decode_error_for_invalid_json(tmp_path):
    """
    Verify that load_manifest raises JSONDecodeError when the manifest file
    contains invalid JSON content.
    """
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{invalid json}", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        load_manifest(manifest_path)


def test_load_manifest_raises_file_not_found_error_for_missing_file(tmp_path):
    """
    Verify that load_manifest raises FileNotFoundError when the manifest
    path does not exist.
    """
    missing_path = tmp_path / "missing_manifest.json"

    with pytest.raises(FileNotFoundError):
        load_manifest(missing_path)


def test_write_download_log_creates_log_file_and_returns_path(tmp_path):
    """
    Verify that write_download_log creates a log file in the output directory
    and returns the path to the created file.

    This test checks:
    - output directory is created automatically
    - returned path exists
    - filename follows expected pattern
    """
    download_results = [{"status": "downloaded", "bytes_written": 100}]
    summary = {"downloaded": 1, "skipped": 0, "failed": 0, "total_bytes_written": 100}

    manifest_path = tmp_path / "manifest.json"
    target_base_path = tmp_path / "data"
    output_dir = tmp_path / "logs"

    log_path = write_download_log(
        download_results=download_results,
        summary=summary,
        manifest_path=manifest_path,
        target_base_path=target_base_path,
        output_dir=output_dir,
    )

    assert isinstance(log_path, Path)
    assert log_path.exists()

    # check filename format
    assert log_path.parent == output_dir
    assert re.match(r"download_log_\d{8}T\d{6}Z\.json", log_path.name)


def test_write_download_log_writes_correct_payload(tmp_path):
    """
    Verify that the JSON payload written to the log file contains
    the expected fields and values.

    This test checks:
    - manifest_path stored as string
    - target_base_path stored as string
    - summary preserved
    - results preserved
    - generated_at_utc exists
    """
    download_results = [{"status": "downloaded", "bytes_written": 123}]
    summary = {"downloaded": 1, "skipped": 0, "failed": 0, "total_bytes_written": 123}

    manifest_path = tmp_path / "manifest.json"
    target_base_path = tmp_path / "data"
    output_dir = tmp_path / "logs"

    log_path = write_download_log(
        download_results,
        summary,
        manifest_path,
        target_base_path,
        output_dir,
    )

    payload = json.loads(log_path.read_text(encoding="utf-8"))

    assert payload["manifest_path"] == str(manifest_path)
    assert payload["target_base_path"] == str(target_base_path)
    assert payload["summary"] == summary
    assert payload["results"] == download_results

    assert "generated_at_utc" in payload


def test_write_download_log_creates_output_directory(tmp_path):
    """
    Verify that write_download_log creates the output directory
    if it does not already exist.
    """
    download_results = []
    summary = {"downloaded": 0, "skipped": 0, "failed": 0, "total_bytes_written": 0}

    manifest_path = tmp_path / "manifest.json"
    target_base_path = tmp_path / "data"

    output_dir = tmp_path / "nested" / "logs"

    assert not output_dir.exists()

    log_path = write_download_log(
        download_results,
        summary,
        manifest_path,
        target_base_path,
        output_dir,
    )

    assert output_dir.exists()
    assert log_path.exists()


def test_write_download_log_default_output_dir(tmp_path, monkeypatch):
    """
    Verify that the default output_dir ('data/logs') is used when
    no output_dir is provided.

    The working directory is changed to a temporary directory
    so the test does not write files to the real project folder.
    """
    monkeypatch.chdir(tmp_path)

    download_results = []
    summary = {"downloaded": 0, "skipped": 0, "failed": 0, "total_bytes_written": 0}

    manifest_path = tmp_path / "manifest.json"
    target_base_path = tmp_path / "data"

    log_path = write_download_log(
        download_results,
        summary,
        manifest_path,
        target_base_path,
    )

    assert log_path.exists()
    assert log_path.parent == Path("data/logs")
