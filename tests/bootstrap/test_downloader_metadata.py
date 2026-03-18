import shutil
from collections import namedtuple

import pytest
import requests

from nyc_taxi_data_service.bootstrap.downloader import (
    check_local_free_space,
    get_remote_file_metadata,
)


class DummyResponse:
    def __init__(self, status_code, ok, headers):
        self.status_code = status_code
        self.ok = ok
        self.headers = headers


DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


def test_get_remote_file_metadata_returns_expected_metadata(monkeypatch):
    """
    Verify that get_remote_file_metadata sends an HTTP HEAD request and
    returns the expected metadata dictionary when the request succeeds.

    This test checks:
    - requests.head is called with allow_redirects=True and the given timeout
    - Content-Length is converted from string to int
    - other selected response headers are copied into the result
    - source_url, status_code, and ok are preserved in the output
    """
    source_url = "https://example.com/yellow_2020-01.parquet"

    def mock_head(url, allow_redirects, timeout):
        assert url == source_url
        assert allow_redirects is True
        assert timeout == 15
        return DummyResponse(
            status_code=200,
            ok=True,
            headers={
                "Content-Length": "12345",
                "Content-Type": "application/octet-stream",
                "ETag": '"abc123"',
                "Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT",
            },
        )

    monkeypatch.setattr(requests, "head", mock_head)

    result = get_remote_file_metadata(source_url=source_url, timeout=15)

    assert result == {
        "source_url": source_url,
        "status_code": 200,
        "ok": True,
        "content_length": 12345,
        "content_type": "application/octet-stream",
        "etag": '"abc123"',
        "last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
    }


def test_get_remote_file_metadata_sets_content_length_to_none_when_missing(monkeypatch):
    """
    Verify that get_remote_file_metadata returns content_length=None when
    the response does not include a Content-Length header.
    """
    source_url = "https://example.com/green_2020-01.parquet"

    def mock_head(url, allow_redirects, timeout):
        return DummyResponse(
            status_code=200,
            ok=True,
            headers={
                "Content-Type": "application/octet-stream",
                "ETag": '"etag-456"',
                "Last-Modified": "Tue, 02 Jan 2024 00:00:00 GMT",
            },
        )

    monkeypatch.setattr(requests, "head", mock_head)

    result = get_remote_file_metadata(source_url=source_url)

    assert result == {
        "source_url": source_url,
        "status_code": 200,
        "ok": True,
        "content_length": None,
        "content_type": "application/octet-stream",
        "etag": '"etag-456"',
        "last_modified": "Tue, 02 Jan 2024 00:00:00 GMT",
    }


def test_get_remote_file_metadata_reraises_request_exception(monkeypatch):
    """
    Verify that get_remote_file_metadata re-raises requests.RequestException
    when the HTTP HEAD request fails.
    """
    source_url = "https://example.com/bad-file.parquet"

    def mock_head(url, allow_redirects, timeout):
        raise requests.RequestException("connection failed")

    monkeypatch.setattr(requests, "head", mock_head)

    with pytest.raises(requests.RequestException, match="connection failed"):
        get_remote_file_metadata(source_url=source_url)


def test_check_local_free_space_returns_enough_space_true(tmp_path, monkeypatch):
    """
    Verify that enough_space=True is returned when free disk space
    is greater than or equal to required_bytes.
    """

    def mock_disk_usage(path):
        return DiskUsage(total=1000, used=200, free=800)

    monkeypatch.setattr(shutil, "disk_usage", mock_disk_usage)

    result = check_local_free_space(
        target_base_path=tmp_path,
        required_bytes=500,
    )

    assert result["target_base_path"] == str(tmp_path)
    assert result["required_bytes"] == 500
    assert result["free_bytes"] == 800
    assert result["enough_space"] is True


def test_check_local_free_space_returns_enough_space_false(tmp_path, monkeypatch):
    """
    Verify that enough_space=False is returned when free disk space
    is smaller than required_bytes.
    """

    def mock_disk_usage(path):
        return DiskUsage(total=1000, used=950, free=50)

    monkeypatch.setattr(shutil, "disk_usage", mock_disk_usage)

    result = check_local_free_space(
        target_base_path=tmp_path,
        required_bytes=100,
    )

    assert result["enough_space"] is False
    assert result["free_bytes"] == 50


def test_check_local_free_space_creates_directory_if_missing(tmp_path, monkeypatch):
    """
    Verify that the target_base_path directory is created automatically
    if it does not exist before calling the function.
    """

    def mock_disk_usage(path):
        return DiskUsage(total=1000, used=100, free=900)

    monkeypatch.setattr(shutil, "disk_usage", mock_disk_usage)

    new_dir = tmp_path / "new_dir"

    assert not new_dir.exists()

    result = check_local_free_space(
        target_base_path=new_dir,
        required_bytes=100,
    )

    assert new_dir.exists()
    assert result["target_base_path"] == str(new_dir)


def test_check_local_free_space_calls_disk_usage_with_created_path(
    tmp_path, monkeypatch
):
    """
    Verify that shutil.disk_usage is called with the resolved Path
    after the directory is created.
    """

    called_paths = []

    def mock_disk_usage(path):
        called_paths.append(path)
        return DiskUsage(total=1000, used=100, free=900)

    monkeypatch.setattr(shutil, "disk_usage", mock_disk_usage)

    target_dir = tmp_path / "space_check"

    check_local_free_space(
        target_base_path=target_dir,
        required_bytes=10,
    )

    assert called_paths == [target_dir]
