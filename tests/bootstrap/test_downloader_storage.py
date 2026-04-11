import shutil
from collections import namedtuple

from nyc_taxi_data_service.bootstrap.downloader import (
    check_local_free_space,
)


class DummyResponse:
    def __init__(self, status_code, ok, headers):
        self.status_code = status_code
        self.ok = ok
        self.headers = headers


DiskUsage = namedtuple("DiskUsage", ["total", "used", "free"])


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
