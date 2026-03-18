import time
from pathlib import Path

import requests

from nyc_taxi_data_service.bootstrap.downloader import (
    download_file,
)


class FakeResponse:
    def __init__(self, chunks, headers=None, status_error=None):
        self._chunks = chunks
        self.headers = headers or {}
        self._status_error = status_error

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        if self._status_error is not None:
            raise self._status_error

    def iter_content(self, chunk_size=1024 * 1024):
        for chunk in self._chunks:
            yield chunk


def test_download_file_skips_existing_file_when_size_matches(tmp_path, monkeypatch):
    """
    Verify that download_file returns a skipped result when the target file
    already exists and its size matches expected_size_bytes.

    This test checks:
    - the existing file is not modified
    - requests.get is not called
    - the result status is "skipped"
    - existing_file_size is included in the returned result
    """
    target_path = tmp_path / "yellow_2020-01.parquet"
    target_path.write_bytes(b"abcd")

    def mock_get(*args, **kwargs):
        raise AssertionError("requests.get should not be called for skipped download")

    monkeypatch.setattr(requests, "get", mock_get)

    result = download_file(
        source_url="https://example.com/yellow_2020-01.parquet",
        target_path=target_path,
        skip_existing=True,
        expected_size_bytes=4,
    )

    assert result["status"] == "skipped"
    assert result["bytes_written"] == 0
    assert result["existing_file_size"] == 4
    assert result["expected_size_bytes"] == 4
    assert result["target_path"] == str(target_path)
    assert result["error"] is None
    assert target_path.read_bytes() == b"abcd"


def test_download_file_downloads_successfully_and_replaces_temp_file(
    tmp_path, monkeypatch
):
    """
    Verify that download_file writes streamed response chunks to a temporary
    .part file, then renames that file to the final target path on success.

    This test checks:
    - streamed chunks are written correctly
    - empty chunks are ignored
    - the final target file contains the expected bytes
    - the temporary .part file does not remain after success
    - the result status is "downloaded"
    """
    target_path = tmp_path / "green_2020-01.parquet"
    source_url = "https://example.com/green_2020-01.parquet"

    def mock_get(url, stream, timeout):
        assert url == source_url
        assert stream is True
        assert timeout == 30
        return FakeResponse(
            chunks=[b"abc", b"", b"def"],
            headers={"Content-Length": "6"},
        )

    monkeypatch.setattr(requests, "get", mock_get)

    result = download_file(
        source_url=source_url,
        target_path=target_path,
        expected_size_bytes=6,
    )

    assert result["status"] == "downloaded"
    assert result["bytes_written"] == 6
    assert result["expected_size_bytes"] == 6
    assert result["target_path"] == str(target_path)
    assert result["error"] is None

    assert target_path.exists()
    assert target_path.read_bytes() == b"abcdef"
    assert not Path(str(target_path) + ".part").exists()


def test_download_file_redownloads_when_existing_size_mismatches(tmp_path, monkeypatch):
    """
    Verify that download_file does not skip when the target file already exists
    but its size does not match expected_size_bytes.

    This test checks:
    - the existing file is replaced by newly downloaded content
    - the result status is "downloaded" rather than "skipped"
    """
    target_path = tmp_path / "fhv_2020-01.parquet"
    target_path.write_bytes(b"old")

    def mock_get(url, stream, timeout):
        return FakeResponse(
            chunks=[b"newdata"],
            headers={"Content-Length": "7"},
        )

    monkeypatch.setattr(requests, "get", mock_get)

    result = download_file(
        source_url="https://example.com/fhv_2020-01.parquet",
        target_path=target_path,
        skip_existing=True,
        expected_size_bytes=7,
    )

    assert result["status"] == "downloaded"
    assert result["bytes_written"] == 7
    assert target_path.read_bytes() == b"newdata"


def test_download_file_retries_then_succeeds_after_request_exception(
    tmp_path, monkeypatch
):
    """
    Verify that download_file retries after a request failure and succeeds on a
    later attempt when max_attempts has not been exhausted.

    This test checks:
    - a RequestException on the first attempt does not immediately fail
    - time.sleep is called between attempts
    - a later successful attempt returns "downloaded"
    - the final target file contains the successful response content
    """
    target_path = tmp_path / "yellow_2020-02.parquet"
    calls = {"count": 0}
    sleep_calls = []

    def mock_get(url, stream, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            raise requests.RequestException("temporary network issue")
        return FakeResponse(
            chunks=[b"retry-ok"],
            headers={"Content-Length": "8"},
        )

    def mock_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(time, "sleep", mock_sleep)

    result = download_file(
        source_url="https://example.com/yellow_2020-02.parquet",
        target_path=target_path,
        expected_size_bytes=8,
        max_attempts=3,
        retry_delay_seconds=1.5,
    )

    assert calls["count"] == 2
    assert sleep_calls == [1.5]
    assert result["status"] == "downloaded"
    assert result["bytes_written"] == 8
    assert result["error"] is None
    assert target_path.read_bytes() == b"retry-ok"


def test_download_file_returns_failed_after_size_mismatch_exhausts_attempts(
    tmp_path, monkeypatch
):
    """
    Verify that download_file returns a failed result when every attempt
    completes with a downloaded size that does not match expected_size_bytes.

    This test checks:
    - size mismatch raises the retry path internally
    - the function returns "failed" after max_attempts is reached
    - the final target file is not created
    - the temporary .part file is cleaned up when cleanup_partial_on_failure=True
    """
    target_path = tmp_path / "green_2020-02.parquet"
    sleep_calls = []

    def mock_get(url, stream, timeout):
        return FakeResponse(
            chunks=[b"abc"],
            headers={"Content-Length": "3"},
        )

    def mock_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(time, "sleep", mock_sleep)

    result = download_file(
        source_url="https://example.com/green_2020-02.parquet",
        target_path=target_path,
        expected_size_bytes=5,
        max_attempts=2,
        retry_delay_seconds=0.25,
        cleanup_partial_on_failure=True,
    )

    assert result["status"] == "failed"
    assert result["bytes_written"] == 3
    assert "downloaded size mismatch" in result["error"]
    assert not target_path.exists()
    assert not Path(str(target_path) + ".part").exists()
    assert sleep_calls == [0.25]


def test_download_file_preserves_partial_file_when_cleanup_disabled(
    tmp_path, monkeypatch
):
    """
    Verify that download_file leaves the temporary .part file in place after a
    failed attempt when cleanup_partial_on_failure is False.

    This test checks:
    - the function returns "failed" after the final attempt
    - the partial temp file remains on disk for inspection or recovery
    - the final target file is not created
    """
    target_path = tmp_path / "fhvhv_2020-01.parquet"

    def mock_get(url, stream, timeout):
        return FakeResponse(
            chunks=[b"abc"],
            headers={"Content-Length": "3"},
        )

    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(time, "sleep", lambda seconds: None)

    result = download_file(
        source_url="https://example.com/fhvhv_2020-01.parquet",
        target_path=target_path,
        expected_size_bytes=10,
        max_attempts=1,
        cleanup_partial_on_failure=False,
    )

    temp_path = Path(str(target_path) + ".part")

    assert result["status"] == "failed"
    assert not target_path.exists()
    assert temp_path.exists()
    assert temp_path.read_bytes() == b"abc"
