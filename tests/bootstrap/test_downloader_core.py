import time
from pathlib import Path

import pytest
import requests

from nyc_taxi_data_service.bootstrap.downloader import download_file

# ---------------------------
# Fixtures
# ---------------------------


@pytest.fixture
def fake_session(monkeypatch):
    """
    Provide a mock requests.Session with controllable .get behavior.
    """

    class FakeSession:
        def __init__(self):
            self._handler = None

        def set_handler(self, handler):
            self._handler = handler

        def get(self, *args, **kwargs):
            return self._handler(*args, **kwargs)

    session = FakeSession()
    return session


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
        if self._status_error:
            raise self._status_error

    def iter_content(self, chunk_size=1024 * 1024):
        for chunk in self._chunks:
            yield chunk


# ---------------------------
# Tests
# ---------------------------


def test_download_file_skips_existing_file(tmp_path, fake_session):
    """
    Verify that download_file skips when target file already exists.
    """
    target_path = tmp_path / "file.parquet"
    target_path.write_bytes(b"existing")

    result = download_file(
        session=fake_session,
        source_url="https://example.com/file.parquet",
        target_path=target_path,
        skip_existing=True,
    )

    assert result["status"] == "skipped"
    assert result["bytes_written"] == 0
    assert result["attempts"] == 0
    assert target_path.read_bytes() == b"existing"


def test_download_file_successful_download(tmp_path, fake_session):
    """
    Verify that download_file writes streamed content and finalizes file.
    """

    def handler(url, stream, timeout):
        return FakeResponse(
            chunks=[b"abc", b"", b"def"],
            headers={"Content-Length": "6"},
        )

    fake_session.set_handler(handler)

    target_path = tmp_path / "file.parquet"

    result = download_file(
        session=fake_session,
        source_url="https://example.com/file.parquet",
        target_path=target_path,
    )

    assert result["status"] == "downloaded"
    assert result["bytes_written"] == 6
    assert result["attempts"] == 1

    assert target_path.exists()
    assert target_path.read_bytes() == b"abcdef"
    assert not Path(str(target_path) + ".part").exists()


@pytest.mark.parametrize(
    "fail_first",
    [True, False],
    ids=["retry_then_success", "success_first_try"],
)
def test_download_file_retries_on_request_exception(
    tmp_path, fake_session, monkeypatch, fail_first
):
    """
    Verify retry behavior for transient request failures.
    """
    calls = {"count": 0}

    def handler(url, stream, timeout):
        calls["count"] += 1
        if fail_first and calls["count"] == 1:
            raise requests.RequestException("temporary error")

        return FakeResponse(chunks=[b"ok"], headers={"Content-Length": "2"})

    fake_session.set_handler(handler)

    sleep_calls = []
    monkeypatch.setattr(time, "sleep", lambda s: sleep_calls.append(s))

    target_path = tmp_path / "file.parquet"

    result = download_file(
        session=fake_session,
        source_url="https://example.com/file.parquet",
        target_path=target_path,
        max_attempts=2,
    )

    assert result["status"] == "downloaded"
    assert target_path.read_bytes() == b"ok"

    if fail_first:
        assert calls["count"] == 2
    else:
        assert calls["count"] == 1


def test_download_file_returns_failed_after_all_attempts(tmp_path, fake_session):
    """
    Verify that download_file returns failed when all attempts error.
    """

    def handler(url, stream, timeout):
        raise requests.RequestException("always fails")

    fake_session.set_handler(handler)

    target_path = tmp_path / "file.parquet"

    result = download_file(
        session=fake_session,
        source_url="https://example.com/file.parquet",
        target_path=target_path,
        max_attempts=2,
    )

    assert result["status"] == "failed"
    assert not target_path.exists()


@pytest.mark.parametrize(
    "cleanup",
    [True, False],
    ids=["cleanup_enabled", "cleanup_disabled"],
)
def test_download_file_partial_file_cleanup_behavior(tmp_path, fake_session, cleanup):
    """
    Verify behavior of .part file cleanup when failure occurs mid-stream.
    """

    def handler(url, stream, timeout):
        class PartialFailResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def raise_for_status(self):
                pass

            def iter_content(self, chunk_size):
                yield b"partial"
                raise requests.RequestException("mid-stream failure")

            headers = {"Content-Length": "10"}

        return PartialFailResponse()

    fake_session.set_handler(handler)

    target_path = tmp_path / "file.parquet"

    result = download_file(
        session=fake_session,
        source_url="https://example.com/file.parquet",
        target_path=target_path,
        max_attempts=1,
        cleanup_partial_on_failure=cleanup,
    )

    temp_path = Path(str(target_path) + ".part")

    if cleanup:
        assert not temp_path.exists()
    else:
        assert temp_path.exists()
        assert temp_path.read_bytes() == b"partial"

    assert result["status"] == "failed"
    assert not target_path.exists()
