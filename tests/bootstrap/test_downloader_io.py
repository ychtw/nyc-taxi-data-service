import json
from pathlib import Path

import pytest
import requests

from nyc_taxi_data_service.bootstrap.downloader_io import (
    download_file,
    format_bytes,
    load_manifest,
)

# ---------------------------
# Fixtures / helpers
# ---------------------------


@pytest.fixture
def fake_session():
    """Provide a fake session with configurable get behavior."""

    class FakeSession:
        def __init__(self):
            self._handler = None

        def set_handler(self, handler):
            self._handler = handler

        def get(self, *args, **kwargs):
            return self._handler(*args, **kwargs)

    return FakeSession()


@pytest.fixture
def fake_storage_writer(tmp_path):
    """Provide a fake storage writer backed by a temp directory."""

    class FakeStorageWriter:
        def __init__(self, base_path: Path):
            self.base_path = base_path
            self.persist_calls = []

        def _resolve(self, target_relative_path: str) -> Path:
            return self.base_path / target_relative_path

        def exists(self, target_relative_path: str) -> bool:
            return self._resolve(target_relative_path).exists()

        def get_target_identifier(self, target_relative_path: str) -> str:
            return str(self._resolve(target_relative_path))

        def persist_file(self, temp_file_path: Path, target_relative_path: str) -> str:
            target_path = self._resolve(target_relative_path)
            target_path.parent.mkdir(parents=True, exist_ok=True)
            temp_file_path.replace(target_path)
            self.persist_calls.append(
                {
                    "temp_file_path": str(temp_file_path),
                    "target_relative_path": target_relative_path,
                    "target_path": str(target_path),
                }
            )
            return str(target_path)

    return FakeStorageWriter(tmp_path)


class FakeResponse:
    """Minimal response object for streamed download tests."""

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
# format_bytes tests
# ---------------------------


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
    ids=[
        "none",
        "zero",
        "one_byte",
        "bytes",
        "max_bytes_before_kb",
        "one_kb",
        "fractional_kb",
        "one_mb",
        "five_mb",
        "one_gb",
        "one_tb",
    ],
)
def test_format_bytes(num_bytes, expected):
    """format_bytes should return human-readable byte strings."""
    assert format_bytes(num_bytes) == expected


# ---------------------------
# load_manifest tests
# ---------------------------


def test_load_manifest_returns_parsed_manifest_dict(tmp_path):
    """load_manifest should return parsed JSON content."""
    manifest_data = {
        "generated_at_utc": "2026-03-17T00:00:00Z",
        "storage": {
            "target_storage_type": "local",
            "target_base_path": "data/raw",
        },
        "entries": [
            {
                "source_url": "https://example.com/file.parquet",
                "target_relative_path": "yellow/2020-01.parquet",
            }
        ],
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest_data), encoding="utf-8")

    result = load_manifest(manifest_path)

    assert result == manifest_data


@pytest.mark.parametrize(
    "content, expected_exception",
    [
        ("{invalid json}", json.JSONDecodeError),
        (None, FileNotFoundError),
    ],
    ids=["invalid_json", "missing_file"],
)
def test_load_manifest_error_cases(tmp_path, content, expected_exception):
    """load_manifest should raise for invalid JSON or missing file."""
    manifest_path = tmp_path / "manifest.json"

    if content is not None:
        manifest_path.write_text(content, encoding="utf-8")

    with pytest.raises(expected_exception):
        load_manifest(manifest_path)


# ---------------------------
# download_file tests
# ---------------------------


def test_download_file_skips_existing_file(fake_session, fake_storage_writer):
    """download_file should skip when destination already exists."""
    target_relative_path = "yellow/file.parquet"
    existing_path = Path(
        fake_storage_writer.get_target_identifier(target_relative_path)
    )
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_bytes(b"existing")

    result = download_file(
        session=fake_session,
        source_url="https://example.com/file.parquet",
        target_relative_path=target_relative_path,
        storage_writer=fake_storage_writer,
        skip_existing=True,
    )

    assert result["status"] == "skipped"
    assert result["bytes_written"] == 0
    assert result["attempts"] == 0
    assert existing_path.read_bytes() == b"existing"


def test_download_file_successful_download(fake_session, fake_storage_writer):
    """download_file should write streamed content and finalize the file."""

    def handler(url, stream, timeout):
        return FakeResponse(
            chunks=[b"abc", b"", b"def"],
            headers={"Content-Length": "6"},
        )

    fake_session.set_handler(handler)
    target_relative_path = "yellow/file.parquet"

    result = download_file(
        session=fake_session,
        source_url="https://example.com/file.parquet",
        target_relative_path=target_relative_path,
        storage_writer=fake_storage_writer,
    )

    final_path = Path(fake_storage_writer.get_target_identifier(target_relative_path))
    temp_path = Path(result["temp_target_path"])

    assert result["status"] == "downloaded"
    assert result["bytes_written"] == 6
    assert result["attempts"] == 1
    assert final_path.exists()
    assert final_path.read_bytes() == b"abcdef"
    assert not temp_path.exists()
    assert len(fake_storage_writer.persist_calls) == 1


def test_download_file_returns_failed_on_request_exception(
    fake_session, fake_storage_writer
):
    """download_file should fail when the request raises RequestException."""

    def handler(url, stream, timeout):
        raise requests.RequestException("always fails")

    fake_session.set_handler(handler)

    result = download_file(
        session=fake_session,
        source_url="https://example.com/file.parquet",
        target_relative_path="yellow/file.parquet",
        storage_writer=fake_storage_writer,
    )

    assert result["status"] == "failed"
    assert result["attempts"] == 1
    assert "always fails" in result["error"]


@pytest.mark.parametrize(
    "cleanup_partial_on_failure",
    [True, False],
    ids=["cleanup_enabled", "cleanup_disabled"],
)
def test_download_file_partial_file_cleanup_behavior(
    fake_session,
    fake_storage_writer,
    cleanup_partial_on_failure,
):
    """download_file should honor partial temp file cleanup behavior."""

    def handler(url, stream, timeout):
        class PartialFailResponse:
            headers = {"Content-Length": "10"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def raise_for_status(self):
                pass

            def iter_content(self, chunk_size):
                yield b"partial"
                raise requests.RequestException("mid-stream failure")

        return PartialFailResponse()

    fake_session.set_handler(handler)
    target_relative_path = "yellow/file.parquet"
    safe_name = target_relative_path.replace("/", "__")
    temp_path = Path(".tmp/downloads") / f"{safe_name}.part"

    result = download_file(
        session=fake_session,
        source_url="https://example.com/file.parquet",
        target_relative_path=target_relative_path,
        storage_writer=fake_storage_writer,
        cleanup_partial_on_failure=cleanup_partial_on_failure,
    )

    if cleanup_partial_on_failure:
        assert not temp_path.exists()
    else:
        assert temp_path.exists()
        assert temp_path.read_bytes() == b"partial"
        temp_path.unlink()

    assert result["status"] == "failed"


def test_download_file_returns_failed_on_persist_error(
    fake_session, fake_storage_writer, monkeypatch
):
    """download_file should fail when persist_file raises an error."""

    def handler(url, stream, timeout):
        return FakeResponse(chunks=[b"abc"], headers={"Content-Length": "3"})

    def broken_persist_file(temp_file_path, target_relative_path):
        raise OSError("persist failed")

    fake_session.set_handler(handler)
    monkeypatch.setattr(
        fake_storage_writer,
        "persist_file",
        broken_persist_file,
    )

    result = download_file(
        session=fake_session,
        source_url="https://example.com/file.parquet",
        target_relative_path="yellow/file.parquet",
        storage_writer=fake_storage_writer,
    )

    assert result["status"] == "failed"
    assert result["attempts"] == 1
    assert "persist failed" in result["error"]
