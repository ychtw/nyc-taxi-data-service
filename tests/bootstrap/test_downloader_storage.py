from pathlib import Path
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from nyc_taxi_data_service.bootstrap.downloader_storage import (
    LocalFileWriter,
    S3FileWriter,
    build_storage_writer,
    get_storage_root,
)

# ---------------------------
# Fixtures
# ---------------------------


@pytest.fixture
def temp_file(tmp_path):
    """Create a temporary file with content."""
    f = tmp_path / "temp.parquet"
    f.write_bytes(b"data")
    return f


@pytest.fixture
def local_writer(tmp_path):
    """Provide a LocalFileWriter with a temp base path."""
    return LocalFileWriter(target_base_path=tmp_path)


@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client."""
    with patch(
        "nyc_taxi_data_service.bootstrap.downloader_storage.boto3.client"
    ) as mock:
        yield mock.return_value


@pytest.fixture
def s3_writer(mock_s3_client):
    """Provide an S3FileWriter with mocked client."""
    return S3FileWriter(bucket_name="test-bucket", key_prefix="prefix")


# ---------------------------
# LocalFileWriter tests
# ---------------------------


def test_local_writer_persist_file_moves_file(local_writer, temp_file):
    """Persisting a file should move it to final destination."""
    result = local_writer.persist_file(temp_file, "a/b/file.parquet")

    target_path = Path(result)

    assert target_path.exists()
    assert target_path.read_bytes() == b"data"
    assert not temp_file.exists()


@pytest.mark.parametrize(
    "exists",
    [True, False],
    ids=["file_exists", "file_missing"],
)
def test_local_writer_exists(tmp_path, local_writer, exists):
    """exists() should reflect actual file presence."""
    rel_path = "file.parquet"
    target = tmp_path / rel_path

    if exists:
        target.write_bytes(b"x")

    assert local_writer.exists(rel_path) is exists


def test_local_writer_get_target_identifier(local_writer):
    """Identifier should be resolved full local path."""
    result = local_writer.get_target_identifier("x/y/file.parquet")

    assert result.endswith("x/y/file.parquet")


# ---------------------------
# S3FileWriter tests
# ---------------------------


@pytest.mark.parametrize(
    "prefix, rel_path, expected",
    [
        ("", "file.parquet", "file.parquet"),
        ("data", "file.parquet", "data/file.parquet"),
    ],
    ids=["no_prefix", "with_prefix"],
)
def test_s3_build_object_key(s3_writer, prefix, rel_path, expected):
    """Object key should include prefix when provided."""
    s3_writer.key_prefix = prefix
    assert s3_writer.build_object_key(rel_path) == expected


def test_s3_exists_true(s3_writer, mock_s3_client):
    """exists() should return True when head_object succeeds."""
    mock_s3_client.head_object.return_value = {}

    assert s3_writer.exists("file.parquet") is True


@pytest.mark.parametrize(
    "error_code",
    ["404", "NoSuchKey", "NotFound"],
    ids=["404", "NoSuchKey", "NotFound"],
)
def test_s3_exists_false(s3_writer, mock_s3_client, error_code):
    """exists() should return False for not-found errors."""

    mock_s3_client.head_object.side_effect = ClientError(
        {"Error": {"Code": error_code}},
        "HeadObject",
    )

    assert s3_writer.exists("file.parquet") is False


def test_s3_persist_file_uploads_and_deletes(temp_file, s3_writer, mock_s3_client):
    """persist_file should upload and delete temp file."""
    result = s3_writer.persist_file(temp_file, "file.parquet")

    mock_s3_client.upload_file.assert_called_once()
    assert not temp_file.exists()
    assert result.startswith("s3://")


# ---------------------------
# Factory + helpers
# ---------------------------


@pytest.mark.parametrize(
    "config, expected_type",
    [
        ({"target_storage_type": "local", "target_base_path": "data"}, LocalFileWriter),
        (
            {
                "target_storage_type": "s3",
                "bucket_name": "b",
            },
            S3FileWriter,
        ),
    ],
    ids=["local", "s3"],
)
def test_build_storage_writer(config, expected_type):
    """Factory should return correct writer type."""
    writer = build_storage_writer(config)
    assert isinstance(writer, expected_type)


def test_build_storage_writer_invalid_type():
    """Invalid storage type should raise ValueError."""
    with pytest.raises(ValueError):
        build_storage_writer({"target_storage_type": "invalid"})


@pytest.mark.parametrize(
    "config, expected",
    [
        (
            {"target_storage_type": "local", "target_base_path": "/data"},
            "/data",
        ),
        (
            {"target_storage_type": "s3", "bucket_name": "b", "key_prefix": "x"},
            "s3://b/x",
        ),
        (
            {"target_storage_type": "s3", "bucket_name": "b"},
            "s3://b",
        ),
    ],
    ids=["local", "s3_with_prefix", "s3_no_prefix"],
)
def test_get_storage_root(config, expected):
    """get_storage_root should build correct identifier."""
    assert get_storage_root(config) == expected
