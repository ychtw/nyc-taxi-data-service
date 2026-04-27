import logging
from abc import ABC, abstractmethod
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)  # initialize default logger


class StorageWriter(ABC):
    """
    Interface for writing downloaded files to a destination.
    """

    @abstractmethod
    def exists(self, target_relative_path: str) -> bool:
        """
        Check whether the destination already contains the target file.
        """
        raise NotImplementedError

    @abstractmethod
    def get_target_identifier(self, target_relative_path: str) -> str:
        """
        Return a destination-specific identifier for logging.
        """
        raise NotImplementedError

    @abstractmethod
    def persist_file(self, temp_file_path: Path, target_relative_path: str) -> str:
        """
        Persist a completed temporary file to the destination backend.
        """
        raise NotImplementedError


class LocalFileWriter(StorageWriter):
    """
    Write downloaded files to the local filesystem.

    Files are finalized by atomically renaming the completed temporary file
    into its final location under target_base_path.
    """

    def __init__(self, target_base_path: Path | str) -> None:
        """
        Initialize a local filesystem writer.

        Args:
            target_base_path: root directory where files should be stored
        """
        self.target_base_path = Path(target_base_path)

    def resolve_target_path(self, target_relative_path: str) -> Path:
        """
        Resolve the final local path for a manifest entry.
        """
        return self.target_base_path / target_relative_path

    def exists(self, target_relative_path: str) -> bool:
        """
        Check whether the target file already exists locally.
        """
        return self.resolve_target_path(target_relative_path).exists()

    def get_target_identifier(self, target_relative_path: str) -> str:
        """
        Return the final local path as a string for logging.
        """
        return str(self.resolve_target_path(target_relative_path))

    def persist_file(self, temp_file_path: Path, target_relative_path: str) -> str:
        """
        Move a completed temporary file into its final local destination.

        Returns:
            Final local destination path as a string.
        """
        target_path = self.resolve_target_path(target_relative_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        temp_file_path.replace(target_path)
        return str(target_path)


class S3FileWriter(StorageWriter):
    """
    Write downloaded files to an S3 bucket.
    """

    def __init__(
        self,
        bucket_name: str,
        key_prefix: str = "",
        region_name: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        """
        Initialize an S3 writer.

        Args:
            bucket_name: destination S3 bucket name
            key_prefix: optional prefix added before target_relative_path
            region_name: AWS region for the S3 client
            endpoint_url: optional custom endpoint, such as LocalStack
        """
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix.strip("/")

        client_kwargs = {
            "service_name": "s3",
            "region_name": region_name,
        }
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        self.client = boto3.client(**client_kwargs)

    def build_object_key(self, target_relative_path: str) -> str:
        """
        Build the S3 object key for a manifest entry.

        Args:
            target_relative_path: relative path from the manifest entry

        Returns:
            Full object key under the configured prefix.
        """
        if self.key_prefix:
            return f"{self.key_prefix}/{target_relative_path}"
        return target_relative_path

    def exists(self, target_relative_path: str) -> bool:
        """
        Check whether the target object already exists in S3.

        Args:
            target_relative_path: relative path from the manifest entry

        Returns:
            True if the object exists, otherwise False.
        """
        key = self.build_object_key(target_relative_path)

        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def get_target_identifier(self, target_relative_path: str) -> str:
        """
        Return the final S3 URI for logging.

        Args:
            target_relative_path: relative path from the manifest entry

        Returns:
            S3 URI string.
        """
        key = self.build_object_key(target_relative_path)
        return f"s3://{self.bucket_name}/{key}"

    def persist_file(self, temp_file_path: Path, target_relative_path: str) -> str:
        """
        Upload a completed temporary file to S3.

        Args:
            temp_file_path: path to the completed local temporary file
            target_relative_path: relative path from the manifest entry

        Returns:
            Final S3 URI as a string.
        """
        key = self.build_object_key(target_relative_path)

        logger.debug(
            "Uploading file to S3: %s -> s3://%s/%s",
            temp_file_path,
            self.bucket_name,
            key,
        )

        self.client.upload_file(
            Filename=str(temp_file_path),
            Bucket=self.bucket_name,
            Key=key,
        )

        temp_file_path.unlink(missing_ok=True)

        return f"s3://{self.bucket_name}/{key}"


def build_storage_writer(storage_config: dict) -> StorageWriter:
    """
    Build a storage writer from resolved runtime or manifest storage config.

    Args:
        storage_config: resolved storage configuration dictionary

    Returns:
        Storage writer instance for the configured destination type.

    Raises:
        ValueError: if the storage type is unsupported
    """
    target_storage_type = storage_config["target_storage_type"]

    if target_storage_type == "local":
        return LocalFileWriter(target_base_path=storage_config["target_base_path"])

    if target_storage_type == "s3":
        return S3FileWriter(
            bucket_name=storage_config["bucket_name"],
            key_prefix=storage_config.get("key_prefix", ""),
            region_name=storage_config.get("region_name"),
            endpoint_url=storage_config.get("endpoint_url"),
        )

    raise ValueError(f"Unsupported target_storage_type: {target_storage_type}")


def get_storage_root(storage_config: dict) -> str:
    """
    Build a human-readable identifier for the storage destination.
    """
    if storage_config["target_storage_type"] == "local":
        return str(storage_config["target_base_path"])

    if storage_config["target_storage_type"] == "s3":
        bucket = storage_config["bucket_name"]
        prefix = storage_config.get("key_prefix", "").strip("/")
        return f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"

    return "unknown"
