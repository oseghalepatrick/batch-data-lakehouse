from urllib.parse import urlparse

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from src.utils.logger import get_logger

logger = get_logger(__name__)


class MinIOStorage:
    """
    Handle Parquet storage operations against MinIO/S3.

    Supports:
    - Relative paths under the configured bucket/root path
    - Full s3:// and s3a:// paths
    - Parquet read/write
    - Object/prefix existence checks
    - Source deletion
    """

    def __init__(self, config):
        self.bucket = config["bucket"]
        self.root_path = config["root_path"].strip("/")

        self.storage_options = {
            "key": config["access_key"],
            "secret": config["secret_key"],
            "client_kwargs": {
                "endpoint_url": config["endpoint"],
            },
        }

        self.s3_client = boto3.client(
            "s3",
            endpoint_url=config["endpoint"],
            aws_access_key_id=config["access_key"],
            aws_secret_access_key=config["secret_key"],
        )

    def path(self, path: str) -> str:
        """
        Build an S3 URI for a relative path.

        Full s3:// or s3a:// paths are returned unchanged.
        """
        if path.startswith(("s3://", "s3a://")):
            return path

        path = path.lstrip("/")

        if self.root_path:
            return f"s3://{self.bucket}/{self.root_path}/{path}"

        return f"s3://{self.bucket}/{path}"

    @staticmethod
    def _parse_s3_path(path: str) -> tuple[str, str]:
        """
        Parse an s3:// or s3a:// URI into bucket and key/prefix.
        """
        normalized_path = path.replace("s3a://", "s3://", 1)
        parsed = urlparse(normalized_path)

        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError(f"Invalid S3 path: {path}")

        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        return bucket, key

    def write_parquet(self, df: pd.DataFrame, path: str) -> str:
        """
        Write a DataFrame as Parquet to MinIO.
        """
        file_path = self.path(path)

        df.to_parquet(
            file_path,
            engine="pyarrow",
            index=False,
            storage_options=self.storage_options,
        )

        logger.info("Parquet written: %s", file_path)

        return file_path

    def read_parquet(self, path: str) -> pd.DataFrame:
        """
        Read a Parquet file or dataset from MinIO.
        """
        file_path = self.path(path)

        logger.info("Reading Parquet: %s", file_path)

        return pd.read_parquet(
            file_path,
            engine="pyarrow",
            storage_options=self.storage_options,
        )

    def exists(self, path: str) -> bool:
        """
        Check whether an object or prefix exists in MinIO.

        Unlike the previous implementation, this does not
        read the entire Parquet file.
        """
        file_path = self.path(path)
        bucket, key = self._parse_s3_path(file_path)

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=key,
                MaxKeys=1,
            )

            return bool(response.get("Contents"))

        except ClientError as exc:
            logger.error(
                "Failed checking path existence: %s - %s",
                file_path,
                exc,
            )
            raise

    def source_exists(self, source_path: str) -> bool:
        """
        Check whether at least one parquet file exists
        under an S3/MinIO source path.

        Supports both:
        - relative paths
        - full s3:// or s3a:// paths
        """
        file_path = self.path(source_path)

        bucket, prefix = self._parse_s3_path(file_path)

        response = self.s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
        )

        parquet_files = [
            obj
            for obj in response.get("Contents", [])
            if obj["Key"].lower().endswith(".parquet")
        ]

        return len(parquet_files) > 0

    def delete_source(self, path: str) -> None:
        """
        Delete a single object from MinIO/S3.
        """
        file_path = self.path(path)
        bucket, key = self._parse_s3_path(file_path)

        try:
            self.s3_client.delete_object(
                Bucket=bucket,
                Key=key,
            )

            logger.info("Source deleted: %s", file_path)

        except ClientError as exc:
            logger.error(f"Failed deleting source: {file_path} - {exc}")
            raise
