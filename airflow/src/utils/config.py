"""Load application, S3, and Iceberg catalog configuration."""

import os
from pathlib import Path

import pyarrow.fs as fs
import yaml
from pyiceberg.catalog import load_catalog


def get_required_env(name: str) -> str:
    """Return a required environment variable or raise a clear error."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is not set: {name}")
    return value


def get_config(config_path: str = "config/generator_config.yaml"):
    """Load generator configuration and inject runtime credentials."""
    project_root = Path(__file__).resolve().parents[2]
    full_path = project_root / config_path

    if not full_path.exists():
        raise FileNotFoundError(f"Missing config file: {full_path}")

    with full_path.open("r") as file:
        config = yaml.safe_load(file)

    # Credentials are supplied at runtime and never stored in YAML.
    minio = config["data_generation"]["minio"]
    minio["access_key"] = get_required_env("AWS_ACCESS_KEY_ID")
    minio["secret_key"] = get_required_env("AWS_SECRET_ACCESS_KEY")

    return config


def get_catalog_config():
    """Create the Nessie-backed Iceberg catalog using environment credentials."""
    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": "http://nessie:19120/iceberg/main/",
            "warehouse": "warehouse",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": get_required_env("AWS_ACCESS_KEY_ID"),
            "s3.secret-access-key": get_required_env("AWS_SECRET_ACCESS_KEY"),
            "s3.region": os.getenv("AWS_REGION", "us-east-1"),
        },
    )


def get_s3fs_config():
    """Create the PyArrow S3 filesystem using environment credentials."""
    return fs.S3FileSystem(
        access_key=get_required_env("AWS_ACCESS_KEY_ID"),
        secret_key=get_required_env("AWS_SECRET_ACCESS_KEY"),
        endpoint_override="minio:9000",
        scheme="http",
        region=os.getenv("AWS_REGION", "us-east-1"),
    )
