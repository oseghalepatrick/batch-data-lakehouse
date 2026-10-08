from pathlib import Path

import pyarrow.fs as fs
import yaml
from pyiceberg.catalog import load_catalog


def get_config(config_path: str = "config/generator_config.yaml"):
    """
    Load generator configuration file.
    The config path is resolved relative to the project root.
    """

    project_root = Path(__file__).resolve().parents[2]
    full_path = project_root / config_path

    if not full_path.exists():
        raise FileNotFoundError(f"Missing config file: {full_path}")

    with full_path.open("r") as file:
        return yaml.safe_load(file)


def get_catalog_config():

    return load_catalog(
        "nessie",
        **{
            "type": "rest",
            "uri": "http://nessie:19120/iceberg/main/",
            "warehouse": "warehouse",
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password123",
            "s3.region": "us-east-1",
        },
    )


def get_s3fs_config():

    return fs.S3FileSystem(
        access_key="admin",
        secret_key="password123",
        endpoint_override="minio:9000",
        scheme="http",
        region="us-east-1",
    )
