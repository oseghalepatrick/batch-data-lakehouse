import os

import boto3

from src.utils.config import get_catalog_config, get_config

catalog = get_catalog_config()
# MinIO configuration
config = get_config()
s3 = boto3.client(
    "s3",
    endpoint_url=config["endpoint"],
    aws_access_key_id=config["access_key"],
    aws_secret_access_key=config["secret_key"],
)

bucket_name = "warehouse"
schemas_to_clean = ["silver", "gold"]


def delete_s3_prefix(bucket, prefix):
    """
    Delete all objects under an S3 prefix
    """
    paginator = s3.get_paginator("list_objects_v2")

    objects = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            objects.append({"Key": obj["Key"]})

    if objects:
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": objects},
        )

    print(f"Deleted {len(objects)} objects from s3://{bucket}/{prefix}")


def delete_local_file(file_path):
    """
    Delete local checkpoint/state files
    """

    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"Deleted file: {file_path}")
    else:
        print(f"File not found: {file_path}")


print("Existing namespaces:")
namespaces = catalog.list_namespaces()
print(namespaces)


for namespace in namespaces:
    schema = namespace[0]

    print(f"\nCleaning schema: {schema}")

    tables = catalog.list_tables(schema)
    for table in tables:
        table_name = f"{table[0]}.{table[1]}"
        print(f"Dropping Iceberg table: {table_name}")
        try:
            catalog.drop_table(table_name)
            print("Dropped metadata")
        except Exception as ex:
            print(f"Failed dropping {table_name}: {ex}")

    views = catalog.list_views(schema)
    for view in views:
        view_name = f"{view[0]}.{view[1]}"
        print(f"Dropping Iceberg view: {view_name}")
        try:
            catalog.drop_view(view_name)
            print("Dropped metadata")
        except Exception as ex:
            print(f"Failed dropping {view_name}: {ex}")

    print(f"Dropping Iceberg namespace: {schema}")
    try:
        catalog.drop_namespace(schema)
        print(f"Dropped namespace {schema}")
    except Exception as ex:
        print(f"Failed dropping {schema}: {ex}")

    # Remove schema folder from MinIO
    delete_s3_prefix(bucket_name, f"{schema}/")

delete_s3_prefix(bucket_name, "bronze/")

delete_local_file("/opt/airflow/checkpoint/generator_checkpoint.json")

print("\nCleanup completed")
