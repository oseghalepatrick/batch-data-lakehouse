import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchTableError,
)
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import NestedField, Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    IdentityTransform,
    MonthTransform,
    YearTransform,
)
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)

from src.utils.config import (
    get_catalog_config,
    get_config,
    get_s3fs_config,
)
from src.utils.logger import get_logger
from src.utils.storage import MinIOStorage

logger = get_logger(__name__)


class BaseProcessor:

    def __init__(self):
        self.catalog = get_catalog_config()
        self.s3 = get_s3fs_config()
        cfg = get_config()["data_generation"]
        self.storage = MinIOStorage(cfg["minio"])

    def normalise_schema(self, table: pa.Table):

        integer_columns = [
            "transaction_id",
            "customer_id",
            "product_id",
            "store_id",
        ]
        for column in integer_columns:
            if column in table.column_names:
                index = table.schema.get_field_index(column)
                table = table.set_column(index, column, table[column].cast(pa.int64()))

        return table

    def read_parquet(self, path):

        logger.info(f"Reading parquet {path}")
        path = path.replace("s3a://", "")
        table = pq.read_table(path, filesystem=self.s3)
        table = self.normalise_schema(table)
        logger.info(f"file schema: \n{table.schema}\n")
        logger.info(f"Records: {table.num_rows}")

        return table

    def create_namespace(self, namespace: str) -> None:
        """Create namespace when it does not already exist."""

        try:
            self.catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            logger.debug(f"Namespace already exists: {namespace}")
        except Exception:
            logger.exception(f"Unexpected error creating namespace: {namespace}")
            raise

    def table_exists(self, identifier: str) -> bool:
        """Return True when an Iceberg table exists."""

        try:
            self.catalog.load_table(identifier)
            return True
        except NoSuchTableError:
            return False

    def load_table(self, identifier: str):
        """Load an existing Iceberg table."""

        return self.catalog.load_table(identifier)

    @staticmethod
    def build_partition_spec(
        schema: Schema, partitions: list[dict] | None = None
    ) -> PartitionSpec:
        """
        Build an Iceberg partition specification.

        Example:
            partitions=[
                {
                    "column": "transaction_timestamp",
                    "transform": "day",
                    "name": "transaction_day",
                }
            ]

        Supported transforms:
            identity
            day
            month
            year
            bucket
        """

        if not partitions:
            return PartitionSpec()

        partition_fields = []

        # Partition field IDs must not collide with schema field IDs.
        next_partition_id = 1000

        for partition in partitions:
            column = partition["column"]
            source_field = schema.find_field(column)
            transform_name = partition.get("transform", "identity").lower()

            if transform_name == "identity":
                transform = IdentityTransform()
            elif transform_name == "day":
                transform = DayTransform()
            elif transform_name == "month":
                transform = MonthTransform()
            elif transform_name == "year":
                transform = YearTransform()
            elif transform_name == "bucket":
                buckets = partition.get("buckets")
                if not buckets:
                    raise ValueError(
                        f"'buckets' is required for bucket partition on {column}"
                    )
                transform = BucketTransform(buckets)
            else:
                raise ValueError(f"Unsupported partition transform: {transform_name}")

            partition_name = partition.get("name", f"{column}_{transform_name}")
            partition_fields.append(
                PartitionField(
                    source_id=source_field.field_id,
                    field_id=next_partition_id,
                    transform=transform,
                    name=partition_name,
                )
            )
            next_partition_id += 1

        return PartitionSpec(*partition_fields)

    def arrow_to_iceberg_schema(self, arrow_schema):

        fields = []
        for idx, field in enumerate(arrow_schema, start=1):
            if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                iceberg_type = StringType()
            elif pa.types.is_int32(field.type):
                iceberg_type = IntegerType()
            elif pa.types.is_int64(field.type):
                iceberg_type = LongType()
            elif pa.types.is_float32(field.type):
                iceberg_type = FloatType()
            elif pa.types.is_float64(field.type):
                iceberg_type = DoubleType()
            elif pa.types.is_date(field.type):
                iceberg_type = DateType()
            elif pa.types.is_timestamp(field.type):
                if field.type.tz:
                    iceberg_type = TimestamptzType()
                else:
                    iceberg_type = TimestampType()
            elif pa.types.is_boolean(field.type):
                iceberg_type = BooleanType()
            else:
                raise ValueError(f"Unsupported type: {field.type}")

            fields.append(
                NestedField(
                    field_id=idx,
                    name=field.name,
                    field_type=iceberg_type,
                    required=False,
                )
            )

        schema = Schema(*fields)
        logger.info(f"Table schema:\n{schema}")
        return schema

    def create_table(
        self,
        identifier: str,
        arrow_table: pa.Table,
        partitions: list[dict] | None = None,
        properties: dict | None = None,
    ):
        """
        Create an Iceberg v2 table.
        Partitioning is optional.
        """

        namespace = identifier.rsplit(".", 1)[0]
        self.create_namespace(namespace)
        schema = self.arrow_to_iceberg_schema(arrow_table.schema)
        partition_spec = self.build_partition_spec(schema, partitions)
        table_properties = {"format-version": "2"}
        if properties:
            table_properties.update(properties)

        logger.info(f"Creating Iceberg table: {identifier}")
        table = self.catalog.create_table(
            identifier,
            schema=schema,
            partition_spec=partition_spec,
            properties=table_properties,
        )
        logger.info(f"Created Iceberg table: {identifier}")
        logger.info(f"Schema:\n{table.schema()}")
        logger.info(f"Partition Spec:\n{table.spec()}")
        logger.info(f"Properties:\n{table.properties}")
        return table

    def append(
        self,
        identifier: str,
        arrow_table: pa.Table,
        partitions: list[dict] | None = None,
    ):
        """
        Append data.
        Creates the table automatically when it does not exist.
        """

        if arrow_table.num_rows == 0:
            logger.info(f"No records to append to {identifier}")
            return

        if not self.table_exists(identifier):
            table = self.create_table(
                identifier=identifier,
                arrow_table=arrow_table,
                partitions=partitions,
            )
        else:
            table = self.load_table(identifier)
        logger.info(f"Appending {arrow_table.num_rows} records to {identifier}")

        table.append(arrow_table)
        logger.info(f"Append completed: {identifier}")

    def _to_scd_arrow(self, df: pd.DataFrame) -> pa.Table:
        """
        Convert an SCD DataFrame to PyArrow.

        effective_start and effective_end are DATE columns.
        NULL effective_end values remain Arrow NULLs.
        """

        df = df.copy()

        date_columns = [
            column
            for column in ["effective_start", "effective_end"]
            if column in df.columns
        ]

        # Vectorised conversion to Python date values
        for column in date_columns:
            df[column] = pd.to_datetime(
                df[column],
                errors="coerce",
            ).dt.date

        # Convert the remaining DataFrame columns
        arrow_table = pa.Table.from_pandas(
            df,
            preserve_index=False,
        )

        # Explicitly enforce DATE type.
        # This is important when effective_end contains only NULL values.
        for column in date_columns:
            column_index = arrow_table.schema.get_field_index(column)

            date_array = pa.array(
                df[column],
                type=pa.date32(),
            )

            arrow_table = arrow_table.set_column(
                column_index,
                column,
                date_array,
            )

        return arrow_table

    def scd_type2_merge(
        self,
        identifier: str,
        source: pa.Table,
        primary_key: str,
        tracked_columns: list[str],
    ):
        """
        Perform incremental SCD Type II merge.

        Logic:
        - New keys:
            Insert as current records.

        - Changed keys:
            1. Delete existing current record only.
            2. Append expired version.
            3. Append new current version.

        - Unchanged keys:
            Ignore.

        Historical records are preserved.

        Required columns:
            effective_start
            effective_end
            is_current
        """

        # Prepare incoming data
        source_df = source.to_pandas()
        source_df["effective_end"] = None
        source_df["is_current"] = True
        logger.info(f"Incoming records: {len(source_df)}")

        # Initial load
        if not self.table_exists(identifier):
            logger.info(f"Creating initial SCD table {identifier}")

            arrow_append = self._to_scd_arrow(source_df)
            logger.info(f"Append schema:\n{arrow_append.schema}")
            table = self.append(identifier, arrow_append)
            logger.info("Initial SCD load completed")

            return

        # Load existing records only for incoming keys
        table = self.load_table(identifier)
        keys = source_df[primary_key].dropna().unique().tolist()
        if not keys:
            logger.info("No keys received")
            return

        key_values = ",".join(str(x) for x in keys)
        existing_df = (
            table.scan(row_filter=(f"{primary_key} IN ({key_values})"))
            .to_arrow()
            .to_pandas()
        )

        if existing_df.empty:
            logger.info("No matching records found, inserting new records")

            arrow_append = self._to_scd_arrow(source_df)
            logger.info(f"Append schema:\n{arrow_append.schema}")
            self.append(identifier, arrow_append)
            return

        current_df = existing_df[existing_df["is_current"].eq(True)].copy()
        logger.info(f"Current matching records: {len(current_df)}")

        # Identify new records
        new_records = source_df[
            ~source_df[primary_key].isin(current_df[primary_key])
        ].copy()
        logger.info(f"New records: {len(new_records)}")

        # Identify changed records
        joined = source_df.merge(
            current_df, on=primary_key, suffixes=("_new", "_old"), how="inner"
        )

        # Compare tracked columns only
        change_mask = pd.Series(False, index=joined.index)

        for col in tracked_columns:

            new_col = f"{col}_new"
            old_col = f"{col}_old"

            column_changed = joined[new_col].ne(joined[old_col]) & ~(
                joined[new_col].isna() & joined[old_col].isna()
            )
            change_mask |= column_changed

        changed_records = joined[change_mask].copy()
        changed_keys = changed_records[primary_key].tolist()

        # Map primary key -> new effective_start
        expired_date = changed_records.set_index(primary_key)[
            "effective_start_new"
        ].to_dict()
        logger.info(f"Changed records: {len(changed_keys)}")

        # Nothing changed
        if not changed_keys and new_records.empty:
            logger.info("No SCD changes detected")
            return

        # Prepare expired versions
        expired_records = current_df[current_df[primary_key].isin(changed_keys)].copy()
        expired_records["effective_end"] = expired_records[primary_key].map(
            expired_date
        )
        expired_records["is_current"] = False

        # Prepare new versions
        changed_new_records = source_df[
            source_df[primary_key].isin(changed_keys)
        ].copy()
        inserts = pd.concat([new_records, changed_new_records], ignore_index=True)

        inserts["effective_end"] = None
        inserts["is_current"] = True

        # Delete only active records
        if changed_keys:
            delete_values = ",".join(str(x) for x in changed_keys)
            delete_filter = f"{primary_key} IN ({delete_values}) AND is_current = true"
            logger.info(f"Deleting current records: {delete_filter}")
            table.delete(delete_filter=delete_filter)

        # Append expired + new versions
        append_df = pd.concat([expired_records, inserts], ignore_index=True)
        if not append_df.empty:
            logger.info(f"Appending SCD records: {len(append_df)}")
            arrow_append = self._to_scd_arrow(append_df)
            logger.info(f"Append schema:\n{arrow_append.schema}")
            self.append(identifier, arrow_append)
        logger.info(f"SCD Type II merge completed: {identifier}")

    def overwrite(
        self,
        identifier: str,
        arrow_table: pa.Table,
    ):
        """Overwrite an existing Iceberg table."""

        table = self.load_table(identifier)

        logger.info(
            "Overwriting %s with %s records",
            identifier,
            arrow_table.num_rows,
        )

        table.overwrite(arrow_table)

        logger.info(f"Overwrite completed: {identifier}")
