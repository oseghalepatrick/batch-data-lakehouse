import hashlib
import json
import random
import time
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NoSuchTableError,
    TableAlreadyExistsError,
    ValidationException,
)
from pyiceberg.types import StringType
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SodaQualityTransformer:
    VALID_DATA_LEVELS = {"bronze", "silver", "gold"}

    def __init__(self, columns, values, data_level, scan_start_ts=None, scan_end_ts=None):
        self.columns = [column.strip('"') for column in columns]
        self.values = values
        self.data_level = data_level.lower().strip()
        self.record = dict(zip(self.columns, self.values))
        self.scan_start_ts = scan_start_ts
        self.scan_end_ts = scan_end_ts

        self._validate_data_level()

        self.scan_id = self._generate_scan_id()

    def _validate_data_level(self):
        if self.data_level not in self.VALID_DATA_LEVELS:
            raise ValueError(
                f"Invalid data_level '{self.data_level}'. "
                f"Expected one of: {sorted(self.VALID_DATA_LEVELS)}"
            )

    @staticmethod
    def _safe_json_load(value):
        if value is None:
            return {}

        if isinstance(value, dict):
            return value

        if isinstance(value, str):
            value = value.strip()

            if not value:
                return {}

            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return {}

        return {}

    def _generate_scan_id(self):
        if self.scan_start_ts:
            ts = str(self.scan_start_ts)
        else:
            ts = str(self.record.get("scan_start_ts", ""))
    
        key = "|".join(
            [
                self.data_level,
                str(self.record.get("definition_name", "")),
                str(self.record.get("table_name", "")),
                str(self.record.get("data_timestamp", "")),
                # str(self.record.get("scan_start_ts", "")),
                ts
            ]
        )

        return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]

    def _get_base_record(self):

        if self.scan_start_ts:
            start_ts = str(self.scan_start_ts)
            end_ts = str(self.scan_end_ts)
        else:
            start_ts = str(self.record.get("scan_start_ts", ""))
            end_ts = str(self.record.get("scan_end_ts", ""))
        return {
            "scan_id": self.scan_id,
            "data_level": self.data_level,
            "definition_name": self.record.get("definition_name"),
            "data_timestamp": self.record.get("data_timestamp"),
            # "scan_start_ts": self.record.get("scan_start_ts"),
            # "scan_end_ts": self.record.get("scan_end_ts"),
            "scan_start_ts": start_ts,
            "scan_end_ts": end_ts,
            "has_errors": self.record.get("has_errors"),
            "has_warnings": self.record.get("has_warnings"),
            "has_failures": self.record.get("has_failures"),
            "table_name": self.record.get("table_name"),
            "row_count": self.record.get("row_count"),
            "source_data_inserted_at_utc": datetime.now(timezone.utc),
        }

    def _transform_standard_metrics(self):
        base = self._get_base_record()
        rows = []

        check_types = [
            "duplicate_count",
            "missing_count",
            "missing_percent",
            "invalid_count",
            "invalid_percent",
            "min",
            "max",
            "avg",
        ]

        for check_type in check_types:
            metrics = self._safe_json_load(self.record.get(check_type))

            for column_name, result in metrics.items():
                if not isinstance(result, dict):
                    continue

                row = {
                    **base,
                    "check_type": check_type,
                    "column_name": column_name,
                    "status": result.get("status"),
                    "check_value": result.get("check_value"),
                    "missing_count": None,
                    "invalid_count": None,
                    "max_column_timestamp": None,
                    "freshness": None,
                    "threshold": None,
                    "expected_type": None,
                    "actual_type": None,
                }

                if check_type == "missing_count":
                    row["missing_count"] = result.get("check_value")

                elif check_type == "missing_percent":
                    row["missing_count"] = result.get("missing_count")

                elif check_type == "invalid_count":
                    row["invalid_count"] = result.get("check_value")

                elif check_type == "invalid_percent":
                    row["invalid_count"] = result.get("invalid_count")

                rows.append(row)

        return rows

    def _transform_freshness(self):
        base = self._get_base_record()
        rows = []

        metrics = self._safe_json_load(self.record.get("freshness"))

        for column_name, result in metrics.items():
            if not isinstance(result, dict):
                continue

            rows.append(
                {
                    **base,
                    "check_type": "freshness",
                    "column_name": column_name,
                    "status": result.get("status"),
                    "check_value": None,
                    "missing_count": None,
                    "invalid_count": None,
                    "max_column_timestamp": result.get("max_column_timestamp"),
                    "freshness": result.get("freshness"),
                    "threshold": result.get("threshold"),
                    "expected_type": None,
                    "actual_type": None,
                }
            )

        return rows

    def _transform_schema(self):
        base = self._get_base_record()
        rows = []

        metrics = self._safe_json_load(self.record.get("schema"))

        for column_name, result in metrics.items():
            if not isinstance(result, dict):
                continue

            rows.append(
                {
                    **base,
                    "check_type": "schema",
                    "column_name": column_name,
                    "status": result.get("status"),
                    "check_value": None,
                    "missing_count": None,
                    "invalid_count": None,
                    "max_column_timestamp": None,
                    "freshness": None,
                    "threshold": None,
                    "expected_type": result.get("expected_type"),
                    "actual_type": result.get("actual_type"),
                }
            )

        return rows

    def transform(self):
        rows = []

        rows.extend(self._transform_standard_metrics())
        rows.extend(self._transform_freshness())
        rows.extend(self._transform_schema())

        df = pd.DataFrame(rows)

        if df.empty:
            return df

        df.insert(
            0,
            "primary_id",
            (
                df["scan_id"].astype(str)
                + "-"
                + df["check_type"].astype(str)
                + "-"
                + df["column_name"].fillna("").astype(str)
            ),
        )

        return df

    @staticmethod
    def prepare_dataframe_for_iceberg(df):
        df = df.copy()

        timestamp_columns = [
            "data_timestamp",
            "scan_start_ts",
            "scan_end_ts",
            "source_data_inserted_at_utc",
            "max_column_timestamp",
        ]

        for column in timestamp_columns:
            if column in df.columns:
                df[column] = pd.to_datetime(
                    df[column],
                    utc=True,
                    errors="coerce",
                )

        integer_columns = ["row_count", "missing_count", "invalid_count"]

        for column in integer_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(
                    df[column],
                    errors="coerce",
                ).astype("Int64")

        if "check_value" in df.columns:
            df["check_value"] = pd.to_numeric(
                df["check_value"],
                errors="coerce",
            )

        boolean_columns = [
            "has_errors",
            "has_warnings",
            "has_failures",
        ]

        for column in boolean_columns:
            if column in df.columns:
                df[column] = df[column].astype("boolean")

        return df

    @staticmethod
    def get_arrow_schema():
        return pa.schema(
            [
                pa.field("primary_id", pa.string(), nullable=False),
                pa.field("scan_id", pa.string(), nullable=False),
                pa.field("data_level", pa.string(), nullable=False),
                pa.field("definition_name", pa.string()),
                pa.field("data_timestamp", pa.timestamp("us", tz="UTC")),
                pa.field("scan_start_ts", pa.timestamp("us", tz="UTC")),
                pa.field("scan_end_ts", pa.timestamp("us", tz="UTC")),
                pa.field("has_errors", pa.bool_()),
                pa.field("has_warnings", pa.bool_()),
                pa.field("has_failures", pa.bool_()),
                pa.field("table_name", pa.string()),
                pa.field("row_count", pa.int64()),
                pa.field("source_data_inserted_at_utc", pa.timestamp("us", tz="UTC")),
                pa.field("check_type", pa.string()),
                pa.field("column_name", pa.string()),
                pa.field("status", pa.string()),
                pa.field("check_value", pa.float64()),
                pa.field("missing_count", pa.int64()),
                pa.field("invalid_count", pa.int64()),
                pa.field("max_column_timestamp", pa.timestamp("us", tz="UTC")),
                pa.field("freshness", pa.string()),
                pa.field("threshold", pa.string()),
                pa.field("expected_type", pa.string()),
                pa.field("actual_type", pa.string()),
            ]
        )

    @staticmethod
    def evolve_table_schema(table):
        field_names = {field.name for field in table.schema().fields}

        if "data_level" not in field_names:
            with table.update_schema() as update:
                update.add_column("data_level", StringType(), required=False)

            table.refresh()

    @staticmethod
    def table_exists(catalog, identifier: str) -> bool:
        """Return True when an Iceberg table exists."""

        try:
            catalog.load_table(identifier)
            return True
        except NoSuchTableError:
            return False

    @classmethod
    def write_to_iceberg(
        cls,
        df,
        catalog,
        table_identifier,
        create_if_missing=True,
        max_retries=6,
    ):
        if df.empty:
            logger.info("No data quality records to write.")
            return

        df = cls.prepare_dataframe_for_iceberg(df)

        arrow_schema = cls.get_arrow_schema()

        try:
            table = catalog.load_table(table_identifier)

            cls.evolve_table_schema(table)

        except Exception:
            if not create_if_missing:
                raise

            namespace = table_identifier.rsplit(".", 1)[0]

            try:
                catalog.create_namespace(namespace)
            except NamespaceAlreadyExistsError:
                logger.debug(f"Namespace already exists: {namespace}")
            except Exception:
                logger.exception(f"Unexpected error creating namespace: {namespace}")
                raise

            if not cls.table_exists(catalog, table_identifier):
                try:
                    table = catalog.create_table(
                        identifier=table_identifier,
                        schema=arrow_schema,
                    )
                except TableAlreadyExistsError:
                    table = catalog.load_table(table_identifier)
            else:
                table = catalog.load_table(table_identifier)

        arrow_table = pa.Table.from_pandas(
            df,
            schema=arrow_schema,
            preserve_index=False,
            safe=True,
        )

        for attempt in range(1, max_retries + 1):
            try:
                # Always reload the latest table metadata/snapshot
                # before each commit attempt.
                table = catalog.load_table(table_identifier)

                table.append(arrow_table)

                logger.info(
                    f"Successfully wrote {arrow_table.num_rows} rows "
                    f"to {table_identifier}"
                )

                return

            except (CommitFailedException, ValidationException) as exc:

                if attempt == max_retries:
                    logger.info(
                        f"Failed to write to {table_identifier} "
                        f"after {max_retries} attempts."
                    )
                    raise

                delay = min(0.25 * (2 ** (attempt - 1)), 5.0)

                delay += random.uniform(0, 0.25)

                logger.info(
                    f"Concurrent Iceberg commit detected for {table_identifier}. "
                    f"Retrying {attempt}/{max_retries} in {delay:.2f}s. "
                    f"Error: {exc}"
                )
                time.sleep(delay)
