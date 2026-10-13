"""Clean and load Bronze sales data into the Silver Iceberg layer."""

from datetime import UTC, datetime, timezone

import pandas as pd
import pyarrow as pa

from src.processors.base_processor import BaseProcessor
from src.utils.checkpoint import CheckpointManager
from src.utils.config import get_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SalesProcessor(BaseProcessor):
    """Validate Bronze sales data and load cleaned records into Silver."""

    def __init__(self, **context):
        """Initialise the processor and load the current product reference count."""
        super().__init__()

        config = get_config()
        cfg = config["data_generation"]

        # Use the checkpoint to validate product IDs against the current dimension size.
        checkpoint = CheckpointManager(cfg["checkpoint"]["file"])
        state = checkpoint.load()
        self.num_products = state["dimensions"]["products"]["current_count"]

        # Use the Airflow logical date when available to keep processing reproducible.
        if context:
            self.tm = context.get("logical_date").replace(tzinfo=None)
        else:
            self.tm = datetime.now(UTC).replace(tzinfo=None)

    def clean_sales_data(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        """Apply validation rules and remove invalid sales records."""
        initial_count = len(sales_df)
        logger.info(f"Initial sales records: {initial_count}")

        valid_payment_methods = [
            "credit_card",
            "debit_card",
            "cash_transfer",
            "digital_wallet",
        ]

        valid_statuses = [
            "completed",
            "pending",
            "cancelled",
        ]

        # Keep only records that satisfy the required business and quality rules.
        sales_df = sales_df[
            # Required fields
            sales_df["transaction_id"].notna()
            & sales_df["customer_id"].notna()
            & sales_df["product_id"].notna()
            & sales_df["transaction_timestamp"].notna()
            # Reject transactions dated after the current processing timestamp.
            & (sales_df["transaction_timestamp"] <= self.tm)
            # Quantity must be positive.
            & (sales_df["quantity"] > 0)
            # Payment method and status must belong to the accepted domains.
            & (sales_df["payment_method"].isin(valid_payment_methods))
            & (sales_df["status"].isin(valid_statuses))
            # Product IDs must exist within the current product dimension range.
            & (sales_df["product_id"] > 0)
            & (sales_df["product_id"] <= self.num_products)
        ].copy()

        removed_count = initial_count - len(sales_df)

        logger.info(f"Removed invalid sales records: {removed_count}")
        logger.info(f"Valid sales records remaining: {len(sales_df)}")

        return sales_df

    def process(self, source_path: str):
        """Clean Bronze sales data and append valid records to Silver."""
        # Skip processing when no Bronze sales file is available.
        if not self.storage.source_exists(source_path):
            logger.info(f"No parquet files found at {source_path}. Skipping.")
            return

        # Read the Bronze Parquet file and convert it to pandas for validation.
        data = self.read_parquet(source_path)
        data_df = data.to_pandas()
        data_df = self.clean_sales_data(data_df)

        # Record when each cleaned row was inserted into the Silver layer.
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if "inserted_at" not in data_df.columns:
            data_df["inserted_at"] = now

        # Convert the cleaned dataframe back to Arrow for the Iceberg write.
        data = pa.Table.from_pandas(
            data_df,
            preserve_index=False,
        )
        data = self.normalise_schema(data)

        # Partition Silver sales by transaction day for efficient time-based queries.
        self.append(
            identifier="silver.cleaned_sales",
            arrow_table=data,
            partitions=[
                {
                    "column": "transaction_timestamp",
                    "transform": "day",
                    "name": "transaction_day",
                }
            ],
        )

        # Remove the Bronze source only after the Silver append completes successfully.
        self.storage.delete_source(source_path)

        logger.info("Sales processing completed")


if __name__ == "__main__":
    SalesProcessor().process("s3a://warehouse/bronze/sales/sales.parquet")
