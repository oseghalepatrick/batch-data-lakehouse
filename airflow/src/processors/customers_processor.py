"""Process Bronze customer data into the Silver SCD Type II dimension."""

from src.processors.base_processor import BaseProcessor
from src.utils.logger import get_logger

logger = get_logger(__name__)


class CustomerProcessor(BaseProcessor):
    """Transform customer data and maintain the Silver customer dimension."""

    def process(self, source_path: str):
        """Process Bronze customer records into an SCD Type II Iceberg table."""
        # Skip processing when no Bronze customer file is available.
        if not self.storage.source_exists(source_path):
            logger.info(f"No parquet files found at {source_path}. Skipping.")
            return

        logger.info("Starting customer SCD Type II processing")

        # Read and normalise the incoming Bronze customer data.
        customers = self.read_parquet(source_path)
        logger.info(f"Customer records received: {customers.num_rows}")

        # Track selected customer attributes and preserve historical changes.
        self.scd_type2_merge(
            identifier="silver.dim_customers",
            source=customers,
            primary_key="customer_id",
            tracked_columns=["city", "email", "phone"],
        )

        # Remove the processed Bronze file after a successful merge.
        self.storage.delete_source(source_path)

        logger.info("Customer processing completed")


if __name__ == "__main__":
    CustomerProcessor().process(
        "s3a://warehouse/bronze/dimensions/customers/customers.parquet"
    )
