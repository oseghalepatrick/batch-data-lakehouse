"""Process Bronze product data into the Silver SCD Type II dimension."""

from src.processors.base_processor import BaseProcessor
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ProductProcessor(BaseProcessor):
    """Transform product data and maintain the Silver product dimension."""

    def __init__(self):
        """Initialise the product processor and shared Iceberg utilities."""
        super().__init__()

    def process(self, source_path: str):
        """Process Bronze product records into an SCD Type II Iceberg table."""
        # Skip processing when no Bronze product file is available.
        if not self.storage.source_exists(source_path):
            logger.info(f"No parquet files found at {source_path}. Skipping.")
            return

        logger.info("Starting product SCD Type II processing")

        # Read and normalise the incoming Bronze product data.
        products = self.read_parquet(source_path)
        logger.info(f"Product records received: {products.num_rows}")

        # Track price changes while preserving historical product versions.
        self.scd_type2_merge(
            identifier="silver.dim_products",
            source=products,
            primary_key="product_id",
            tracked_columns=["price"],
        )

        # Remove the processed Bronze file after a successful merge.
        self.storage.delete_source(source_path)

        logger.info("Product processing completed")


if __name__ == "__main__":
    ProductProcessor().process(
        "s3a://warehouse/bronze/dimensions/products/products.parquet"
    )
