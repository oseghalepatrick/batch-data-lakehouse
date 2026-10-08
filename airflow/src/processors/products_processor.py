from src.processors.base_processor import BaseProcessor
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ProductProcessor(BaseProcessor):

    def __init__(self):
        super().__init__()

    def process(self, source_path: str):

        if not self.storage.source_exists(source_path):
            logger.info(f"No parquet files found at {source_path}. Skipping.")
            return

        logger.info("Starting product SCD Type II processing")

        # Read bronze parquet
        products = self.read_parquet(source_path)

        logger.info(f"Product records received: {products.num_rows}")

        self.scd_type2_merge(
            identifier="silver.products",
            source=products,
            primary_key="product_id",
            tracked_columns=["price"],
        )

        self.storage.delete_source(source_path)

        logger.info("Product processing completed")


if __name__ == "__main__":

    ProductProcessor().process(
        "s3a://warehouse/bronze/dimensions/products/products.parquet"
    )
