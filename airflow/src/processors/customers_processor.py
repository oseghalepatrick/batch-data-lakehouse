from src.processors.base_processor import BaseProcessor
from src.utils.logger import get_logger

logger = get_logger(__name__)


class CustomerProcessor(BaseProcessor):

    def process(self, source_path: str):

        if not self.storage.source_exists(source_path):
            logger.info(f"No parquet files found at {source_path}. Skipping.")
            return

        logger.info("Starting customer SCD Type II processing")

        # Read bronze parquet
        customers = self.read_parquet(source_path)

        logger.info(f"Customer records received: {customers.num_rows}")

        self.scd_type2_merge(
            identifier="silver.customers",
            source=customers,
            primary_key="customer_id",
            tracked_columns=["city", "email", "phone"],
        )

        self.storage.delete_source(source_path)

        logger.info("Customer processing completed")


if __name__ == "__main__":

    CustomerProcessor().process("s3a://warehouse/bronze/dimensions/customers/customers.parquet")
