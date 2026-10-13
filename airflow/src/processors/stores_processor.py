"""Process Bronze store data into the Silver store dimension."""

from src.processors.base_processor import BaseProcessor
from src.utils.logger import get_logger

logger = get_logger(__name__)


class StoresProcessor(BaseProcessor):
    """Load store data from Bronze and append it to the Silver dimension."""

    def process(self, source_path: str):
        """Process Bronze store records and append them to the Silver Iceberg table."""
        # Skip processing when no Bronze store file is available.
        if not self.storage.source_exists(source_path):
            logger.info(f"No parquet files found at {source_path}. Skipping.")
            return

        # Read and normalise the incoming Bronze store data.
        data = self.read_parquet(source_path)

        logger.info(f"Store records received: {data.num_rows}")

        # Store records are appended directly because store history is not tracked as SCD Type II.
        self.append(
            identifier="silver.dim_stores",
            arrow_table=data,
        )

        # Remove the Bronze source after a successful append.
        self.storage.delete_source(source_path)

        logger.info("Stores processing completed")


if __name__ == "__main__":
    StoresProcessor().process("s3a://warehouse/bronze/dimensions/stores/stores.parquet")
