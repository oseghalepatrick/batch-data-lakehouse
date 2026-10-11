from src.processors.base_processor import BaseProcessor
from src.utils.logger import get_logger

logger = get_logger(__name__)


class StoresProcessor(BaseProcessor):

    def process(self, source_path: str):

        if not self.storage.source_exists(source_path):
            logger.info(f"No parquet files found at {source_path}. Skipping.")
            return

        data = self.read_parquet(source_path)
        self.append(
            identifier="silver.dim_stores",
            arrow_table=data,
        )
        self.storage.delete_source(source_path)
        logger.info("Stores processing completed")


if __name__ == "__main__":

    StoresProcessor().process("s3a://warehouse/bronze/dimensions/stores/stores.parquet")
