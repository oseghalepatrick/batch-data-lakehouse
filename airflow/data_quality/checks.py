"""Run Soda data quality checks and persist results to Iceberg."""

import os
from string import Template

from data_quality.process_scan_result import SodaDQProcessor
from data_quality.quality_transformer import SodaQualityTransformer
from soda.scan import Scan
from src.utils.config import (
    get_catalog_config,
    get_config,
)
from src.utils.logger import get_logger
from src.utils.storage import MinIOStorage

logger = get_logger(__name__)

base_dir = os.path.dirname(os.path.abspath(__file__))
logger.info(f"base_dir: {base_dir}")


def check(
    layer="bronze",
    table="customers",
    table_type=None,
    data_source="trino",
    scan_result_table="gold.data_quality_results",
):
    """Run Soda checks for a Bronze, Silver, or Gold dataset."""
    config = get_config()
    cfg = config["data_generation"]
    storage = MinIOStorage(cfg["minio"])

    scan = Scan()

    if layer == "bronze":
        # Bronze checks run directly against Parquet files stored in MinIO.
        if table_type == "dimension":
            source_path = f"dimensions/{table}/{table}.parquet"
        else:
            source_path = f"{table}/{table}.parquet"

        logger.info(f"Bronze source path: {source_path}")

        # Skip the scan when the expected Bronze source has not been created.
        if not storage.source_exists(source_path):
            logger.warning(
                "Skipping data quality check. "
                f"Bronze parquet file does not exist: {source_path}"
            )
            return

        df = storage.read_parquet(source_path)

        # Avoid running Soda against an empty dataframe.
        if df.empty:
            logger.warning(
                "Skipping data quality check. "
                f"Bronze parquet file is empty: {source_path}"
            )
            return

        scan.set_verbose()
        scan.add_pandas_dataframe(
            dataset_name=table,
            pandas_df=df,
            data_source_name=data_source,
        )
    else:
        # Silver and Gold checks use Trino. Substitute the target schema
        # dynamically into the shared Soda configuration.
        config_file = f"{base_dir}/configuration.yml"

        with open(config_file) as f:
            config_file_template = Template(f.read())
            config_yaml_str = config_file_template.safe_substitute(
                {"SCHEMA": layer}
            )

        scan.set_verbose()
        scan.add_configuration_yaml_str(config_yaml_str)

    scan.set_data_source_name(data_source)

    # Each layer/table combination has its own SodaCL check definition.
    template_path = f"{base_dir}/{layer}/{table}.yml"
    logger.info(f"Check definition: {template_path}")

    with open(template_path) as f:
        checks_template = Template(f.read())
        rendered_yaml_str = checks_template.safe_substitute(
            {"TABLE": table}
        )

    scan.add_sodacl_yaml_str(rendered_yaml_str)
    scan.set_scan_definition_name(layer)

    # Execute the Soda scan and capture the complete scan result payload.
    result = scan.execute()
    scan_results = scan.get_scan_results()

    logger.info(f"Logging scans:\n{scan.get_logs_text()}")

    # Convert Soda's nested scan output into tabular rows.
    writer = SodaDQProcessor()
    columns, values = writer.save_scan_results(scan_results)

    transformer = SodaQualityTransformer(
        columns,
        values,
        layer,
    )

    result_df = transformer.transform()

    # Do not create an Iceberg commit when Soda produced no result rows.
    if result_df.empty:
        logger.warning(
            "No transformed data quality results. "
            "Skipping Iceberg write."
        )
        return

    # Persist historical quality results so they can be queried by Trino
    # and displayed in the Streamlit monitoring dashboard.
    catalog = get_catalog_config()

    SodaQualityTransformer.write_to_iceberg(
        df=result_df,
        catalog=catalog,
        table_identifier=scan_result_table,
    )

    # Bronze failures are logged so the raw-data pipeline can continue,
    # while downstream Silver and Gold failures stop the task.
    if result == 2 and layer == "bronze":
        logger.error("Soda Scan failed")
    elif result == 2:
        raise ValueError("Soda Scan failed")


if __name__ == "__main__":
    check(
        layer="bronze",
        table="products",
        table_type="dimension",
    )