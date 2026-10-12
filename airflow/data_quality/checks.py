import os
import random
from datetime import datetime, timedelta, timezone
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
    scan_time=None,
    scan_result_table="gold.data_quality_results",
):
    config = get_config()
    cfg = config["data_generation"]
    storage = MinIOStorage(cfg["minio"])

    if scan_time:
        logical_time = datetime.fromisoformat(scan_time.replace("Z", "+00:00"))

        current_time = datetime.now(timezone.utc)

        scan_start_ts = logical_time.replace(
            hour=current_time.hour,
            minute=current_time.minute,
            second=current_time.second,
            microsecond=current_time.microsecond,
        )
    else:
        scan_start_ts = datetime.now(timezone.utc)

    scan_end_ts = scan_start_ts + timedelta(seconds=random.randint(0, 60))

    logger.info(
        "Logical scan start: %s",
        scan_start_ts.isoformat(),
    )

    logger.info(
        "Logical scan end: %s",
        scan_end_ts.isoformat(),
    )

    scan = Scan()

    if layer == "bronze":
        if table_type == "dimension":
            source_path = f"dimensions/{table}/{table}.parquet"
        else:
            source_path = f"{table}/{table}.parquet"
        logger.info(f"Bronze source path: {source_path}")

        if not storage.source_exists(source_path):
            logger.warning(
                "Skipping data quality check. "
                f"Bronze parquet file does not exist: {source_path}"
            )
            return

        df = storage.read_parquet(source_path)
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
        config_file = f"{base_dir}/configuration.yml"
        with open(config_file) as f:
            config_file_template = Template(f.read())
            config_yaml_str = config_file_template.safe_substitute({"SCHEMA": layer})

        scan.set_verbose()
        scan.add_configuration_yaml_str(config_yaml_str)

    scan.set_data_source_name(data_source)
    template_path = f"{base_dir}/{layer}/{table}.yml"
    logger.info(f"Check definition: {template_path}")

    with open(template_path) as f:
        checks_template = Template(f.read())
        rendered_yaml_str = checks_template.safe_substitute({"TABLE": table})

    scan.add_sodacl_yaml_str(rendered_yaml_str)
    scan.set_scan_definition_name(layer)
    if scan_time:
        logger.info(f"Using logical scan time: {scan_time}")

        # print
        # Logical timestamp stored in Soda results
        scan._data_timestamp = datetime.fromisoformat(scan_time.replace("Z", "+00:00"))
        scan.add_variables(
            {
                "SCAN_TIME": scan_start_ts,
            }
        )
    result = scan.execute()

    scan_results = scan.get_scan_results()
    logger.info(f"Logging scans:\n{scan.get_logs_text()}")

    writer = SodaDQProcessor()
    columns, values = writer.save_scan_results(scan_results)
    if scan_time:
        scan_ts = scan_start_ts
        scan_end = scan_end_ts
        transformer = SodaQualityTransformer(
            columns,
            values,
            layer,
            scan_ts,
            scan_end,
        )
    else:
        transformer = SodaQualityTransformer(
            columns,
            values,
            layer,
        )

    result_df = transformer.transform()
    if result_df.empty:
        logger.warning(
            "No transformed data quality results. " "Skipping Iceberg write."
        )
        return

    catalog = get_catalog_config()
    SodaQualityTransformer.write_to_iceberg(
        df=result_df,
        catalog=catalog,
        table_identifier=scan_result_table,
    )

    if result == 2 and layer == "bronze":
        logger.error("Soda Scan failed")
    elif result == 2:
        raise ValueError("Soda Scan failed")


if __name__ == "__main__":
    check(
        layer="bronze",
        table="products",
        table_type="dimension",
        scan_time="2026-10-05T00:00:00+00:00",
    )
