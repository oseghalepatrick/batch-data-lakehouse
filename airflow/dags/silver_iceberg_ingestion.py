from datetime import datetime, timedelta, timezone

from airflow.sdk import chain, dag, task
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import ExecutionConfig, RenderConfig
from cosmos.constants import (
    # ExecutionMode,
    LoadMode,
    TestIndirectSelection,
)
from dbt.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG


@dag(
    dag_id="datalake_pipeline",
    start_date=datetime(2026, 10, 5).astimezone(timezone.utc),
    schedule="@daily",
    catchup=True,
    # schedule=None,
    # catchup=False,
    tags=["iceberg", "trino", "dbt", "minio", "pyiceberg", "soda", "data-quality"],
    max_active_runs=1,
)
def datalake_pipeline():

    @task.external_python(
        python="/opt/airflow/soda_venv/bin/python",
        expect_airflow=False,
        expect_pendulum=False,
    )
    def run_quality_check(layer="bronze", table="customers", table_type=None, scan_time=None):
        from data_quality.checks import check


        print(
            f"External task arguments: "
            f"layer={layer}, "
            f"table={table}, "
            f"table_type={table_type}, "
            f"scan_time={scan_time}"
        )

        return check(layer=layer, table=table, table_type=table_type, scan_time=scan_time)

    @task()
    def produce_data():

        from airflow.sdk import get_current_context
        from src.sales_data_generator import SalesDataGenerator

        context = get_current_context()
        generator = SalesDataGenerator(**context)
        generator.run()

    @task()
    def process_customers():

        from src.processors.customers_processor import CustomerProcessor

        CustomerProcessor().process(
            "s3a://warehouse/bronze/dimensions/customers/customers.parquet"
        )

    @task()
    def process_products():

        from src.processors.products_processor import ProductProcessor

        ProductProcessor().process(
            "s3a://warehouse/bronze/dimensions/products/products.parquet"
        )

    @task()
    def process_stores():

        from src.processors.stores_processor import StoresProcessor

        StoresProcessor().process(
            "s3a://warehouse/bronze/dimensions/stores/stores.parquet"
        )

    @task()
    def process_sales():

        from airflow.sdk import get_current_context
        from src.processors.sales_processor import SalesProcessor

        context = get_current_context()
        SalesProcessor(**context).process("s3a://warehouse/bronze/sales/sales.parquet")

    data_marts = DbtTaskGroup(
        group_id="data_marts",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(load_method=LoadMode.DBT_LS, select=["path:models"]),
        execution_config=ExecutionConfig(
            test_indirect_selection=TestIndirectSelection.CAUTIOUS,
        ),
        default_args={
            "retries": 1,
            "retry_delay": timedelta(seconds=30),
        },
    )

    bronze_customers_dq = run_quality_check.override(
        task_id="bronze_customers_quality"
    )(
        layer="bronze",
        table="customers",
        table_type="dimension",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    bronze_products_dq = run_quality_check.override(task_id="bronze_products_quality")(
        layer="bronze",
        table="products",
        table_type="dimension",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    bronze_stores_dq = run_quality_check.override(task_id="bronze_stores_quality")(
        layer="bronze",
        table="stores",
        table_type="dimension",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    bronze_sales_dq = run_quality_check.override(task_id="bronze_sales_quality")(
        layer="bronze",
        table="sales",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    silver_customers_dq = run_quality_check.override(
        task_id="silver_customers_quality"
    )(
        layer="silver",
        table="dim_customers",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    silver_products_dq = run_quality_check.override(task_id="silver_products_quality")(
        layer="silver",
        table="dim_products",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    silver_stores_dq = run_quality_check.override(task_id="silver_stores_quality")(
        layer="silver",
        table="dim_stores",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    silver_sales_dq = run_quality_check.override(task_id="silver_sales_quality")(
        layer="silver",
        table="cleaned_sales",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    gold_fact_sales_dq = run_quality_check.override(task_id="gold_fact_sales_quality")(
        layer="gold",
        table="fact_sales",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    gold_product_perf_dq = run_quality_check.override(
        task_id="gold_product_performance_quality"
    )(
        layer="gold",
        table="agg_product_performance",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    gold_daily_sales_dq = run_quality_check.override(
        task_id="gold_daily_sales_quality"
    )(
        layer="gold",
        table="agg_daily_sales",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    gold_customer_ltv_dq = run_quality_check.override(
        task_id="gold_customer_lifetime_value_quality"
    )(
        layer="gold",
        table="agg_customer_lifetime_value",
        scan_time="{{ data_interval_end.isoformat() }}",
    )

    chain(
        produce_data(),
        [
            bronze_customers_dq,
            bronze_products_dq,
            bronze_stores_dq,
            bronze_sales_dq,
        ],
        [
            process_customers(),
            process_products(),
            process_stores(),
            process_sales(),
        ],
        [
            silver_customers_dq,
            silver_products_dq,
            silver_stores_dq,
            silver_sales_dq,
        ],
        data_marts,
        [
            gold_fact_sales_dq,
            gold_daily_sales_dq,
            gold_customer_ltv_dq,
            gold_product_perf_dq,
        ],
    )


datalake_pipeline()
