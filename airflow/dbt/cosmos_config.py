from pathlib import Path

from cosmos.config import ProfileConfig, ProjectConfig

DBT_CONFIG = ProfileConfig(
    profile_name="iceberg_datalake",
    target_name="dev",
    profiles_yml_filepath=Path("/opt/airflow/dbt/profiles/profiles.yml"),
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path="/opt/airflow/dbt/iceberg_datalake/",
)
