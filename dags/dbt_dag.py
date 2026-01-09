from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, LoadMode
from cosmos.constants import ExecutionMode
from datetime import datetime

DBT_PROJECT_PATH = "/usr/local/airflow/dbt/data_pipeline"
DBT_PROFILES_PATH = "/usr/local/airflow/dbt/data_pipeline/profiles.yml"

@dag(
    dag_id="dbt_snowflake_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def dbt_pipeline():

    dbt_tg = DbtTaskGroup(
        group_id="dbt_tg",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH
        ),
        profile_config=ProfileConfig(
            profiles_yml_filepath=DBT_PROFILES_PATH,
            profile_name="data_pipeline",
            target_name="dev"
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS   # <-- key fix to avoid parse-time dbt run
        )
    )

dbt_pipeline()
