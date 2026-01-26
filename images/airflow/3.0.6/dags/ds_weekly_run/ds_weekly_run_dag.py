import datetime

from pendulum import duration
from cosmos import DbtTaskGroup, RenderConfig, LoadMode 
from airflow.sdk import dag

from common.slack_notifications import bad_boy, good_boy
from common.dbt_cosmos_config import DBT_PROJECT_CONFIG, DBT_EXECUTION_CONFIG, PROD_DBT_PROFILE_CONFIG

DS_WEEKLY_RENDER_CONFIG = RenderConfig(
  selector='weekly_ds_run',
  load_method=LoadMode.DBT_LS,
  enable_mock_profile=False
)

@dag(
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  schedule = '0 21 * * 4', ## (non DST) 3 PM US/Central on Thursday
  start_date=datetime.datetime(2026, 1, 15),
  catchup=False,
  default_args={
    "retries": 2,
    "retry_delay": duration(seconds=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": duration(minutes=5),
  },
    tags=['data_science']
)
def weeklyDsRun():
  dbt_run = DbtTaskGroup(
    group_id='weekly_ds_run',
    project_config=DBT_PROJECT_CONFIG,
    profile_config=PROD_DBT_PROFILE_CONFIG,
    execution_config=DBT_EXECUTION_CONFIG,
    render_config=DS_WEEKLY_RENDER_CONFIG ,
    operator_args={
          "py_system_site_packages": False,
          "py_requirements": ["dbt-snowflake"],
          "install_deps": True,
          "emit_datasets": False,
          "execution_timeout": datetime.timedelta(minutes=10)
    },
  )

weeklyDsRun()

