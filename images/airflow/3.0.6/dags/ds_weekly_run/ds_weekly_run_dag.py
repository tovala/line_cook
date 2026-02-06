import datetime

from pendulum import duration
from cosmos import DbtTaskGroup, RenderConfig, LoadMode, TestBehavior, DbtRunOperationLocalOperator
from airflow.sdk import dag, chain, Variable
from airflow.timetables.trigger import CronTriggerTimetable

from common.slack_notifications import bad_boy, good_boy, getSlackChannelNameParam
from common.dbt_cosmos_config import DBT_PROJECT_CONFIG, DBT_WATCHER_EXECUTION_CONFIG, PROD_DBT_PROFILE_CONFIG, DBT_PROJECT_DIR, DBT_EXECUTABLE_PATH

@dag(
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  schedule=CronTriggerTimetable('0 15 * * 4', timezone='America/Chicago'),
  start_date=datetime.datetime(2026, 1, 15),
  catchup=False,
  default_args={
    'retries': 2,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['data_science', 'dbt'],
  params={
    'channel_name': getSlackChannelNameParam()
  },
  render_template_as_native_obj=True
)
def weeklyDsRun():
  '''
  Weekly Run of any dbt models with the 'weekly_ds_run' selector.
  '''
  build_weekly_ds_models = DbtTaskGroup(
    group_id='weekly_ds_run',
    project_config=DBT_PROJECT_CONFIG,
    profile_config=PROD_DBT_PROFILE_CONFIG,
    execution_config=DBT_WATCHER_EXECUTION_CONFIG,
    render_config=RenderConfig(
      selector='weekly_ds_run',
      load_method=LoadMode.DBT_LS,
      enable_mock_profile=False,
      test_behavior=TestBehavior.AFTER_ALL
    ),
    operator_args={
      'py_system_site_packages': False,
      'py_requirements': ['dbt-snowflake'],
      'install_deps': True,
      'emit_datasets': False,
      'execution_timeout': datetime.timedelta(minutes=10),
    },
  )

weeklyDsRun()

