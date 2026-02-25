import datetime

from cosmos import DbtTaskGroup, RenderConfig, LoadMode, TestBehavior
from airflow.sdk import dag

from common.slack_notifications import bad_boy, good_boy, slack_param
from common.dbt_cosmos_config import DBT_PROJECT_CONFIG, DBT_WATCHER_EXECUTION_CONFIG, PROD_DBT_PROFILE_CONFIG

@dag(
  dag_id='dbt_notifications_testing',
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  start_date=datetime.datetime(2026, 1, 15),
  tags=['data_science', 'dbt'],
  params={
    'channel_name': slack_param()
  },
  render_template_as_native_obj=True
)
def dbtNotificationsTesting():
  '''
  Weekly Run of any dbt models with the 'weekly_ds_run' selector.
  '''
  dbt_models = DbtTaskGroup(
    group_id='notifications_testing_dbt_models',
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

dbtNotificationsTesting()

def print_context(context):
  print('***** printing context ******')
  for key, val in context.items():
    print(f'*****{key}: {val} \n')