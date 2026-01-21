import datetime
import os 

from pendulum import duration
from airflow.sdk import dag, task, chain, Variable
from common.slack_notifications import bad_boy, good_boy
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig, ExecutionMode, InvocationMode
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping, SnowflakeEncryptedPrivateKeyPemProfileMapping, get_automatic_profile_mapping

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

project_config = ProjectConfig(
  dbt_project_path=f'{AIRFLOW_HOME}/spice_rack',
  models_relative_path='models', # test removing entirely 
  project_name='spice_rack',
  install_dbt_deps=True
)

profile_config = ProfileConfig(
  profile_name='spice_rack',
  target_name='test',
  profile_mapping = # get_automatic_profile_mapping(conn_id='snowflake')
      SnowflakePrivateKeyPemProfileMapping(
      conn_id='snowflake',
      profile_args={
        'schema': 'test_elly',
      },
      #profiles_yml_filepath=f'{AIRFLOW_HOME}/spice_rack',
    ), 
)

execution_config = ExecutionConfig(
  # execution_mode=ExecutionMode.VIRTUALENV,
  # virtualenv_dir=Path('/dbt_venv/bin/dbt')
  dbt_executable_path=f'{AIRFLOW_HOME}/dbt_venv/bin/dbt'
)

ds_weekly_config = RenderConfig(
  select=['path:models/anise/', 'path:models/basil/'],
  enable_mock_profile=False
)

dbt_run = DbtDag(
  dag_id='weeklyDsRun',
  project_config=project_config,
  profile_config=profile_config,
  execution_config=execution_config,
  render_config=ds_weekly_config ,
  operator_args={
        "py_system_site_packages": False,
        "py_requirements": ["dbt-snowflake"],
        "install_deps": True,
        "emit_datasets": False,
        "execution_timeout": datetime.timedelta(minutes=10)
  }, 
  # Normal dag parameters 
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  schedule = '0 21 * * 4', ## (non DST) 3 PM US/Central on Thursday
  ## schedule = '0 20 * * 4' ## (DST) 3 PM US/Central on Thursday
  start_date=datetime.datetime(2026, 1, 15),
  catchup=False,
  max_active_tasks=1,
  dagrun_timeout=datetime.timedelta(minutes=10),
  default_args={
    #"retries": 2,
    #"retry_delay": duration(seconds=2),
    #"retry_exponential_backoff": True,
    #"max_retry_delay": duration(minutes=5),
  },
    tags=['data_science']
)

