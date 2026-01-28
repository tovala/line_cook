import os 
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode 
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping
from airflow.sdk import Variable

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
SF_AWS_KEY = Variable.get('dbt_sf_aws_key')
SF_AWS_SECRET = Variable.get('dbt_sf_aws_secret')

# To use with one-off operators
DBT_PROJECT_DIR = f'{AIRFLOW_HOME}/dags/spice_rack'
DBT_EXECUTABLE_PATH = f'{AIRFLOW_HOME}/dbt_venv/bin/dbt'

DBT_PROJECT_CONFIG = ProjectConfig(
  dbt_project_path=DBT_PROJECT_DIR,
  project_name='spice_rack',
  install_dbt_deps=True,
  env_vars={
    'SF_AWS_KEY': SF_AWS_KEY,
    'SF_AWS_SECRET': SF_AWS_SECRET,
  }
)

# Much faster - experimental Cosmos feature - uses `dbt build` so will run all 
# models, tests, seeds, snapshots for the given selector
DBT_WATCHER_EXECUTION_CONFIG = ExecutionConfig(
  execution_mode=ExecutionMode.WATCHER,
  dbt_executable_path=f'{AIRFLOW_HOME}/dbt_venv/bin/dbt'
)

# Uses `dbt run` in Virtual Env
DBT_VENV_EXECUTION_CONFIG = ExecutionConfig(
  execution_mode=ExecutionMode.VIRTUALENV,
  dbt_executable_path=DBT_EXECUTABLE_PATH
)

# Use this version when opening a PR for Prod deployment
PROD_DBT_PROFILE_CONFIG = ProfileConfig(
  profile_name='spice_rack',
  target_name='prod',
  profile_mapping = SnowflakePrivateKeyPemProfileMapping(
    conn_id='snowflake_dbt_prod',
    profile_args = {
      "threads": 16
    }
  )
)

# Use this version for local testing
TEST_DBT_PROFILE_CONFIG = ProfileConfig(
  profile_name='spice_rack',
  target_name='test',
  profile_mapping = SnowflakePrivateKeyPemProfileMapping(
    conn_id='snowflake_dbt_test',
    profile_args = {
      "threads": 8
    }
  )
)
