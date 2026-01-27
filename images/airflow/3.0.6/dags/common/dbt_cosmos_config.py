import os 
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode 
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping
from airflow.sdk import Variable

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
SF_AWS_KEY = Variable.get('dbt_sf_aws_key')
SF_AWS_SECRET = Variable.get('dbt_sf_aws_secret')

DBT_PROJECT_CONFIG = ProjectConfig(
  dbt_project_path=f'{AIRFLOW_HOME}/dags/spice_rack',
  project_name='spice_rack',
  install_dbt_deps=True,
  env_vars={
    'SF_AWS_KEY': SF_AWS_KEY,
    'SF_AWS_SECRET': SF_AWS_SECRET,
  }
)

DBT_EXECUTION_CONFIG = ExecutionConfig(
  execution_mode=ExecutionMode.VIRTUALENV,
  dbt_executable_path=f'{AIRFLOW_HOME}/dbt_venv/bin/dbt'
)

# Use this version when opening a PR for Prod deployment
PROD_DBT_PROFILE_CONFIG = ProfileConfig(
  profile_name='spice_rack',
  target_name='prod',
  profile_mapping = SnowflakePrivateKeyPemProfileMapping(conn_id='snowflake_dbt_prod')
)

# Use this version for local testing
TEST_DBT_PROFILE_CONFIG = ProfileConfig(
  profile_name='spice_rack',
  target_name='test',
  profile_mapping = SnowflakePrivateKeyPemProfileMapping(conn_id='snowflake_dbt_test')
)
