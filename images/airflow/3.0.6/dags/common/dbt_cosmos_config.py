import os 
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode 
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping 

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

DBT_PROJECT_CONFIG = ProjectConfig(
  dbt_project_path=f'{AIRFLOW_HOME}/spice_rack',
  project_name='spice_rack',
  install_dbt_deps=True
)

DBT_EXECUTION_CONFIG = ExecutionConfig(
  execution_mode=ExecutionMode.VIRTUALENV,
  dbt_executable_path=f'{AIRFLOW_HOME}/dbt_venv/bin/dbt'
)

PROD_DBT_PROFILE_CONFIG = ProfileConfig(
  profile_name='spice_rack',
  target_name='prod',
  profile_mapping = 
    SnowflakePrivateKeyPemProfileMapping(
        conn_id='snowflake',
        profile_args={
            'schema': 'prod',
        },
    ), 
)

TEST_DBT_PROFILE_CONFIG = ProfileConfig(
  profile_name='spice_rack',
  target_name='test',
  profile_mapping = 
    SnowflakePrivateKeyPemProfileMapping(
        conn_id='snowflake',
        profile_args={
            'schema': 'test_elly',
        },
    ), 
)
