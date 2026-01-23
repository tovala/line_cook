import os 
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode 
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping 

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

dbt_project_config = ProjectConfig(
  dbt_project_path=f'{AIRFLOW_HOME}/spice_rack',
  # models_relative_path='models', # test removing entirely 
  project_name='spice_rack',
  install_dbt_deps=True
)

dbt_profile_config = ProfileConfig(
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

dbt_execution_config = ExecutionConfig(
  execution_mode=ExecutionMode.VIRTUALENV,
  dbt_executable_path=f'{AIRFLOW_HOME}/dbt_venv/bin/dbt'
)