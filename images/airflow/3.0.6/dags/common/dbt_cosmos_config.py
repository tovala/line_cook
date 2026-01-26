import os 
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, ExecutionMode 
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping
from airflow.sdk import Variable

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]
SF_AWS_KEY = Variable.get('dbt_sf_aws_key')
SF_AWS_SECRET = Variable.get('dbt_sf_aws_secret')

DBT_PROJECT_CONFIG = ProjectConfig(
  dbt_project_path=f'{AIRFLOW_HOME}/spice_rack',
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

def getProfileConfig(target: str) -> ProfileConfig:
  '''
  Returns the appropriate DBT Profile Config for the given target. If target is Prod, use prod ProfileConfig,
  otherwise default ProfileConfig will be test.
  
  Args:
    target (str): the dbt target to use 
  
  Output:
    (ProfileConfig): Profile Config object for the given target
  '''

  if target.upper() in ['PROD', 'PRODUCTION']:
    return ProfileConfig(
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
  else:
    return ProfileConfig(
      profile_name='spice_rack',
      target_name='test',
      profile_mapping = 
        SnowflakePrivateKeyPemProfileMapping(
            conn_id='snowflake',
            profile_args={
                'schema': f"test_{Variable.get('branch_schema_name', default='default_user')}",
            },
        ), 
    )
