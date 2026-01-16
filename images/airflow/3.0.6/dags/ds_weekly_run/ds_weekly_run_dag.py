import datetime
import os 

from pendulum import duration
from airflow.sdk import dag, task, chain, Variable
from common.slack_notifications import bad_boy, good_boy
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import SnowflakeEncryptedPrivateKeyPemProfileMapping

@dag(
    on_failure_callback=bad_boy,
    on_success_callback=good_boy,
    schedule = '0 21 * * 4', ## (non DST) 3 PM US/Central on Thursday
    ## schedule = '0 20 * * 4' ## (DST) 3 PM US/Central on Thursday
    start_date=datetime.datetime(2026, 1, 15),
    catchup=False,
    # dag_run_timeout=datetime.timedelta(minutes=10),
    default_args={
      "retries": 2,
      "retry_delay": duration(seconds=2),
      "retry_exponential_backoff": True,
      "max_retry_delay": duration(minutes=5),
    },
    tags=['data_science'],
    params={
        'SF_ACCOUNT': 'jta05846'
    }
)
def weeklyDsRun():
  '''Weekly Data Science Pipeline Run
  Description: Initiate the dbt run and test commands for the Data Science (DS) pipeline 

  Schedule: Weekly 

  Dependencies: 

  Variables:

  '''

  AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

  project_config = ProjectConfig(
    dbt_project_path=f'{AIRFLOW_HOME}/dags/dbt',
    models_relative_path=f'{AIRFLOW_HOME}/dags/dbt',
    project_name='spice_rack',
    install_dbt_deps=True
  )

  profile_config = ProfileConfig(
    profile_name='spice_rack',
    target_name='test',
    profile_mapping=SnowflakeEncryptedPrivateKeyPemProfileMapping(
        conn_id='snowflake',
        profile_args={
          'account': 'jta05846',
          'user': 'JENKINS',
          'role': 'ETL', 
          'warehouse': 'ETL_WAREHOUSE',
          'database': 'MASALA'
        },
    ),
  )

  execution_config = ExecutionConfig(
    dbt_executable_path=f'{AIRFLOW_HOME}/dbt_venv/bin/dbt'
  )

  render_config = RenderConfig(
    select=['path:models/anise/', 'path:models/basil/']
  )

  dbt_run = DbtTaskGroup(
    group_id='dbt_run',
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=render_config 
  )

weeklyDsRun()