import datetime
import os 

from typing import Any, Dict, List
from pendulum import duration

from airflow.sdk import dag, task, chain, Variable
from airflow.exceptions import AirflowException
from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

@dag(
    on_failure_callback=SlackNotifier(
      slack_conn_id='team-data-notifications',
      text='Warning: Weekly Data Science Pipeline Failure. DBT Run/Test commands failed to run for the Data Science Pipeline.',
      channel='team-data-notifications'
    ),
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
        'target': 'prod'
    }
)
def weekly_ds_run():
  '''Weekly Data Science Pipeline Run
  Description: Initiate the dbt run and test commands for the Data Science (DS) pipeline 

  Schedule: Weekly 

  Dependencies: 

  Variables:

  '''

  project_config = ProjectConfig(
    dbt_project_path='images/airflow/3.0.6/dags/dbt/ds_weekly_run',
    models_relative_path='/ds_weekly_run/models',
    project_name='spice_rack',
    install_dbt_deps=True
  )

  profile_config = ProfileConfig(
    profile_name='spice_rack',
    target_name='dev',
    profiles_yml_filepath='images/airflow/3.0.6/dags/dbt/ds_weekly_run'
    )

  dbt_run = DbtDag(
    dag_id='weekly_ds_run',
    project_config=project_config,
    profile_config=profile_config
  )


''' previous airflow_dbt_python operators 

  dbt_run = DbtRunOperator(
    task_id='weekly_ds_build', 
    selector_name='weekly_ds_run',
    project_dir=Variable.get('spice_rack_dbt_bucket'),
    profiles_dir=f'{Variable.get('spice_rack_dbt_bucket')}/.dbt/',
    profile='spice_rack',
    target=context.get('params')['target']
  )

  dbt_test = DbtTestOperator(
    task_id='weekly_ds_build', 
    selector_name='weekly_ds_run',
    project_dir=Variable.get('spice_rack_dbt_bucket'),
    profiles_dir=f'{Variable.get('spice_rack_dbt_bucket')}/.dbt/',
    profile='spice_rack',
    target=context.get('params')['target']
  )
  '''

  # Run all dbt DS models, then test all dbt DS models 
  # chain(dbt_run, dbt_test) 

# DAG Call 
weekly_ds_run() 