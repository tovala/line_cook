import os
from pendulum import duration

from airflow.sdk import chain, dag, task
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, BranchSQLOperator

from common.slack_notifications import bad_boy, good_boy, getSlackChannelNameParam
from common.chili import chili_params, generate_copy_into_chili_query, generate_create_chili_table_query
from experian.chili_vars import COLUMNS

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
  dag_id='experian_load_to_chili',
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  schedule=None, # Externally triggered by experian_extraction_dag
  catchup=False,
  default_args={
    'retries': 2,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['internal', 'data-integration', 'chili'],
  params={
    #'channel_name': getSlackChannelNameParam(),
    **chili_params(table='experian_customers',
                   stage='experian_stage',
                   columns=COLUMNS,
                   full_refresh=False,
                   storage_integration='EXPERIAN_STORAGE_INTEGRATION',
                   url='s3://tovala-data-experian/'
    )
  },
  template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates',
  user_defined_macros={
    'generate_copy_into': generate_copy_into_chili_query,
    'generate_create': generate_create_chili_table_query
  }
)
def experianLoad():

  table_exists = BranchSQLOperator(
    task_id='check_table_exists',
    conn_id='snowflake',
    sql='check_table_existence.sql',
    follow_task_ids_if_true='chili_load',
    follow_task_ids_if_false='create_chili_table',
  )

  create_stage = SQLExecuteQueryOperator(
    task_id='create_stage',
    conn_id='snowflake', 
    sql='create_stage.sql'
  )

  create_chili_table = SQLExecuteQueryOperator(
    task_id='create_chili_table',
    conn_id='snowflake',
    sql='{{ generate_create(table=params.table, columns=params.columns, stage=params.stage, run_id=run_id) }}'
  )

  chili_load = SQLExecuteQueryOperator(
    task_id='chili_load',
    conn_id='snowflake',
    sql='{{ generate_copy_into(table=params.table, columns=params.columns, stage=params.stage) }}',
    trigger_rule='none_failed'
  )

  chain([table_exists, create_stage], create_chili_table, chili_load)
experianLoad()