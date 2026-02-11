import os
from pendulum import duration

from airflow.sdk import dag, Param
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.slack_notifications import bad_boy, good_boy, getSlackChannelNameParam
from common.chili import chili_params, render_chili_query
from experian.chili_vars import COLUMNS

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

def fetch_table_existence(cursor):
  output, = cursor.fetchone()
  if output == 0:
    return False
  
  if output == 1:
    return True
  
  else:
    raise AirflowException(f'Unexpected query result: {output}. Expected 1 if table exists, or 0 if it doesn\'t.')

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
    'channel_name': getSlackChannelNameParam(),
    **chili_params(table='experian_customers', stage='experian_stage', columns=COLUMNS, full_refresh=False)
  },
  template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates',
  user_defined_macros={
    'render_chili_query': render_chili_query
  }
)
def newExperianLoad():
  table_exists = SQLExecuteQueryOperator(
    task_id='check_table_exists',
    sql='check_table_existence.sql',
    handler=fetch_table_existence
  )

  chili_load = SQLExecuteQueryOperator(
    task_id='chili_load',
    sql=render_chili_query(table_exists),
    split_statements=True
  )

newExperianLoad()