import os
from pendulum import duration

from airflow.sdk import chain, dag, task
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLValueCheckOperator

from common.slack_notifications import bad_boy, good_boy, getSlackChannelNameParam
from common.chili import chili_params, renderChiliQuery
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
    'channel_name': getSlackChannelNameParam(),
    **chili_params(table='experian_customers',
                   stage='experian_stage',
                   columns=COLUMNS,
                   full_refresh=False,
                   storage_integration='EXPERIAN_STORAGE_INTEGRATION',
                   url='s3://tovala-data-experian/'
    )
  },
  template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates'
)
def experianLoad():

  table_exists = SQLValueCheckOperator(
    task_id='check_table_exists',
    conn_id='snowflake',
    sql='check_table_existence.sql',
    pass_value=1,
    retries=0
  )

  create_stage = SQLExecuteQueryOperator(
    task_id='create_stage',
    conn_id='snowflake', 
    sql='create_stage.sql'
  )

  #rendered_sql = renderChiliQuery(table_exists)

  @task(trigger_rule='all_done')
  def test(**context):

    dag_run_id = context['dag_run'].id
    # TODO: figure out why dagrun doesn't have get_task_instance
    
    #ti = fetch_task_instance(dag_run_id=dag_run_id, dag_id='experian_load_to_chili', task_id='check_table_exists')
    print(ti)
    

  get_context = test()
  #chili_load = SQLExecuteQueryOperator(
    #task_id='chili_load',
    #conn_id='snowflake',
   # sql=rendered_sql,
   # split_statements=True
  #)

  chain([table_exists, create_stage], get_context)
experianLoad()