from pendulum import duration

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, Param
from airflow.timetables.trigger import CronTriggerTimetable

from common.slack_notifications import bad_boy, good_boy, getSlackChannelNameParam
from experian.pre_process_setup_task_group import preProcessSetup
from experian.process_customer_batch_task_group import processBatch

@dag(
  dag_id='experian_extraction',
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  schedule=CronTriggerTimetable('0 3 * * *', timezone='America/Chicago'),
  catchup=False,
  default_args={
    'retries': 2,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['internal', 'data-integration'],
  params={
    'channel_name': getSlackChannelNameParam(),
    'temp_table_prefix': Param('experian_customers', type='string', description='prefix for temp table of all customers to send to experian at the time of the run - created in snowflake as {temp_table_prefix}_temp - table will not persist beyond dag run.'),
    'batch_size': Param(100, type='integer', minimum=1, maximum=300, description='number of customers per batch. Due to Experian API limitations, must be <= 300.'),
    'full_refresh': Param(False, type='boolean')
  }
)
def experianExtraction():
  ''' Experian Extraction Pipeline
  Description: Retrieves Experian information for new customers

  Schedule: Daily at 3AM

  Dependencies:

  Variables:

  '''
  pre_process_setup = preProcessSetup()
  
  process_batch = processBatch.partial(erichs=pre_process_setup['erichs']).expand(stupid_list=pre_process_setup['stupid_list'])
  
  delete_temporary_table = SQLExecuteQueryOperator(
    task_id='delete_temporary_table',
    trigger_rule='all_done_min_one_success', # always tear down as long as at least part of the dag was attempted successfully 
    conn_id='snowflake',
    sql='DROP TABLE brine.{{ params.temp_table_prefix }}_temp;'
  )
  
  trigger_chili_load = TriggerDagRunOperator(
    task_id='trigger_chili_load_dag',
    trigger_rule='all_success',
    trigger_dag_id='experian_load_to_chili',
    trigger_run_id='triggered_{{ run_id }}'
  )

  process_batch >> delete_temporary_table >> trigger_chili_load

  

experianExtraction()
