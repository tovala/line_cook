
import os
from typing import Dict, List
from pendulum import duration

from airflow.sdk import dag, task, chain, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, BranchSQLOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import bad_boy, good_boy, slack_param
from common.chili import chili_macros

from cio_sf_integration.chili_vars import DELIVERIES_COLUMNS, BROADCASTS_COLUMNS, CAMPAIGNS_COLUMNS, PEOPLE_COLUMNS, METRICS_COLUMNS, OUTPUTS_COLUMNS, SUBJECTS_COLUMNS

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  # Move CIO data into Snowflake tables every day at 7am 
  schedule=CronTriggerTimetable('0 7 * * *', timezone='America/Chicago'),
  catchup=False,
  default_args={
    'retries': 2,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['internal', 'data-integration', 'chili'],
  params={
    'channel_name': slack_param(),
    'database': 'MASALA',
    'schema': 'CHILI_V2',
    'stage': 'CIO_STAGE',
    's3_url': 's3://tovala-data-customerio/',
    'storage_integration': 'CIO_STORAGE_INTEGRATION',
    'file_format': 'parquet',
  },
  template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates',
  user_defined_macros={
    **chili_macros()
  }
)
def customerIOSnowflakeIntegration():
  '''Customer IO - Snowflake Integration 
  Description: We move Customer IO data from S3 buckets to a Snowflake external stage. 
    Now, we want to copy data from the external stage (customerio_data_v2.s3_files) into 
    the tables we created within customerio_data_v2. 

  Schedule: Daily

  Dependencies:

  Variables:

  '''
  # Subset of Customer IO table schemas that we want to load into Snowflake 
  TABLE_NAMES = ['broadcasts', 'campaigns', 'campaign_actions', 'deliveries', 'metrics', 'outputs', 'people', 'subjects']
  
  #### Custom Task Definitions
  @task(task_id='set_table_params')
  def setTableParams(tables: List[str]) -> List[Dict[str, str]]:
    expanded_args = []

    for table in tables: 
      expanded_args.append({'table': f'CIO_{table.upper()}', 'pattern': f'.*{table}.*.parquet', 'columns':f'{table.upper()}_COLUMNS'}) 

    return expanded_args 

  #### Task Instances
  @task_group(group_id='cio_chili_load')
  def cioChiliLoad(current_table: str, current_pattern: str, current_columns: str):
    table_exists = BranchSQLOperator(
      task_id='check_table_exists',
      conn_id='snowflake',
      sql='check_table_existence.sql',
      follow_task_ids_if_true='chili_load.copy_tables',
      follow_task_ids_if_false='chili_load.create_chili_table',
    )

    create_stage = SQLExecuteQueryOperator(
      task_id='create_stage',
      conn_id='snowflake', 
      sql='create_stage.sql'
    )

    create_chili_table = SQLExecuteQueryOperator(
      task_id='create_chili_table',
      conn_id='snowflake',
      sql='{{ generate_create(params.database, params.schema, params.table, params.columns, params.where_clause, params.stage, run_id) }}',
      params={
        'table': current_table,
        'columns': current_columns,
        'where_clause': None
      }
    )

    copy_tables = CopyFromExternalStageToSnowflakeOperator(
    task_id='copyTable', 
    snowflake_conn_id='snowflake',
    stage='MASALA.CHILI_V2.cio_stage',
    file_format="(TYPE = 'parquet')",
    copy_options='MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE',
    table=current_table,
    pattern=current_pattern
    )

    chain([table_exists, create_stage], create_chili_table, copy_tables)
  
  table_params = setTableParams(TABLE_NAMES)

  cioChiliLoad.expand(table_params)

# DAG call
customerIOSnowflakeIntegration()