from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import task_group, chain

from common.sql_operator_handlers import fetch_single_result, fetch_stupid_list
    
@task_group(group_id='pre_process_setup')
def preProcessSetup():
  erichs = SQLExecuteQueryOperator(
    task_id='get_erichs', 
    conn_id='snowflake', 
    sql='queries/experian_erichs.sql',
    handler=fetch_single_result
  )

  create_temporary_table = SQLExecuteQueryOperator(
    task_id='create_temporary_table', 
    conn_id='snowflake',
    sql='queries/create_temp_customers_table.sql',
  )

  get_batch_quantity = SQLExecuteQueryOperator(
    task_id='get_stupid_list_for_number_of_batches', 
    conn_id='snowflake',
    sql='SELECT CEIL(COUNT(*)/{{ params.batch_size }}) FROM brine.{{ params.temp_table_prefix }}_temp;',
    handler=fetch_stupid_list
  )

  chain(create_temporary_table, get_batch_quantity)

  return {
    'erichs': erichs.output,
    'stupid_list': get_batch_quantity.output
  }