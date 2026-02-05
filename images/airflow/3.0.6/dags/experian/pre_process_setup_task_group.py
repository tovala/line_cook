from airflow.sdk import task_group, chain
from airflow.exceptions import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def fetch_single_result(cursor):
  output, = cursor.fetchone()
  return output

def fetch_stupid_list(cursor):
  '''
  Docstring for fetch_stupid_list. Sets a list with the same value a given number of times so that we can expand and run batches in parallel.
  
  :param cursor: Snowflake cursor
  '''
  output, = cursor.fetchone()
  try:
    # empty params dict - if additional params are needed for batch processing, add them here
    
    # List of repeated params tells expand how many times to run
    stupid_list = ['dummy_str' for index in range(int(output))]
    return stupid_list
  
  except ValueError as e:
    raise AirflowException('Query did not return an integer, cannot get range.')
    

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