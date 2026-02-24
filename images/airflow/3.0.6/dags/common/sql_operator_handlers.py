from airflow.exceptions import AirflowException

def fetch_single_result(cursor):
  output, = cursor.fetchone()
  return output

def fetch_results_array(cursor):
  output = [x[0] for x in cursor.fetchall()]
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
  
def fetch_typeform_responses(cursor):
  forms_and_responses = {}
  for response in cursor.fetchall():
      form_dict = {}
      form_dict[response[1]] = response[2].split(', ')
      forms_and_responses[response[0]] = form_dict
  return forms_and_responses