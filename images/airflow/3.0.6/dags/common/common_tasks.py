from airflow.sdk import task

@task(task_id='get_dag_params', multiple_outputs=True)
def getDagParams(**context):
  '''Returns Dict representation of dag-level Params.
  Useful when you need to reference a dag Param in a non-templated field in an Operator.
  '''
  dag_params = context['params']

  return dag_params