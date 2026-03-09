from airflow.sdk import task

@task(task_id='get_dag_params', multiple_outputs=True)
def getDagParams(**context):
  dag_params = context['params']

  return dag_params