
@task_group()
def snapshotSnowflakeToS3():
  '''
  '''
  unload_stage = SQLExecuteQueryOperator(
    task_id='create_unload_stage', 
    conn_id='snowflake', 
    sql='create_stage.sql',
    params={
      'database': 'MASALA',
      'schema': 'BRINE',
      'stage': 'cohort_model_snapshot_stage',
      's3_url': 's3://tovala-data-cohort-model/output/',
      'storage_integration': 'COHORT_MODEL_STORAGE_INTEGRATION',
      'file_format': 'parquet',
    },
  )

  get_table_names = SQLExecuteQueryOperator(
    task_id='get_table_names',
    conn_id='snowflake',
    sql='get_tables_from_schema.sql',
    handler=fetch_results_array
  )

  unload_tables = SQLExecuteQueryOperator.partial(
    task_id='unload_table',
    conn_id='snowflake',
    sql='unload_to_stage.sql'
  ).expand(params=[{'table': t} for t in get_table_names])