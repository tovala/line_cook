
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

  # TODO: for each table in the temp cohort model schema, run the COPY INTO unload command

