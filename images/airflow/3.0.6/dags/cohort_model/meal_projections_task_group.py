import os
from typing import List

import polars as pl

from airflow.sdk import task_group, chain, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from cohort_model.common_functions import get_cohort_age_matrix, get_projected_order_counts_expr

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@task_group(group_id='order_projections')
def mealProjections(projection_terms_array: List[str]) -> None:
  @task
  def computeMealProjections(order_projections_csv: str, projection_terms_array: List[str], **context) -> str:
    dag_params = context['params']
    run_id = context['run_id']
    ti = context['ti'] 
    
    start_term = ti.xcom_pull(task_ids='get_projection_start_term', key='return_value')
    end_term = ti.xcom_pull(task_ids='get_projection_end_term', key='return_value')

    database = dag_params.get('database')
    runtime_schema = dag_params.get('runtime_schema_prefix') + '_' + run_id

    local_filename = 'order_projections.csv'

    hook = SnowflakeHook(snowflake_conn_id='snowflake')
    conn = hook.get_conn()
    cursor = conn.cursor()

    order_projections_matrix = pl.read_csv(f'{AIRFLOW_HOME}/{order_projections_csv}')

    init_meal_counts = cursor.execute(f'''
      WITH future_meal_counts AS (
        SELECT
            term_id
            ,SUM((CASE
                WHEN is_sale_period AND NOT is_d2c_holiday
                THEN (projected_d2c_sales * {d2c_sale})
                WHEN is_d2c_holiday
                THEN (projected_d2c_sales * {d2c_holiday})
                ELSE (projected_d2c_sales * {d2c_nonholiday})
            END + (projected_amazon_sales * {amazon}))) AS meal_count
        from mugwort.combined_oven_sales GROUP BY term_id
        )
        select 
            COALESCE(hmo.cohort, fmc.term_id) AS cohort
            , COALESCE(hmo.meal_count, fmc.meal_count) AS meal_count
        from MASALA."COHORT_MODEL_manual__2026-04-27T20:56:27.669400+00:00".HISTORICAL_MEAL_ORDERS hmo
        FULL JOIN future_meal_counts fmc ON fmc.term_id = hmo.cohort
        WHERE hmo.term_id = hmo.cohort OR hmo.cohort IS NULL
        ORDER BY all;
    ''')



  compute_meal_projections = computeMealProjections(projection_terms_array)
  
  order_projections_to_S3 = LocalFilesystemToS3Operator(
    task_id='order_projections_output',
    filename=f'{AIRFLOW_HOME}/{compute_meal_projections}',
    dest_key='outputs/{{ run_id }}/order_projections.csv',
    dest_bucket='tovala-data-cohort-model',
    replace=True
  )

  chain(compute_meal_projections, order_projections_to_S3)