import os
from typing import List

import polars as pl

from airflow.sdk import task_group, chain, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

from cohort_model.common_functions import get_cohort_age_matrix, get_projected_order_counts_expr

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@task_group(group_id='order_projections')
def orderProjections(projection_terms_array: List[str]) -> None:
  @task
  def computeOrderProjections(projection_terms_array: List[str], **context) -> str:
    dag_params = context['params']
    run_id = context['run_id']
    ti = context['ti'] 
    
    start_term = ti.xcom_pull(task_ids='get_projection_start_term', key='return_value')
    end_term = ti.xcom_pull(task_ids='get_projection_end_term', key='return_value')

    database = dag_params.get('database')
    runtime_schema = dag_params.get('runtime_schema_prefix') + '_' + run_id
    lookback_window = dag_params.get('lookback_adjustment_window')
    longtail_value = dag_params.get('longtail_weekly_retention_multiplier')

    local_filename = 'order_projections.csv'

    hook = SnowflakeHook(snowflake_conn_id='snowflake')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cohort_age_matrix = get_cohort_age_matrix(start_term=start_term, end_term=end_term)

    # Set Up dataframes from Snowflake Queries
    # fetch aggregate order retention curve generated for this run as a DataFrame
    agg_order_retention_curves_arrow_table = cursor.execute(
      f'''SELECT
            *
          FROM { database }."{ runtime_schema }".AGGREGATE_ORDER_RETENTION_CURVES;
      '''
    ).fetch_arrow_all()

    agg_order_retention_curves = pl.from_arrow(agg_order_retention_curves_arrow_table)

    # fetch inital order values by cohort
    init_order_val_arrow_table = cursor.execute(
      f'''SELECT 
            cohort
            ,order_count AS init_order_val 
          FROM { database }."{ runtime_schema }".HISTORICAL_MEAL_ORDERS
          WHERE term_id = cohort
          UNION
          SELECT 
            *
          FROM { database }.mugwort.FUTURE_COHORT_INITIAL_ORDER_PROJECTIONS
          ORDER BY cohort;
      '''
    ).fetch_arrow_all()

    inital_order_values_by_cohort_matrix = pl.from_arrow(init_order_val_arrow_table)

    # fetch cohort ages during the lookback window terms
    lookback_cohort_age_matrix = get_cohort_age_matrix(start_term=start_term, end_term=end_term, lookback_window=lookback_window)
      
    lookback_all_data = pl.concat([lookback_cohort_age_matrix, agg_order_retention_curves, inital_order_values_by_cohort_matrix], how='align')
    
    lookback_projected_order_counts = lookback_all_data.select(
      pl.col('COHORT')
      ,get_projected_order_counts_expr(longtail_value=longtail_value, col_regex='^TERM_\d+$')
    ).select(
      pl.col('COHORT')
      ,PROJECTED_ORDER_COUNTS_TOTAL = pl.sum_horizontal(pl.exclude('COHORT'))
    )

    # Calculate Correction Factor
    # fetch total order count within lookback window
    lookback_order_sum_arrow_table = cursor.execute(
      f'''WITH lookback as (
            SELECT 
              *
            FROM { database }."{ runtime_schema}".historical_meal_orders
            QUALIFY ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY term_id desc)<= { lookback_window }
          )
          SELECT 
            cohort
            ,CASE WHEN COUNT(term_id) = { lookback_window } 
                  THEN SUM(order_count)
                  ELSE NULL 
            END AS actual_order_counts_total
          FROM lookback GROUP BY cohort ORDER BY cohort;
      '''
    ).fetch_arrow_all()

    lookback_order_sum_by_cohort_matrix = pl.from_arrow(lookback_order_sum_arrow_table)

    correction_factor_order_counts = pl.concat([lookback_order_sum_by_cohort_matrix, lookback_projected_order_counts], how='align')

    correction_factor_matrix = correction_factor_order_counts.select(
      pl.col('COHORT')
      ,pl.when(pl.col('ACTUAL_ORDER_COUNTS_TOTAL').is_not_null())
      .then(pl.col('ACTUAL_ORDER_COUNTS_TOTAL')/pl.col('PROJECTED_ORDER_COUNTS_TOTAL'))
      .otherwise(1).alias('CORRECTION_FACTOR')
    )

    all_data = pl.concat([cohort_age_matrix, agg_order_retention_curves, inital_order_values_by_cohort_matrix], how='align')
    
    projected_order_counts = all_data.select(
      pl.col('COHORT')
      ,get_projected_order_counts_expr(longtail_value=longtail_value, col_regex='^TERM_\d+$')
    )

    # fetch holiday skip adjustments
    skip_adjustment_arrow_table = cursor.execute(
      f'''SELECT DISTINCT
            'TERM_' || ft.term_id::STRING AS term_id
            , 1 + skip_adjustment AS holiday_skip_multiplier
          FROM { database }.mugwort.skip_adjustments AS sa
          RIGHT JOIN { database }.mugwort.future_terms AS ft
          ON sa.term_id = ft.term_id
          WHERE ft.term_id BETWEEN { start_term } AND { end_term }
          AND NOT is_skipped_term
          ORDER BY term_id;
      '''
    ).fetch_arrow_all()
    
    skip_adjustment_matrix = pl.from_arrow(skip_adjustment_arrow_table)

    projected_order_counts_transpose = projected_order_counts.select(
      pl.when(pl.col('^TERM_\d+$') < 0).then(None).otherwise(pl.col('^TERM_\d+$')).name.keep()
    ).transpose()

    order_projections_skip_adj = (projected_order_counts_transpose * skip_adjustment_matrix.get_column('HOLIDAY_SKIP_MULTIPLIER')).transpose(column_names=projection_terms_array)
    order_projections_corrected = order_projections_skip_adj.select(pl.col('^TERM_\d+$')) * correction_factor_matrix.get_column('CORRECTION_FACTOR')

    final_order_projections = pl.concat([projected_order_counts.select(pl.col('COHORT')), order_projections_corrected.select(pl.all().round())], how='horizontal')
    final_order_projections.write_csv(local_filename)

    
    return local_filename

  compute_order_projections = computeOrderProjections(projection_terms_array)
  
  order_projections_to_S3 = LocalFilesystemToS3Operator(
    task_id='order_projections_output',
    filename=f'{AIRFLOW_HOME}/{compute_order_projections}',
    dest_key='output/{{ run_id }}/order_projections.csv',
    dest_bucket='tovala-data-cohort-model',
    replace=True
  )

  chain(compute_order_projections, order_projections_to_S3)
