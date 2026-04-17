from typing import List, Dict
import ast
import polars as pl

from airflow.sdk import task_group, chain, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.sql_operator_handlers import fetch_results_array


@task_group(group_id='order_projections')
def orderProjections() -> None:
    
  @task
  def computeOrderProjections(projection_terms_array: List[int], cohort_age_dict: Dict[str, List[int]], **context):
    dag_params = context['params']
    database = dag_params.get('database')
    runtime_schema = dag_params.get('runtime_schema_prefix') + '_' + context['run_id']
    lookback_window = dag_params.get('lookback_adjustment_window')
    longtail_value = dag_params.get('longtail_weekly_retention_multiplier')


    hook = SnowflakeHook(snowflake_conn_id='snowflake')
    conn = hook.get_conn()
    cursor = conn.cursor()

    cohort_age_matrix = pl.DataFrame(cohort_age_dict).transpose(include_header=True, header_name='COHORT', column_names = projection_terms_array).cast({'COHORT': pl.Int64})

    # Set Up dataframes from Snowflake Queries
    # fetch aggregate order retention curve generated for this run as a DataFrame
    agg_order_retention_curves_arrow_table = cursor.execute(f'''SELECT *
    FROM { database }."{ runtime_schema }".AGGREGATE_ORDER_RETENTION_CURVES;
    ''').fetch_arrow_all()

    agg_order_retention_curves = pl.from_arrow(agg_order_retention_curves_arrow_table)

    # fetch inital order values by cohort
    init_order_val_arrow_table = cursor.execute(f'''SELECT 
      cohort
      ,order_count AS init_order_val 
    FROM { database }."{ runtime_schema }".HISTORICAL_MEAL_ORDERS
    WHERE term_id = cohort
    UNION
    SELECT 
      cohort
      ,init_order_val 
    FROM { database }."{ runtime_schema }".FUTURE_COHORT_INITIAL_ORDER_PREDICTIONS
    ORDER BY cohort;
    ''').fetch_arrow_all()

    inital_order_values_by_cohort_matrix = pl.from_arrow(init_order_val_arrow_table)

    # fetch cohort ages during the lookback window terms
    lookback_cohort_age_arrow_table  = cursor.execute(f'''WITH lookback_cohort_age AS (
    SELECT 
      cohort
      , term_id
      , cohort_age
    FROM { database }."{ runtime_schema }".cohort_age
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY term_id DESC)<= { lookback_window }
    )
    SELECT 
      *
    FROM lookback_cohort_age PIVOT (MIN(cohort_age) FOR term_id IN (ANY ORDER BY term_id)) ORDER BY cohort;
    ''').fetch_arrow_all()

    lookback_cohort_age_matrix = pl.from_arrow(lookback_cohort_age_arrow_table)
      
    lookback_all_data = pl.concat([lookback_cohort_age_matrix, agg_order_retention_curves, inital_order_values_by_cohort_matrix], how='align')
    
    lookback_projected_order_counts = lookback_all_data.select(
      pl.col('COHORT')
      ,get_projected_order_counts_expr(longtail_value=longtail_value, col_regex='^\d+$')
    ).select(
      pl.col('COHORT')
      ,PROJECTED_ORDER_COUNTS_TOTAL = pl.sum_horizontal(pl.exclude('COHORT'))
    )

    # Calculate Correction Factor
    # fetch total order count within lookback window
    lookback_order_sum_arrow_table = cursor.execute(f'''WITH lookback as (
    SELECT 
      *
    FROM { database }."{ runtime_schema}".historical_meal_orders qualify ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY term_id desc)<= { lookback_window })
    SELECT 
        cohort
        ,CASE WHEN COUNT(term_id) = { lookback_window } 
          THEN SUM(order_count)
          ELSE NULL 
        END AS actual_order_counts_total
    FROM lookback GROUP BY cohort ORDER BY cohort;
    ''').fetch_arrow_all()

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
    skip_adjustment_arrow_table = cursor.execute(f'''SELECT DISTINCT
      'TERM_' || term_id::STRING AS term_id
      , 1 + holiday_skip_adjustment AS holiday_skip_multiplier
    FROM { database }.mugwort.skip_adjustments 
    WHERE term_id BETWEEN 
      (SELECT MIN(term_id) FROM { database }.mugwort.future_terms) -- cohort model prediction starting term (first future term)
    AND 
      (SELECT MAX(term_id) FROM { database }.mugwort.combined_oven_sales) -- cohort model prediction ending term (last term with predicted oven sales)
    ORDER BY term_id;
    ''').fetch_arrow_all()

    skip_adjustment_matrix = pl.from_arrow(skip_adjustment_arrow_table)

    print(skip_adjustment_matrix)
    print(correction_factor_matrix)
    print(projected_order_counts)
    
    projected_order_counts_transpose = projected_order_counts.select(
      pl.when(pl.col('^TERM_\d+$') < 0).then(None).otherwise(pl.col('^TERM_\d+$')).name.keep()
    ).transpose() 
    order_projections_skip_adj = (projected_order_counts_transpose * skip_adjustment_matrix.get_column('HOLIDAY_SKIP_MULTIPLIER')).transpose(column_names=projection_terms_array)
    order_projections_corrected = order_projections_skip_adj.select(pl.col('^TERM_\d+$')) * correction_factor_matrix.get_column('CORRECTION_FACTOR')

    final_order_projections = pl.concat([projected_order_counts.select(pl.col('COHORT')), order_projections_corrected], how='horizontal')
    
    print(final_order_projections)
    final_order_projections_csv = final_order_projections.write_csv()

    return final_order_projections_csv
    

  projection_terms_array = SQLExecuteQueryOperator(
    task_id='get_projection_terms',
    conn_id='snowflake',
    sql='queries/dataframes/projection_terms_vector.sql',
    handler=fetch_results_array
  )

  cohort_age_dict = SQLExecuteQueryOperator(
    task_id='get_cohort_age_dict',
    conn_id='snowflake',
    sql='queries/dataframes/future_term_cohort_age_matrix.sql',
    params={
      'projection_terms_array': projection_terms_array.output
    },
    handler=fetch_all_future_term_cohort_age
  )

  compute_order_projections = computeOrderProjections(projection_terms_array.output, cohort_age_dict.output)

  chain(projection_terms_array, cohort_age_dict, compute_order_projections)


### Helper Fns ###
# Handler for retrieving a Dict[int, List[int]] representing the matrix of cohort age for all terms being projected
def fetch_all_future_term_cohort_age(cursor):
  cohort_future_term_age_dict = {}
  results = cursor.fetchall()
  for row in results:
    key = row[0]
    val_array = ast.literal_eval(row[1])
    cohort_future_term_age_dict |= {key: val_array}

  return cohort_future_term_age_dict


def get_projected_order_counts_expr(longtail_value: float, col_regex: str):
  # set up conditional logic to get point from agg retention curve
  projected_order_counts_expr = pl.when(pl.col(col_regex) > 78).then((longtail_value ** (pl.col(col_regex).cast(pl.Int64) - 78)) * pl.col('AGG_WEEK_78') * pl.col('INIT_ORDER_VAL'))
  
  for week_num in range(79):
    projected_order_counts_expr.when(pl.col(col_regex) == week_num).then(pl.col(f'AGG_WEEK_{week_num}') * pl.col('INIT_ORDER_VAL'))
  
  projected_order_counts_expr = projected_order_counts_expr.otherwise(pl.col(col_regex)).name.keep()

  return projected_order_counts_expr