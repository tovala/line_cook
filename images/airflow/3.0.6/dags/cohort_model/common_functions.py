import polars as pl
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def get_cohort_age_matrix(start_term: int, end_term: int ) -> pl.DataFrame:
  hook = SnowflakeHook(snowflake_conn_id='snowflake')
  conn = hook.get_conn()
  cursor = conn.cursor()

  # fetch cohort age by projection term
  cohort_age_matrix_arrow_table = cursor.execute(
    f'''WITH cohort_age AS (
          SELECT 
            'TERM_' || term_id AS term
            ,cohort
            , cohort_age
          FROM masala.mugwort.cohort_age
          WHERE cohort <= { end_term }
          AND cohort_age IS NOT NULL
          AND term_id BETWEEN { start_term } AND { end_term }
        )
        SELECT 
            *
        FROM cohort_age
        PIVOT 
            (MIN(cohort_age) FOR term IN (ANY ORDER BY term)) 
        ORDER BY cohort;'''
  ).fetch_arrow_all()

  cohort_age_matrix = pl.from_arrow(cohort_age_matrix_arrow_table)
  return cohort_age_matrix

def get_projected_order_counts_expr(longtail_value: float, col_regex: str):
  # set up conditional logic to get point from agg retention curve
  projected_order_counts_expr = pl.when(pl.col(col_regex) > 78).then((longtail_value ** (pl.col(col_regex).cast(pl.Int64) - 78)) * pl.col('AGG_WEEK_78') * pl.col('INIT_ORDER_VAL'))
  
  for week_num in range(79):
    projected_order_counts_expr = projected_order_counts_expr.when(pl.col(col_regex) == week_num).then(pl.col(f'AGG_WEEK_{week_num}') * pl.col('INIT_ORDER_VAL'))
  
  projected_order_counts_expr = projected_order_counts_expr.otherwise(pl.col(col_regex)).name.keep()
  return projected_order_counts_expr
