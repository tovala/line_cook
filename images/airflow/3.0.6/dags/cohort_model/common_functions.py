import polars as pl
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def get_cohort_age_matrix(start_term: int, end_term: int, **kwargs) -> pl.DataFrame:
  hook = SnowflakeHook(snowflake_conn_id='snowflake')
  conn = hook.get_conn()
  cursor = conn.cursor()

  lookback_window = kwargs.get('lookback_window')

  # default - get cohort ages for all terms within the projection window
  sql_query = f'''SELECT 
                  'TERM_' || term_id AS term
                  , cohort
                  , cohort_age
                FROM masala.mugwort.cohort_age
              WHERE cohort <= { end_term }
              AND cohort_age IS NOT NULL
              AND term_id BETWEEN { start_term } AND { end_term }
              ORDER BY ALL;'''
  
  # if lookback_window kwarg is passed, only get cohort age matrix for the lookback window
  if lookback_window:
    sql_query = f'''WITH all_cohorts AS (
                      SELECT 
                          cohort
                          , offset_without_holidays 
                      FROM wash.cohorts 
                      UNION
                      SELECT 
                          cohort
                          , offset_without_holidays 
                      FROM mugwort.future_cohorts
                      WHERE cohort <= { end_term }
                    )
                    , projection_terms AS (
                        SELECT
                            term_id
                            , week_without_holidays
                        FROM wash.cohorts
                        WHERE term_id < { start_term }
                        ORDER BY term_id DESC
                        FETCH FIRST { lookback_window } ROWS
                    )
                    SELECT 
                      'TERM_' || pt.term_id AS term 
                      , c.cohort
                      , pt.week_without_holidays + c.offset_without_holidays - 1 AS cohort_age
                    FROM projection_terms pt
                    CROSS JOIN all_cohorts c
                    ORDER BY ALL;'''
  
  cohort_age_matrix_arrow_table = cursor.execute(sql_query).fetch_arrow_all()

  cohort_ages = pl.from_arrow(cohort_age_matrix_arrow_table)
  cohort_age_matrix = cohort_ages.pivot('TERM', index='COHORT', values='COHORT_AGE')
  return cohort_age_matrix

def get_projected_order_counts_expr(longtail_value: float, col_regex: str):
  # set up conditional logic to get point from agg retention curve
  projected_order_counts_expr = pl.when(pl.col(col_regex) > 78).then((longtail_value ** (pl.col(col_regex).cast(pl.Int64) - 78)) * pl.col('AGG_WEEK_78') * pl.col('INIT_ORDER_VAL'))
  
  for week_num in range(79):
    projected_order_counts_expr = projected_order_counts_expr.when(pl.col(col_regex) == week_num).then(pl.col(f'AGG_WEEK_{week_num}') * pl.col('INIT_ORDER_VAL'))
  
  projected_order_counts_expr = projected_order_counts_expr.otherwise(pl.col(col_regex)).name.keep()
  return projected_order_counts_expr
