import os
from pendulum import duration

from airflow.sdk import dag, chain, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.slack_notifications import slack_param, good_boy, bad_boy

from cohort_model.yarrow.extended_daily_oven_sales_projections_task_group import extendedDailyOvenSalesProjections
from cohort_model.yarrow.cohort_mix_projections_task_group import cohortMixProjections
from cohort_model.yarrow.create_default_inputs_from_csv_task_group import defaultInputsFromCSV


AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
    dag_id='cohort_model_yarrow_tables',
    on_failure_callback=bad_boy,
    on_success_callback=good_boy,
    schedule=None, # This dag will only run when manually triggered (or eventually when triggered by file upload to S3)
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': duration(seconds=2),
        'retry_exponential_backoff': True,
        'max_retry_delay': duration(minutes=5),
    },
    tags=['internal', 'cohort-model'],
    params={
      'channel_name': slack_param(),
      'database': Param('MASALA', type='string'),
      'schema': Param('YARROW', type='string'),
      'stage': Param('cohort_model_inputs_stage', type='string'),
      's3_url': Param('s3://tovala-data-cohort-model/input/', type='string'),
      'storage_integration': Param('COHORT_MODEL_STORAGE_INTEGRATION', type='string'),
      'file_format': Param('CSV', type='string'),
      'file_format_name': Param('yarrow_csv_file_format', type='string'),
      'cohort_characteristics': Param(['purchase_month', 'on_commitment', 'price_bucket'], type='array')
    },
    template_searchpath = f'{AIRFLOW_HOME}/dags/common/templates'
)
def cohortModelDefaultInputs():
  '''Retention Curves

  Description: Current State for V1. Pull manual retention curve csv from S3, create aggregate retention curve for each cohort.

  Schedule: TBD

  Dependencies:

  Variables:

  '''
  create_file_format = SQLExecuteQueryOperator(
    task_id='create_file_format',
    conn_id='snowflake',
    sql='create_file_format.sql',
    params={
      'file_format_options': 'PARSE_HEADER = true TRIM_SPACE = true ERROR_ON_COLUMN_COUNT_MISMATCH = false'
    }
  )

  create_cohort_model_inputs_stage = SQLExecuteQueryOperator(
    task_id='create_cohort_model_inputs_stage', 
    conn_id='snowflake', 
    sql='create_stage.sql'
  )

  create_extended_sales_prediction_table = extendedDailyOvenSalesProjections(
    table='extended_daily_oven_sales_projections',
    table_columns_file='queries/extended_daily_sales_projections/table_columns.sql'
  )

  create_cohort_characteristics_projections = cohortMixProjections()

  create_six_week_attach_rates = defaultInputsFromCSV('OVEN_SALES_SIX_WEEK_ATTACH_RATES', 'six_week_attach_rates.csv')
  create_meals_per_order_assumptions = defaultInputsFromCSV('OVEN_SALES_MEALS_PER_ORDER_ASSUMPTIONS', 'meals_per_order_assumptions.csv')
  


  chain(create_file_format, create_cohort_model_inputs_stage, [create_extended_sales_prediction_table, create_cohort_characteristics_projections, create_six_week_attach_rates, create_meals_per_order_assumptions])

    


cohortModelDefaultInputs()