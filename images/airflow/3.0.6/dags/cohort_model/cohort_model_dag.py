import os

from airflow.sdk import dag, chain, task_group, Param, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import slack_param
from cohort_model.snapshot_to_s3_task_group import snapshotSnowflakeToS3
from cohort_model.default_inputs import LOOKBACK_ADJUSTMENT_WINDOW, LONGTAIL_WEEKLY_RETENTION_MULTIPLIER
from cohort_model.cohort_model_params import mealsPerOrderAssumptionsParam, sixWeekAttachRateParam


AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
    dag_id='cohort_model',
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # TODO: add schedule
    # schedule=MultipleCronTriggerTimetable('30 8 * * 1', '25 22 * * 3', timezone='America/Chicago'),
    catchup=False,
    tags=['internal'],
    params={
        'channel_name': slack_param(),
        'database': Param('MASALA', type='string'),
        'schema': Param('YARROW', type='string'),
        'runtime_schema': Param('COHORT_MODEL_TEMP', type='string'),
        'refresh_cohort_mix_projections': Param(False, type='boolean'),
        'refresh_retention_curves': Param(False, type='boolean'),
        'lookback_adjustment_window': Param(LOOKBACK_ADJUSTMENT_WINDOW, type='number'),
        'longtail_weekly_retention_multiplier': Param(LONGTAIL_WEEKLY_RETENTION_MULTIPLIER, type='number'),
        'meals_per_order_d2c_not_holiday': mealsPerOrderAssumptionsParam('D2C Not Holiday', 6.200),
        'meals_per_order_d2c_holiday': mealsPerOrderAssumptionsParam('D2C Holiday', 5.680),
        'meals_per_order_sale': mealsPerOrderAssumptionsParam('Sale', 6.000),
        'meals_per_order_amazon': mealsPerOrderAssumptionsParam('Amazon', 5.200),
        'meals_per_order_costco': mealsPerOrderAssumptionsParam('Costco', 5.000),
        'meals_per_order_other': mealsPerOrderAssumptionsParam('Other', 5.200),
        'six_week_attach_rates_d2c_not_holiday': sixWeekAttachRateParam('D2C Not Holiday', [0.721, 0.188, 0.026, 0.010, 0.010, 0.000]),
        'six_week_attach_rates_d2c_holiday': sixWeekAttachRateParam('D2C Holiday', [0.704, 0.109, 0.061, 0.023, 0.029, 0.021]),
        'six_week_attach_rates_sale': sixWeekAttachRateParam('Sale', [0.704, 0.109, 0.061, 0.023, 0.029, 0.021]),
        'six_week_attach_rates_amazon': sixWeekAttachRateParam('Amazon', [0.000, 0.000, 0.200, 0.050, 0.000, 0.000]),
        'six_week_attach_rates_costco': sixWeekAttachRateParam('Costco', [0.000, 0.000, 0.150, 0.100, 0.000, 0.000]),
        'six_week_attach_rates_other': sixWeekAttachRateParam('Other', [0.000, 0.000, 0.200, 0.050, 0.000, 0.000]),
    },
    template_searchpath = f'{AIRFLOW_HOME}/dags/common/templates'
)
def cohortModel():
  '''Cohort Model
  Description: 

  Schedule: TBD

  Dependencies:

  Variables:

  '''

  INPUT_MODELS = ['actual_oven_sales', 'combined_oven_sales', 'historical_meal_orders', 'projected_oven_sales', 'weekly_meal_and_meal_order_counts']
  CSV_INPUTS = ['cohort_mix_projections', 'daily_oven_d2c_sales_splits', 'daily_oven_sales_projections', 'meal_retention_curves', 'model_order_retention_curves']
    
  #### Custom Task Definitions
  @task_group(group_id='create_temp_table')
  def createTempTable(model: str) -> None:
    create_empty_model = SQLExecuteQueryOperator(
      task_id='create_cohort_models', 
      conn_id='snowflake', 
      sql='create_table.sql',
      params={
        'table': model,
        'table_columns_file': f'queries/{model}/table_columns.sql'
      }
    )

    load_model_default = SQLExecuteQueryOperator(
      task_id='load_model_default', 
      conn_id='snowflake', 
      sql='create_table.sql',
      params={
        'table': model,
        'table_columns_file': f'queries/{model}/default_input.sql'
      }
    )
          
    # TODO: Add S3 override here - maybe add an override param? like a list of models w override - set comparison to standard list to avoid doing both?
    # copy_from_csv = CopyFromExternalStageToSnowflakeOperator.partial(
    #   task_id='copyTable', 
    #   snowflake_conn_id='snowflake',
    #   stage='MASALA.CHILI_V2.cohort_model_input_stage',
    #   file_format="(TYPE = 'csv')",
    #   params={
    #       'table': f'CHILI_V2.{model}',
    #       'pattern': f'.*{model}.*.parquet'
    #   }
    # )

    chain(create_empty_model, load_model_default)

  #### Task Instances
  cohort_model_input_stage = SQLExecuteQueryOperator(
    task_id='createCohortModelInputStage', 
    conn_id='snowflake', 
    sql='create_stage.sql',
    params={
      'parent_database': 'MASALA',
      'schema_name': 'CHILI_V2',
      'stage_name': 'cohort_model_input_stage',
      'url': 's3://tovala-data-cohort-model/input/',
      'storage_integration': 'COHORT_MODEL_STORAGE_INTEGRATION',
      'file_type': 'csv',
    },
  )

  ## TODO: add retention curve task group here - use the cohort model input stage - if refresh flag = True

  ## TODO: add cohort mix projection task group here - use cohort model input stage - if refresh flag = True

  @task.branch
  def refreshManualInputs(context):
    # TODO: set up logic for when to trigger the load inputs task groups
    pass



  create_temp_schema = SQLExecuteQueryOperator(
    task_id='create_temp_schema',
    conn_id='snowflake',
    sql='CREATE SCHEMA IF NOT EXISTS {{ params.schema }};'
  )

  #create_temp_tables = createTempTable.expand(model=INPUT_MODELS)

  # TODO: pull input csvs from S3 to tables in temp schema
  #copy_from_csv = CopyFromExternalStageToSnowflakeOperator.partial(
  #task_id='copyTable', 
  #  snowflake_conn_id='snowflake',
  #  stage='MASALA.CHILI_V2.cohort_model_input_stage',
  #  file_format="(TYPE = 'csv')",
  #  params={
  #    'table': f'CHILI_V2.{model}',
  #    'pattern': f'.*{model}.*.parquet'
  #  }
  #)

  #snapshot_inputs = snapshotSnowflakeToS3()

  drop_temp_schema = SQLExecuteQueryOperator(
    task_id='drop_temp_schema',
    conn_id='snowflake',
    sql='DROP SCHEMA {{ params.schema }};'
  )
    

  chain([create_temp_schema, cohort_model_input_stage], drop_temp_schema)

cohortModel()