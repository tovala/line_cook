import os

from airflow.sdk import dag, chain, task_group, Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import slack_param


AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # TODO: add schedule
    # schedule=MultipleCronTriggerTimetable('30 8 * * 1', '25 22 * * 3', timezone='America/Chicago'),
    catchup=False,
    tags=['internal'],
    params={
        'channel_name': slack_param(),
        'database': Param('MASALA', type='string'),
        'schema_name': Param('COHORT_MODEL_INPUTS', type='string')
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

    MODELS = ['actual_oven_sales', 'combined_oven_sales', 'historical_meal_orders', 'projected_oven_sales', 'weekly_meal_and_meal_order_counts']
    
    #### Custom Task Definitions
    @task_group(group_id='create_temp_table')
    def createTempTable(model: str) -> None:
        create_empty_model = SQLExecuteQueryOperator(
            task_id='create_cohort_models', 
            conn_id='snowflake', 
            sql='create_table.sql',
            params={
                'table_name': model,
                'table_columns_file': f'queries/{model}/table_columns.sql'
            }
        )

        load_model_default = SQLExecuteQueryOperator(
            task_id='load_model_default', 
            conn_id='snowflake', 
            sql='create_table.sql',
            params={
                'table_name': model,
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
    
    create_temp_schema = SQLExecuteQueryOperator(
        task_id='create_temp_schema',
        conn_id='snowflake',
        sql='CREATE SCHEMA IF NOT EXISTS {{ params.schema_name }};'
    )

    create_temp_tables = createTempTable.expand(MODELS)

    # TODO: DROP temp schema

    drop_temp_schema = SQLExecuteQueryOperator(
        task_id='drop_temp_schema',
        conn_id='snowflake',
        sql='DROP SCHEMA {{ params.schema_name }};'
    )
    

    chain([create_temp_schema, cohort_model_input_stage], create_temp_tables, drop_temp_schema)

cohortModel()