import datetime
import os
from typing import Any, List, Dict

from airflow.sdk import dag, task, chain, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

from common.slack_notifications import bad_boy, good_boy

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # TODO: add schedule
    # schedule=MultipleCronTriggerTimetable('30 8 * * 1', '25 22 * * 3', timezone='America/Chicago'),
    catchup=False,
    tags=['internal'],
    params={
        'channel_name': '',
        'parent_database': 'MASALA',
        'schema_name': 'TEST_TAYLOR_BRINE'
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

    MODELS = ['historical_meal_orders']
    
    #### Custom Task Definitions
    @task_group(group_id='create_temp_table')
    def createTempTable(model: str) -> None:
        create_model = SQLExecuteQueryOperator.partial(
            task_id='create_cohort_models', 
            conn_id='snowflake', 
            sql='create_table.sql',
            params={
                'table_name': model,
                'table_columns_file': f'queries/{model}.sql'
            }
        )

        copy_from_csv = CopyFromExternalStageToSnowflakeOperator.partial(
        task_id='copyTable', 
        snowflake_conn_id='snowflake',
        stage='MASALA.CHILI_V2.cohort_model_input_stage',
        file_format="(TYPE = 'csv')",
        params={
            'table': f'CHILI_V2.{model}',
            'pattern': f'.*{model}.*.parquet'
        }
        )

        # TODO: Add S3 override here - maybe add an override param? like a list of models w override - set comparison to standard list to avoid doing both?
        chain(create_model, copy_from_csv)

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
    
    test_schema = SQLExecuteQueryOperator(
        task_id='create_test_schema',
        conn_id='snowflake',
        sql='CREATE SCHEMA IF NOT EXISTS {{ params.schema_name }};'
    )

    create_temp_tables = createTempTable.expand(MODELS)

    

    chain([test_schema, cohort_model_input_stage], create_temp_tables)

cohortModel()