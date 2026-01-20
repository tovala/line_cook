import datetime
import os
from typing import Any, List, Dict

from airflow.sdk import dag, task, chain
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
    @task()
    def generateParams(model_names: List[str], **kwargs) -> List[Dict[str, Dict[str, str]]]:
        '''
        Set up parameters to pass to create cohort models sql template for each model in the supplied list.
        Args:
            model_names (str): list of models to be created
        Output:
            expanded_kwargs (str): list of dicts that are formatted kwargs to pass to SQLExecuteQueryOperator 
        '''
        expanded_kwargs = []

        dag_params = kwargs['params']

        for model in model_names:
            params = {**dag_params, 'table_name': model, 'table_columns_file': f'queries/{model}.sql'}

            expanded_kwargs.append({'params': params})

        return expanded_kwargs
    
    @task()
    def getParamsForExternalStage(tables: List[str]) -> List[Dict[str, str]]:
        expanded_args = []

        for table in tables: 
            expanded_args.append({'table': f'CHILI_V2.{table}', 'pattern': f'.*{table}.*.parquet'})

        return expanded_args 

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

    generate_params = generateParams(MODELS)

    create_models = SQLExecuteQueryOperator.partial(
        task_id='create_cohort_models', 
        conn_id='snowflake', 
        sql='create_table.sql',
    ).expand_kwargs(generate_params)

    external_stage_params = getParamsForExternalStage(MODELS)

    copy_from_csv = CopyFromExternalStageToSnowflakeOperator.partial(
    task_id='copyTable', 
    snowflake_conn_id='snowflake',
    stage='MASALA.CHILI_V2.cohort_model_input_stage',
    file_format="(TYPE = 'csv')",
    ).expand_kwargs(external_stage_params)

    chain([test_schema, generate_params, cohort_model_input_stage], create_models)

cohortModel()