import datetime
from typing import Any, List, Dict

from airflow.sdk import dag, task, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from common.slack_notifications import bad_boy, good_boy
from common.macros.sql_templating import render_sql_from_file


@dag(
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # TODO: add schedule
    # schedule=MultipleCronTriggerTimetable('30 8 * * 1', '25 22 * * 3', timezone='America/Chicago'),
    catchup=False,
    tags=['internal'],
    params={
        'channel_name': ''
    },
)
def cohortModel():
    '''Cohort Model
    Description: 

    Schedule: TBD

    Dependencies:

    Variables:

    '''

    MODELS = ['historical_meal_orders']

    @task()
    def generateParams(model_names: List[str]) -> List[Dict[str, Dict[str, str]]]:
        '''
        Set up parameters to pass to create cohort models sql template for each model in the supplied list.
        Args:
            model_names (str): list of models to be created
        Output:
            expanded_kwargs (str): list of dicts that are formatted kwargs to pass to SQLExecuteQueryOperator 
        '''
        expanded_kwargs = []

        for model in model_names:
            params = {'table_name': model, 'table_columns_file': f'queries/{model}.sql'}

            expanded_kwargs.append({'params': params})

        return expanded_kwargs

    
    test_schema = SQLExecuteQueryOperator(
        task_id='create_test_schema',
        conn_id='snowflake',
        sql='CREATE SCHEMA IF NOT EXISTS TEST_TAYLOR_BRINE;'
    )

    generate_params = generateParams(MODELS)

    create_models = SQLExecuteQueryOperator.partial(
        task_id='create_cohort_models', 
        conn_id='snowflake', 
        sql='templates/create_table.sql',
    ).expand_kwargs(generate_params)

    chain([test_schema, generate_params], create_models)

cohortModel()