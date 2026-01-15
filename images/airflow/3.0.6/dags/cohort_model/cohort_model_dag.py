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
    user_defined_macros={
        "render_sql_from_file": render_sql_from_file,
    }
)
def cohort_model():
    '''Cohort Model
    Description: 

    Schedule: TBD

    Dependencies:

    Variables:

    '''

    MODELS = ['historical_meal_orders']
    test_schema = SQLExecuteQueryOperator(
        task_id='create_test_schema',
        conn_id='snowflake',
        sql='CREATE SCHEMA IF NOT EXISTS TEST_TAYLOR_BRINE;'
    )
    create_models = SQLExecuteQueryOperator(
        task_id='create_cohort_models', 
        conn_id='snowflake', 
        sql='templates/create_table.sql',
        params={'table_name': 'historical_meal_orders', 'table_columns_file': 'queries/historical_meal_orders.sql'},
    )

    chain(test_schema, create_models)

cohort_model()