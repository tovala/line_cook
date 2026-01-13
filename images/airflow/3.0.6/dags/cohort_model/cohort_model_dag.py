import datetime

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from common.slack_notifications import bad_boy, good_boy
from typing import Any, List, Dict

@dag(
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # TODO: add schedule
    # schedule=MultipleCronTriggerTimetable('30 8 * * 1', '25 22 * * 3', timezone='America/Chicago'),
    catchup=False,
    tags=['internal'],
    params={
        'channel_name': '#team-data-notifications'
    }
)
def cohort_model():
    '''Cohort Model
    Description: Runs preparatory steps for Simple Autofill:

    Schedule: TBD

    Dependencies:

    Variables:

    '''

    MODELS = ['historical_meal_orders']

    def generate_create_statement(table_name, table_schema='brine'):
        return f"""
            CREATE OR REPLACE TABLE {table_schema}.{table_name} ();
        """

    @task()
    def processModels(tables: List[str]) -> List[Dict[str, str]]:
        expanded_args = []

        for table in tables: 
            create_sql=generate_create_statement(table)
            # expanded_args.append({'table_name': f'{table}', 'create_sql' : create_sql})
            expanded_args.append[create_sql]

        return expanded_args 

    process_models = processModels(MODELS)

    create_tables = SQLExecuteQueryOperator.partial(
        task_id='create_cohort_model_tables',
        conn_id='snowflake',
    ).expand(sql=process_models)

    # table_columns = '''
    #     term_id INTEGER
    #     , cohort INTEGER 
    #     , order_count INTEGER 
    #     , meal_count INTEGER
    # '''

    create_models = SQLExecuteQueryOperator(
        task_id='create_cohort_models', 
        conn_id='snowflake', 
        sql='templates/create_table.sql',
        parameters={'table_name': 'historical_meal_orders'},
    )

cohort_model()