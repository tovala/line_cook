import datetime

from pendulum import duration
from cosmos.operators.local import DbtSourceLocalOperator, DbtTestLocalOperator
from airflow.sdk import dag, Variable
from airflow.timetables.trigger import CronTriggerTimetable

from common.slack_notifications import bad_boy, good_boy
from common.dbt_cosmos_config import PROD_DBT_PROFILE_CONFIG, DBT_PROJECT_DIR, DBT_EXECUTABLE_PATH, DBT_WATCHER_EXECUTION_CONFIG
from common.dbt_custom_operators import runOperatorCustom

@dag(
    on_failure_callback=bad_boy,
    on_success_callback=good_boy,
    schedule=CronTriggerTimetable('0 5 * * *', timezone='America/Chicago'),
    start_date=datetime.datetime(2026, 1, 15),
    catchup=False,
    tags=['internal', 'dbt'],
    params={
        'channel_name': '#team-data-notifications'
    },
    render_template_as_native_obj=True
)

def compost():
    '''
    Runs a series of dbt operations for maintenance purposes
    '''
    # 1. Tear Down Testing
    # dbt run-operation clean_up_all_test --target test
    clean_up_test_schemas = runOperatorCustom.runInTest(
        task_id='clean_up_test_schemas',
        macro_name='clean_up_all_test',
    )

    # 2. Tear Down Old Models
    # dbt run-operation cleanup_old_models --target prod
    tear_down_old_models = runOperatorCustom.runInProd(
        task_id='tear_down_old_models',
        macro_name='cleanup_old_models',
    )

    # 3. Test Source Freshness
    #TODO: Output failures to Slack
    # dbt source freshness
    source_freshness = DbtSourceLocalOperator(
        task_id='source_freshness',
        profile_config=PROD_DBT_PROFILE_CONFIG,
        env={
            'SF_AWS_KEY': Variable.get('dbt_sf_aws_key'),
            'SF_AWS_SECRET': Variable.get('dbt_sf_aws_secret')
        },
        project_dir=DBT_PROJECT_DIR,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    )

    # 4. Test Harvest
    #TODO: Output failures to Slack
    # dbt test --selector harvest --target prod
    test_harvest = DbtTestLocalOperator(
        task_id='test_harvest',
        profile_config=PROD_DBT_PROFILE_CONFIG,
        execution_config=DBT_WATCHER_EXECUTION_CONFIG,
        env={
            'SF_AWS_KEY': Variable.get('dbt_sf_aws_key'),
            'SF_AWS_SECRET': Variable.get('dbt_sf_aws_secret')
        },
        project_dir=DBT_PROJECT_DIR,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        selector='harvest'
    )

compost()