import datetime

from pendulum import duration
from cosmos import DbtTaskGroup, RenderConfig, LoadMode, TestBehavior, DbtRunOperationLocalOperator
from airflow.sdk import dag, chain, Variable
from airflow.timetables.trigger import CronTriggerTimetable

from common.slack_notifications import bad_boy, good_boy
from common.dbt_cosmos_config import DBT_PROJECT_CONFIG, DBT_WATCHER_EXECUTION_CONFIG, PROD_DBT_PROFILE_CONFIG, TEST_DBT_PROFILE_CONFIG, DBT_PROJECT_DIR, DBT_EXECUTABLE_PATH

@dag(
#   on_failure_callback=bad_boy,
#   on_success_callback=good_boy,
    # schedule=CronTriggerTimetable("0 5 * * *", timezone="America/Chicago"),
    start_date=datetime.datetime(2026, 1, 15),
    catchup=False,
    # default_args={
    #     'retries': 2,
    #     'retry_delay': duration(seconds=2),
    #     'retry_exponential_backoff': True,
    #     'max_retry_delay': duration(minutes=5),
    # },
    tags=['internal'],
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
    # dbt run-operation clean_up_all_test --profiles-dir $PWD/.dbt --target test
    clean_up_test_schemas = DbtRunOperationLocalOperator(
        task_id='clean_up_test_schemas',
        # This is intentionally set to test so it cleans up test schemas 
        profile_config=TEST_DBT_PROFILE_CONFIG, 
        env={
            'SF_AWS_KEY': Variable.get('dbt_sf_aws_key'),
            'SF_AWS_SECRET': Variable.get('dbt_sf_aws_secret')
        },
        project_dir=DBT_PROJECT_DIR,
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        macro_name='clean_up_all_test',
     )

#   build_weekly_ds_models = DbtTaskGroup(
#     group_id='weekly_ds_run',
#     project_config=DBT_PROJECT_CONFIG,
#     profile_config=PROD_DBT_PROFILE_CONFIG,
#     execution_config=DBT_WATCHER_EXECUTION_CONFIG,
#     render_config=RenderConfig(
#       selector='weekly_ds_run',
#       load_method=LoadMode.DBT_LS,
#       enable_mock_profile=False,
#       test_behavior=TestBehavior.AFTER_ALL
#     ),
#     operator_args={
#       'py_system_site_packages': False,
#       'py_requirements': ['dbt-snowflake'],
#       'install_deps': True,
#       'emit_datasets': False,
#       'execution_timeout': datetime.timedelta(minutes=10),
#     },
#   )
    pass 

compost()