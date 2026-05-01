import os
import datetime
from pendulum import duration
from cosmos import DbtTaskGroup, RenderConfig, LoadMode, TestBehavior

from airflow.sdk import dag, chain, Param
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from cohort_model.generate_aggregate_curves_task_group import generateAggregateRetentionCurves
from cohort_model.order_projections_task_group import orderProjections
from cohort_model.runtime_queries_task_group import runtimeQueries
from cohort_model.snapshot_to_s3_task_group import snapshotSnowflakeToS3
from common.dbt_cosmos_config import DBT_PROJECT_CONFIG, DBT_WATCHER_EXECUTION_CONFIG, PROD_DBT_PROFILE_CONFIG
from common.slack_notifications import slack_param
from common.sql_operator_handlers import fetch_single_result, fetch_results_array

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
    dag_id='cohort_model',
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # TODO: add schedule
    # schedule=MultipleCronTriggerTimetable('30 8 * * 1', '25 22 * * 3', timezone='America/Chicago'),
    catchup=False,
    default_args={
      'retries': 2,
      'retry_delay': duration(seconds=2),
      'retry_exponential_backoff': True,
      'max_retry_delay': duration(minutes=5),
    },
    tags=['internal'],
    params={
        'channel_name': slack_param(),
        'database': Param('MASALA', type='string', title='Snowflake Database Name'),
        'schema': Param('YARROW', type='string', title='Snowflake Source Schema', description='Static Non-DBT schema containing cohort model inputs. *NOTE: must exist'),
        'runtime_schema_prefix': Param('COHORT_MODEL', type='string', title='Runtime Snowflake Schema Prefix', description='To ensure unique schema name - format "{prefix}_{run_id}". Schema only exists for the duration of the run.'),
        # Snapshot Settings Params
        's3_url': Param('s3://tovala-data-cohort-model/output/', type='string', title= 'S3 Destination URL', section='Snapshot Settings'),
        'file_format': Param('parquet', type='string', enum=['parquet', 'csv', 'json'], title='Output File Format', description='File type of runtime table snapshots', section='Snapshot Settings'),
        'stage': Param('cohort_model_snapshot_stage', type='string', title='Snowflake External Stage Name', description='Name of external stage to use for snapshots. Will be created if does not already exist.', section='Snapshot Settings'),
        'storage_integration': Param('COHORT_MODEL_STORAGE_INTEGRATION', type='string', title='Snowflake/S3 Storage Integration.', description='*NOTE: must exist. See sous-chef repo for existing configured storage integrations.', section='Snapshot Settings'),
        # Default Assumptions Params
        'lookback_adjustment_window': Param(5, title='Correction Factor Lookback Window', type='number', section='Default Assumptions'),
        'longtail_weekly_retention_multiplier': Param(0.9945, title='Rentention Curve Longtail Weekly Multiplier', type='number', section='Default Assumptions'),
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
  '''
  run_mugwort = DbtTaskGroup(
    group_id='mugwort_cohort_model_inputs',
    project_config=DBT_PROJECT_CONFIG,
    profile_config=PROD_DBT_PROFILE_CONFIG,
    execution_config=DBT_WATCHER_EXECUTION_CONFIG,
    render_config=RenderConfig(
      selector='cohort_model_runtime',
      load_method=LoadMode.DBT_LS,
      enable_mock_profile=False,
      test_behavior=TestBehavior.AFTER_ALL
    ),
    operator_args={
      'py_system_site_packages': False,
      'py_requirements': ['dbt-snowflake'],
      'install_deps': True,
      'emit_datasets': False,
      'execution_timeout': datetime.timedelta(minutes=10),
    },
  )
  '''

  create_runtime_schema = SQLExecuteQueryOperator(
    task_id='create_runtime_schema',
    conn_id='snowflake',
    sql='CREATE SCHEMA IF NOT EXISTS {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}";'
  ).as_setup()

  create_agg_retention_curves = generateAggregateRetentionCurves()

  create_temp_queries = runtimeQueries(default_queries=['historical_meal_orders', 'actual_oven_sales'])

  start_term = SQLExecuteQueryOperator(
    task_id='get_projection_start_term',
    conn_id='snowflake',
    sql='SELECT MIN(term_id) FROM {{ params.database }}.mugwort.future_terms',
    handler=fetch_single_result
  )

  end_term = SQLExecuteQueryOperator(
    task_id='get_projection_end_term',
    conn_id='snowflake',
    sql='SELECT MAX(term_id) from {{ params.database }}.mugwort.combined_oven_sales',
    handler=fetch_single_result
  )

  projection_terms_array = SQLExecuteQueryOperator(
    task_id='get_projection_terms',
    conn_id='snowflake',
    sql='queries/projection_terms_array.sql',
    handler=fetch_results_array
  )

  run_order_projections = orderProjections(projection_terms_array.output)

  

  snapshot_inputs = snapshotSnowflakeToS3()
  
  delete_runtime_schema = SQLExecuteQueryOperator(
    task_id='drop_runtime_schema',
    conn_id='snowflake',
    sql='DROP SCHEMA {{ params.database }}."{{ params.runtime_schema_prefix }}_{{ run_id }}";'
  ).as_teardown()

  
  chain(create_runtime_schema, [create_temp_queries, create_agg_retention_curves], run_order_projections, snapshot_inputs, delete_runtime_schema)
  chain([start_term, end_term], projection_terms_array, run_order_projections)

    

cohortModel()