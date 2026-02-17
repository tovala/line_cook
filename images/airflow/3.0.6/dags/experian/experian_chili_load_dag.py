import os
from pendulum import duration

from airflow.sdk import dag

from common.slack_notifications import bad_boy, good_boy, slack_param
from common.chili import chili_params, chili_macros, chiliLoad
from experian.chili_vars import COLUMNS

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

@dag(
  dag_id='experian_load_to_chili',
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  schedule=None, # Externally triggered by experian_extraction_dag
  catchup=False,
  default_args={
    'retries': 2,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['internal', 'data-integration', 'chili'],
  params={
    'channel_name': slack_param(),
    **chili_params(table='experian_customers',
                   stage='experian_stage',
                   columns=COLUMNS,
                   storage_integration='EXPERIAN_STORAGE_INTEGRATION',
                   s3_url='s3://tovala-data-experian/',
    )
  },
  template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates',
  user_defined_macros=chili_macros()
)
def experianLoad():
  chiliLoad()

experianLoad()