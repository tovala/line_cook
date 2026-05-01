import os
from pendulum import duration
from airflow.sdk import dag

from common.chili import chili_params, chili_macros, chiliLoad

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

COLUMNS = '''
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated
'''

@dag(
  dag_id='chili_tovala_assist',
  schedule=None, # Triggered by chili dag
  catchup=False,
  max_active_runs=1,
  default_args={
    'retries': 2,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['internal', 'data-integration', 'chili'],
  params={
    **chili_params(table='tovala_assist',
                   stage='tovala_assist_stage',
                   columns=COLUMNS,
                   storage_integration='TOVALA_ASSIST_STORAGE_INTEGRATION',
                   s3_url='s3://cdn.tovala.com/assist/',
                   pattern=r'assist/(.*)recipes.(.*)json'
    )
  },
  template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates',
  user_defined_macros=chili_macros()
)
def tovalaAssistLoad():
  chiliLoad()

tovalaAssistLoad()
