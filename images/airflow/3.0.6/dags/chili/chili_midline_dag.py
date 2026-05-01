import os
from pendulum import duration
from airflow.sdk import dag

from common.chili import chili_params, chili_macros, chiliLoad

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

# test_run_time parses the unix epoch out of the filename suffix (e.g. .../1701234567.json).
COLUMNS = '''
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated
  , CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_NTZ(CAST(REGEXP_SUBSTR(filename, '([0-9]+)\\\\.json$', 1, 1, 'e') AS BIGINT))) AS test_run_time
'''

@dag(
  dag_id='chili_midline',
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
    **chili_params(table='midline',
                   stage='midline_stage',
                   columns=COLUMNS,
                   storage_integration='MIDLINE_STORAGE_INTEGRATION',
                   s3_url='s3://oven-manufacturing-logs/',
                   pattern=r'(midline|(midline[1-9]))/.*\.json'
    )
  },
  template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates',
  user_defined_macros=chili_macros()
)
def midlineLoad():
  chiliLoad()

midlineLoad()
