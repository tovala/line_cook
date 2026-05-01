import os
from pendulum import duration
from airflow.sdk import dag

from common.chili import chili_params, chili_macros, chiliLoad

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

# upload_time regex parses YYYY-MM-DD HH:MM:SS out of the filename.
# term_id / cycle_id / facility_network are also derived from the filename.
COLUMNS = '''
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated
  , TRY_TO_TIMESTAMP(REGEXP_SUBSTR(filename, '\\\\d{4}-\\\\d{2}-\\\\d{2}') || ' ' || REGEXP_SUBSTR(filename, '\\\\d{2}:\\\\d{2}:\\\\d{2}')) AS upload_time
  , REGEXP_REPLACE(REGEXP_SUBSTR(filename, 'term\\\\d{3,4}'), 'term', '')::INTEGER AS term_id
  , REGEXP_REPLACE(REGEXP_SUBSTR(filename, 'cycle[1|2]'), 'cycle', '')::INTEGER AS cycle_id
  , REGEXP_SUBSTR(filename, 'chicago|slc') AS facility_network
'''

@dag(
  dag_id='chili_shipment_file',
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
    **chili_params(table='shipment_file',
                   stage='shipment_file_stage',
                   columns=COLUMNS,
                   storage_integration='SHIPMENT_FILE_STORAGE_INTEGRATION',
                   s3_url='s3://tovala-data-engineering/shipment_file/',
                   file_format_name='json_gzip_file_format',
                   pattern=r'shipment_file/.*master\.gz'
    )
  },
  template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates',
  user_defined_macros=chili_macros()
)
def shipmentFileLoad():
  chiliLoad(file_format_options='COMPRESSION = GZIP')

shipmentFileLoad()
