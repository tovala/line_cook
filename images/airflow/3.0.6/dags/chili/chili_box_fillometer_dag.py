import os
from pendulum import duration
from airflow.sdk import dag

from common.chili import chili_params, chili_macros, chiliLoad

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

#CHANGE
COLUMNS = '''
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated
'''

@dag(
  dag_id='chili_box_fillometer', #CHANGE
  schedule=None, # Triggered by chili dag
  catchup=False,
  default_args={
    'retries': 2,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['internal', 'data-integration', 'chili'],
  params={ #CHANGE
    **chili_params(table='box_fillometer_logs',
                   stage='box_fillometer_stage',
                   columns=COLUMNS,
                   storage_integration='BOX_FILLOMETER_STORAGE_INTEGRATION',
                   s3_url='s3://misevala/prod/box_fillometer/order_boxes/',
                   file_format_name='json_strip_outer_array_file_format',
                   pattern=r'prod/box_fillometer/order_boxes/([0-9]+)-([A-Za-z0-9]+)-cycle([0-9]+)\.orders\.json'
    )
  },
  template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates',
  user_defined_macros=chili_macros()
)
def boxFillometerLoad(): #CHANGE
  chiliLoad(file_format_options='STRIP_OUTER_ARRAY = TRUE')

boxFillometerLoad() #CHANGE
