import os
from pendulum import duration
from airflow.sdk import dag

from common.chili import chili_params, chili_macros, chiliLoad

AIRFLOW_HOME = os.environ["AIRFLOW_HOME"]

# upload_time regex parses YYYY-MM-DD HH:MM:SS out of the filename.
COLUMNS = '''
  TRY_PARSE_JSON($1) AS raw_data
  , METADATA$FILENAME AS filename
  , CURRENT_TIMESTAMP()::TIMESTAMPTZ AS updated
  , TRY_TO_TIMESTAMP(REGEXP_SUBSTR(filename, '\\\\d{4}-\\\\d{2}-\\\\d{2}') || ' ' || REGEXP_SUBSTR(filename, '\\\\d{2}:\\\\d{2}:\\\\d{2}')) AS upload_time
  , raw_data:termID::INTEGER AS term_id
  , raw_data:runID::INTEGER AS run_id
  , raw_data:userStatesIncluded::STRING AS included_user_states
  , raw_data:summaryCreationStartTime::TIMESTAMPTZ AS summary_created_at
  , raw_data:users AS users_json
  , raw_data:users:availableOffers AS offers_json
'''

@dag(
  dag_id='chili_marketing_incentives',
  schedule=None, # Triggered by chili dag
  catchup=False,
  default_args={
    'retries': 2,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['internal', 'data-integration', 'chili'],
  params={
    **chili_params(table='marketing_incentives_logs',
                   stage='marketing_incentives_stage',
                   columns=COLUMNS,
                   storage_integration='MARKETING_INCENTIVES_STORAGE_INTEGRATION',
                   s3_url='s3://tovala-software-data-release/offeravailability/'
    )
  },
  template_searchpath=f'{AIRFLOW_HOME}/dags/common/templates',
  user_defined_macros=chili_macros()
)
def marketingIncentivesLoad():
  chiliLoad()

marketingIncentivesLoad()
