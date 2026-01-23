import datetime
import os 

from pendulum import duration

from common.slack_notifications import bad_boy, good_boy
from common.dbt_cosmos_config import dbt_project_config, dbt_profile_config, dbt_execution_config
from cosmos import DbtDag, RenderConfig, LoadMode 


ds_weekly_config = RenderConfig(
  selector='weekly_ds_run',
  load_method=LoadMode.DBT_LS,
  enable_mock_profile=False
)

dbt_run = DbtDag(
  dag_id='weeklyDsRun',
  project_config=dbt_project_config,
  profile_config=dbt_profile_config,
  execution_config=dbt_execution_config,
  render_config=ds_weekly_config ,
  operator_args={
        "py_system_site_packages": False,
        "py_requirements": ["dbt-snowflake"],
        "install_deps": True,
        "emit_datasets": False,
        "execution_timeout": datetime.timedelta(minutes=10)
  }, 
  # Normal dag parameters 
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  schedule = '0 21 * * 4', ## (non DST) 3 PM US/Central on Thursday
  ## schedule = '0 20 * * 4' ## (DST) 3 PM US/Central on Thursday
  start_date=datetime.datetime(2026, 1, 15),
  catchup=False,
  default_args={
    "retries": 2,
    "retry_delay": duration(seconds=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": duration(minutes=5),
  },
    tags=['data_science']
)

