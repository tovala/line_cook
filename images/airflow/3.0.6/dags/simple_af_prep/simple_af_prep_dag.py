import datetime

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from common.slack_notifications import bad_boy, good_boy, getSlackChannelNameParam
from typing import Any, List

@dag(
    on_failure_callback=bad_boy,
    on_success_callback=good_boy,
    schedule=MultipleCronTriggerTimetable("30 8 * * 1", "25 22 * * 3", timezone="America/Chicago"),
    catchup=False,
    tags=['internal'],
    params={
        'channel_name': getSlackChannelNameParam('#autofill-notifications')
    }
)
def simple_af_prep():
  '''Simple AF Prep
  Description: Runs preparatory steps for Simple Autofill:
  1. Populates brine.weekly_meal_counts
  2. Populates brine.anticombinations

  Schedule: Twice weekly at 8:30 AM Monday, 10:25 PM Wednesday CST

  Dependencies:

  Variables:

  '''
  @task
  def fetch_results(data: List[tuple[int,Any]]) ->  int:
    '''
    This is a pass-through function that converts the tuple returned by the SQLExecuteQueryOperator into a parseable List/tuple.
    '''
    return data[0][0]

  # 1. Load new anticombinations into brine.anticombinations  
  load_anticombos = SQLExecuteQueryOperator(
      task_id="load_anticombos", 
      conn_id="snowflake", 
      sql="queries/side_swap_anticombos.sql",
      do_xcom_push=True,
  )

  # 2. Fetch latest term_id
  fetch_term_id = SQLExecuteQueryOperator(
      task_id="fetch_term_id", 
      conn_id="snowflake", 
      sql="queries/upcoming_term.sql",
      do_xcom_push=True,
  )

  # 3. Parse xcom to get latest term_id 
  term_id = fetch_results(fetch_term_id.output)

  # 4. Add production counts for latest term to brine.weekly_meal_counts
  load_weekly_meal_counts = SQLExecuteQueryOperator(
      task_id="load_weekly_meal_counts", 
      conn_id="snowflake", 
      sql="queries/prepare_weekly_meal_counts.sql",
      parameters={"current_term_id": term_id},
  )
  
# Dag call
simple_af_prep()