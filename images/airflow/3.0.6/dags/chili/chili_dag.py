from pendulum import duration

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, Param
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.state import DagRunState

from common.slack_notifications import bad_boy, good_boy, slack_param

CHILD_DAGS = [
  'chili_box_fillometer',
  'chili_cdn_menu',
  'chili_marketing_incentives',
  'chili_midline',
  # Append each chili child dag_id here as its table migrates off spice_rack:
  # device_to_serial, micro_logs, oven_logs, shipment_file,
  # tovala_assist, tovala_preset.
]

@dag(
  dag_id='chili',
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  schedule=CronTriggerTimetable('0 */3 * * *', timezone='America/Chicago'),
  catchup=False,
  # Native rendering preserves the bool type of full_refresh when passed via conf.
  render_template_as_native_obj=True,
  default_args={
    'retries': 2,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['internal', 'data-integration', 'chili'],
  params={
    'channel_name': slack_param(),
    'full_refresh': Param(False, type='boolean'),
  }
)
def chili():
  for child_dag_id in CHILD_DAGS:
    TriggerDagRunOperator(
      task_id=f'trigger_{child_dag_id}',
      trigger_dag_id=child_dag_id,
      trigger_run_id='triggered_{{ run_id }}',
      conf={'full_refresh': '{{ params.full_refresh }}'},
      wait_for_completion=True,
      poke_interval=30,
      failed_states=[DagRunState.FAILED],
      allowed_states=[DagRunState.SUCCESS],
      deferrable=True,
      retries=0,
    )

chili()
