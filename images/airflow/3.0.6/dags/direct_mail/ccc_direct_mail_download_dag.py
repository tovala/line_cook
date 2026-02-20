from typing import List
import re
from pendulum import duration

from airflow.sdk import dag, task, chain, Param
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from common.slack_notifications import bad_boy, good_boy, slack_param
from direct_mail.ccc_download_task_group import processSFTPFiles

@dag(
  dag_id='ccc_direct_mail_download',
  on_failure_callback=bad_boy,
  on_success_callback=good_boy,
  schedule=CronTriggerTimetable('5 3 * * *', timezone='America/Chicago'),
  catchup=False,
  default_args={
    'retries': 3,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['external', 'ingestion'],
  params={
    'channel_name': slack_param(),
    'direct_mail_bucket': Param('tovala-ccc-direct-mail', type='string'),
    'fetch_all': Param(False, type='boolean', description='If true, reprocess previously processed files.')
  }
)
def CCCDirectMailDownload():
  '''CCC Direct Mail Import
  Description: Grabs files from CCC SFTP and sends to S3
  # 1. Fetch files to be processed from S3 (short circuit task to stop DAG if there are none)
  # 2. Pass to task group to process each file 

  Schedule: Daily at 3:05 AM 

  Dependencies:

  Variables:

  '''

  @task()
  def fetchSFTPFiles(filename_pattern: str = 'CLIENT_OUTPUT_(.*).TXT') -> List[str]:
    '''
    Fetches all the files matching the specified pattern from the CCC SFTP
    
    :param filename_pattern: regex pattern for matching output file
    :return: List of filenames
    '''
    sftp_hook = SFTPHook(ssh_conn_id='direct_mail_ccc_sftp')
    files = sftp_hook.list_directory('.')

    return [f for f in files if re.fullmatch(filename_pattern, f.upper())]
    
  @task.short_circuit
  def nonEmptySFTPFiles(sftp_filenames: List[str]) -> bool:
    '''
    Short circuit task to stop DAG if no files remain to be processed
    
    :param sftp_filenames: List of files (or empty list) from fetchSFTPFiles
    :return: True if there are files to process/False o/w
    '''
    if sftp_filenames == []:
      return False
    else:
      return True
  
  sftp_filenames = fetchSFTPFiles()

  trigger_chili_load = TriggerDagRunOperator(
    task_id='trigger_chili_load_dag',
    trigger_rule=TriggerRule.NONE_FAILED,
    trigger_dag_id='direct_mail_load_to_chili',
    trigger_run_id='triggered_{{ run_id }}'
  )

  chain(nonEmptySFTPFiles(sftp_filenames), processSFTPFiles.expand(sftp_filenames=sftp_filenames), trigger_chili_load)

CCCDirectMailDownload()