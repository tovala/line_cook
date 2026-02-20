from typing import List, Dict
import re
from pendulum import duration

from airflow.sdk import dag, task, chain
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.sftp.hooks.sftp import SFTPHook

from common.slack_notifications import bad_boy, good_boy, slack_param
from direct_mail.ccc_download_task_group import processSFTPFiles

@dag(
  dag_id='ccc_direct_mail_download',
  # on_failure_callback=bad_boy,
  # on_success_callback=good_boy,
  # schedule=CronTriggerTimetable('5 3 * * *', timezone='America/Chicago'),
  catchup=False,
  default_args={
    'retries': 3,
    'retry_delay': duration(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': duration(minutes=5),
  },
  tags=['external'],
  params={
    'channel_name': slack_param('#team-data-notifications'),
    'direct_mail_bucket': 'tovala-ccc-direct-mail',
  }
)
def CCCDirectMailDownload():
  '''CCC Direct Mail Import
  Description: Grabs files from CCC SFTP and sends to S3
  # 1. Fetch files to be processed from S3 (short circuit task to stop DAG if there are none)
  # 2. Process filenames into a dictionary for downstream tasks
  # 3. Upload raw files to S3
  # 4. TODO Convert files to JSON
  # 5. TODO Trigger ingestion DAG

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

  chain(nonEmptySFTPFiles(sftp_filenames), processSFTPFiles.expand(sftp_filenames=sftp_filenames))

  #TODO: Either convert to CSV or load as pipe delimited 
  # Option 1: Convert using operator (but that is processed locally)
  # Option 2: Convert using a Lambda
  # Option 3: Convert all the old files - down side is that it breaks if we add new fields (which they have done)

CCCDirectMailDownload()