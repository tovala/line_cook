from typing import Dict
from airflow.sdk import dag, task, chain
from airflow.timetables.trigger import CronTriggerTimetable
from common.slack_notifications import bad_boy, good_boy, slack_param
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.providers.sftp.hooks.sftp import SFTPHook

@dag(
  dag_id='ccc_direct_mail_download',
  # on_failure_callback=bad_boy,
  # on_success_callback=good_boy,
  # schedule=CronTriggerTimetable('5 3 * * *', timezone='America/Chicago'),
  catchup=False,
  tags=['external'],
  params={
    'channel_name': slack_param('#team-data-notifications'),
  }
)
def CCCDirectMailDownload():
  '''CCC Direct Mail Import
  Description: Grabs files from CCC SFTP and sends to S3
  # 1. Grabs files and uploads to S3

  Schedule: Daily at 3:05 AM 

  Dependencies:

  Variables:

  '''

  @task()
  def fetchSFTPFiles(filename_pattern: str = 'CLIENT_OUTPUT_') -> list:
    sftp_hook = SFTPHook(ssh_conn_id='direct_mail_ccc_sftp')
    files = sftp_hook.list_directory('.')

    return [f for f in files if filename_pattern in f]

  @task()
  def generateExpandDictionary(file_list: list) -> list:
    sftp_dict = [{'sftp_path': './'+ f, 's3_key': 'data_downloads/' + f} for f in file_list]
    return sftp_dict
  
  sftp_filenames = fetchSFTPFiles()
  sftp_dict = generateExpandDictionary(sftp_filenames)


  sftp_to_s3 = SFTPToS3Operator.partial(
    task_id='sftp_to_s3',
    sftp_conn_id='direct_mail_ccc_sftp',
    s3_bucket='tovala-ccc-direct-mail',
  ).expand_kwargs(sftp_dict)

  #TODO: Either convert to CSV or load as pipe delimited 

CCCDirectMailDownload()