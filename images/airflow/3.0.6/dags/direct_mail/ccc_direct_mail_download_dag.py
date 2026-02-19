from typing import Dict
from airflow.sdk import dag, task, chain
from airflow.timetables.trigger import CronTriggerTimetable
from common.slack_notifications import bad_boy, good_boy, slack_param
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator

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
  Description: Grabs file from CCC SFTP and sends to S3
  # 1. Generates the start date, end date, and filename

  Schedule: Daily at 3:05 AM 

  Dependencies:

  Variables:

  '''

  sftp_to_s3 = SFTPToS3Operator(
    task_id='sftp_to_s3',
    sftp_conn_id='direct_mail_ccc_sftp',
    sftp_path='./CLIENT_OUTPUT*',
    s3_bucket='tovala-ccc-direct-mail',
    s3_key='data_download/'
  )

  sftp_to_s3