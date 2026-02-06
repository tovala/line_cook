import datetime

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.trigger import CronTriggerTimetable
from common.slack_notifications import bad_boy, good_boy, getSlackChannelNameParam
from typing import Any, List
from common.sql_operator_handlers import fetch_single_result
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

# sftp_port = 22  # Default SFTP port
# sftp_host_path = '/spice_rack/sftp_ccc/host'
# sftp_username_path = '/spice_rack/sftp_ccc/username'
# sftp_password_path = '/spice_rack/sftp_ccc/pw'

# sftp_host = cap.get_parameter_store(sftp_host_path)
# sftp_username = cap.get_parameter_store(sftp_username_path)
# sftp_password = cap.get_parameter_store(sftp_password_path)

def get_last_sunday():
    today = datetime.date.today()
    offset = (today.weekday() + 1) % 7
    return today - datetime.timedelta(days=offset)

@dag(
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # schedule=CronTriggerTimetable('0 2 * * 1', timezone='America/Chicago')
    catchup=False,
    tags=['external'],
    params={
        'channel_name': getSlackChannelNameParam('#team-data-notifications'),
        'end_date': str(get_last_sunday()),
        'start_date': str(get_last_sunday() - datetime.timedelta(days=6))
    }
)
def ccc_direct_mail_send():
    '''CCC Direct Mail Export
    Description: Extracts recent orders and uploads them to SFTP server for CCC
    # 1. Uploads the requested data to S3
    # 2. Sends the data to CCC's SFTP 

    Schedule: Weekly at 2:00 AM Monday

    Dependencies:

    Variables:

    '''

    upload_to_s3 = SqlToS3Operator(
        task_id='upload_to_s3',
        sql_conn_id='snowflake',
        query='queries/direct_mail_ccc.sql', 
        s3_bucket='tovala-ccc-direct-mail', 
        s3_key='data_sends/AXMATCH_TOVALA_Direct_Mail_{{params.start_date}}-{{params.end_date}}.csv', 
        replace=True, 
        file_format='CSV', 
    )

    # send_to_sftp = S3ToSFTPOperator(
    #     s3_bucket='tovala-ccc-direct-mail',
    #     s3_key='data_sends/AXMATCH_TOVALA_Direct_Mail_{{params.start_date}}-{{params.end_date}}.csv',
    #     sftp_path=,
    #     sftp_conn_id=
    # )

ccc_direct_mail_send()