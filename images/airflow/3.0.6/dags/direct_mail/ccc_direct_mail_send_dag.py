import datetime

from airflow.sdk import dag, task, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.timetables.trigger import CronTriggerTimetable
from common.slack_notifications import bad_boy, good_boy, getSlackChannelNameParam
from typing import Any, List
from common.sql_operator_handlers import fetch_single_result
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
# from airflow.providers.sftp.operators.sftp import SFTPOperator

@dag(
    # on_failure_callback=bad_boy,
    # on_success_callback=good_boy,
    # schedule=CronTriggerTimetable('0 2 * * 1', timezone='America/Chicago')
    catchup=False,
    tags=['external'],
    params={
        'channel_name': getSlackChannelNameParam('#team-data-notifications'),
    }
)
def ccc_direct_mail_send():
    '''CCC Direct Mail Export
    Description: Extracts recent orders and uploads them to SFTP server for CCC
    # 1. Generates the start date, end date, and filename
    # 2. Uploads the requested data to S3
    # 3. Sends the data to CCC's SFTP 

    Schedule: Weekly at 2:00 AM Monday

    Dependencies:

    Variables:

    '''
    @task(multiple_outputs=True)
    def getDatesandFilename():
        today = datetime.date.today()
        offset = (today.weekday() + 1) % 7
        end_date = today - datetime.timedelta(days=offset)
        start_date = end_date - datetime.timedelta(days=6)
        filename = f'AXMATCH_TOVALA_Direct_Mail_{str(start_date)}-{str(end_date)}.csv'
        return {'start_date': str(start_date),
                'end_date': str(end_date),
                'filename': filename}

    dates_and_filename = getDatesandFilename()

    upload_to_s3 = SqlToS3Operator(
        task_id='upload_to_s3',
        sql_conn_id='snowflake',
        query='queries/direct_mail_ccc.sql', 
        s3_bucket='tovala-ccc-direct-mail', 
        s3_key=f"data_sends/{dates_and_filename['filename']}", 
        parameters={'start_date': dates_and_filename['start_date'],
                    'end_date': dates_and_filename['end_date']},
        replace=True, 
        file_format='CSV', 
    )

    chain(dates_and_filename, upload_to_s3)

    # send_to_sftp = S3ToSFTPOperator(
    #     s3_bucket='tovala-ccc-direct-mail',
    #     s3_key='data_sends/AXMATCH_TOVALA_Direct_Mail_{{params.start_date}}-{{params.end_date}}.csv',
    #     sftp_path=,
    #     sftp_conn_id=
    # )

ccc_direct_mail_send()