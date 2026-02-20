from airflow.sdk import task_group, task
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
    
@task_group(group_id='process_sftp_file')
def processSFTPFiles(sftp_filenames):

  @task()
  def generateS3Key(filename):
    return f'data_downloads/{filename}'
  
  @task()
  def generateSFTPPath(filename):
    return f'./{filename}'

  s3_key = generateS3Key(filename=sftp_filenames)
  sftp_path = generateSFTPPath(filename=sftp_filenames)

  checkProcessedFiles = S3ListOperator(
    task_id="checkProcessedFiles",
    bucket='{{ params.direct_mail_bucket }}',
    prefix=s3_key
  )

  sftp_to_s3 = SFTPToS3Operator(
    task_id='sftp_to_s3',
    sftp_conn_id='direct_mail_ccc_sftp',
    s3_bucket='{{ params.direct_mail_bucket }}',
    s3_key=s3_key, 
    sftp_path=sftp_path
  )