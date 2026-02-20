from airflow.sdk import task_group, task, chain
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.utils.trigger_rule import TriggerRule
    
@task_group(group_id='process_sftp_file')
def processSFTPFiles(sftp_filenames):

  @task()
  def generateS3Key(filename):
    return f'data_downloads/{filename}'
  
  @task()
  def generateSFTPPath(filename):
    return f'./{filename}'
  
  @task.short_circuit
  def fileAlreadyProcessed(s3_key: str) -> bool:
    if s3_key:
      return False
    else:
      return True
    
  @task.branch()
  def fetchAllorNot(**context):
    if context['params']['fetch_all']:
      return ['process_sftp_file.sftptoS3']
    else:
      return ['process_sftp_file.checkProcessedFiles', 'process_sftp_file.fileAlreadyProcessed', 'process_sftp_file.sftptoS3']

  s3_key = generateS3Key(filename=sftp_filenames)
  sftp_path = generateSFTPPath(filename=sftp_filenames)

  checkProcessedFiles = S3ListOperator(
    task_id="checkProcessedFiles",
    bucket='{{ params.direct_mail_bucket }}',
    prefix=s3_key
  )

  file_check = fileAlreadyProcessed(checkProcessedFiles)

  sftptoS3 = SFTPToS3Operator(
    task_id='sftptoS3',
    sftp_conn_id='direct_mail_ccc_sftp',
    s3_bucket='{{ params.direct_mail_bucket }}',
    s3_key=s3_key, 
    sftp_path=sftp_path,
    trigger_rule=TriggerRule.NONE_FAILED
  )
  
  fetchAll = fetchAllorNot()

  chain((s3_key, sftp_path), fetchAll, checkProcessedFiles, file_check, sftptoS3)