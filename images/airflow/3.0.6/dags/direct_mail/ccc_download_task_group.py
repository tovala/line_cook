from typing import List

from airflow.sdk import task_group, task, chain
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.utils.trigger_rule import TriggerRule
    
@task_group(group_id='process_sftp_file')
def processSFTPFiles(sftp_filenames):
  '''
  Task group for processing each file present in CCC SFTP
  
  :param sftp_filenames: filename from SFTP to process
  1. Generate S3 Key
  2. Generate SFTP Path
  3. Check if fetch_all - if not, go directly to Load to S3
  4. Grab filenames from S3 if they match filename being processed
  5. Short circuit if file already exists
  6. Load to S3
  '''

  @task()
  def generateS3Key(filename: str) -> str:
    '''
    Creates the S3 key for filename to process
    
    :param filename: filename to process
    :return: data_downloads/filename
    '''
    return f'data_downloads/{filename}'
  
  @task()
  def generateSFTPPath(filename:str) -> str:
    '''
    Creates the SFTP path for filename to process
    
    :param filename: filename to process
    :return: ./filename
    '''
    return f'./{filename}'
  
  @task.short_circuit
  def fileAlreadyProcessed(s3_filename: List) -> bool:
    '''
    Checks to see if file was already processed
    
    :param s3_filename: list of filenames from S3 matching original file being processed
    :return: True if no files are returned that match, False o/w
    '''
    if len(s3_filename) == 0:
      return True
    else:
      return False
    
  @task.branch()
  def fetchAllorNot(**context):
    '''
    Creates branch s.t.
      If processing all files, skip the checking steps and proceed directly to uploading to s3
      If NOT processing all files, check if they have been processed already before uploading to s3    
    '''
    if context['params']['fetch_all']:
      return ['process_sftp_file.sftp_to_s3']
    else:
      return ['process_sftp_file.check_processed_files']
    
  s3_key = generateS3Key(filename=sftp_filenames)
  sftp_path = generateSFTPPath(filename=sftp_filenames)

  check_processed_files = S3ListOperator(
    task_id="check_processed_files",
    bucket='{{ params.direct_mail_bucket }}',
    prefix=s3_key
  )

  file_already_processed = fileAlreadyProcessed(check_processed_files.output)

  sftp_to_s3 = SFTPToS3Operator(
    task_id='sftp_to_s3',
    sftp_conn_id='direct_mail_ccc_sftp',
    s3_bucket='{{ params.direct_mail_bucket }}',
    s3_key=s3_key, 
    sftp_path=sftp_path,
    # This means that sftp_to_s3 runs as long as all upstream tasks were either skipped or succeeded
    trigger_rule=TriggerRule.NONE_FAILED
  )
  
  fetch_all_check = fetchAllorNot()

  chain((s3_key, sftp_path), fetch_all_check, check_processed_files, file_already_processed, sftp_to_s3)