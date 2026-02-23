import os
from typing import List, Dict

from airflow.sdk import task_group, task, chain
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator, S3FileTransformOperator
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.utils.trigger_rule import TriggerRule

AIRFLOW_HOME = os.environ['AIRFLOW_HOME']
    
@task_group(group_id='process_sftp_file')
def processSFTPFiles(sftp_filenames):
  '''
  Task group for processing each file present in CCC SFTP
  
  :param sftp_filenames: filename from SFTP to process
  1. Generate required paths/filenames
  2. Check if fetch_all - if not, go directly to Load to S3
  3. Grab filenames from S3 if they match filename being processed
  4. Short circuit if file already exists
  5. Load to S3
  '''
  
  @task(multiple_outputs=True)
  def generateFilenames(filename:str, **context) -> Dict[str, str]:
    '''
    Generate a dictionary of permutations of filename for downstream tasks
    
    :param filename: filename to be processed
    :return: dictionary of filenames/sftp/s3 locations
    '''
    s3_key = f'data_downloads/{filename}'
    gz_filename = s3_key.replace('TXT', 'gz')
    bucket_address = f"s3://{context['params']['direct_mail_bucket']}"
    filename_dict = {'s3_key': s3_key, 
                     'sftp_path': f'./{filename}', 
                     'source_s3_key': f'{bucket_address}/{s3_key}',
                     'dest_s3_key': f'{bucket_address}/{gz_filename}'}
    return filename_dict

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
  
  filename_dict = generateFilenames(filename=sftp_filenames)

  fetch_all_check = fetchAllorNot()

  check_processed_files = S3ListOperator(
    task_id='check_processed_files',
    bucket='{{ params.direct_mail_bucket }}',
    prefix=filename_dict['s3_key']
  )

  file_already_processed = fileAlreadyProcessed(check_processed_files.output)

  sftp_to_s3 = SFTPToS3Operator(
    task_id='sftp_to_s3',
    sftp_conn_id='direct_mail_ccc_sftp',
    s3_bucket='{{ params.direct_mail_bucket }}',
    s3_key=filename_dict['s3_key'], 
    sftp_path=filename_dict['sftp_path'],
    # This means that sftp_to_s3 runs as long as all upstream tasks were either skipped or succeeded
    trigger_rule=TriggerRule.NONE_FAILED
  )

  file_transform = S3FileTransformOperator(
    task_id='file_transform',
    source_s3_key=filename_dict['source_s3_key'],
    dest_s3_key=filename_dict['dest_s3_key'],
    transform_script=f'{AIRFLOW_HOME}/dags/direct_mail/scripts/txt_to_gz.py',
    replace=True,
  )

  chain(filename_dict, fetch_all_check, check_processed_files, file_already_processed, sftp_to_s3, file_transform)