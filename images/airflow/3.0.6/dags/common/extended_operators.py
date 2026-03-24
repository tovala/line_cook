from typing import Sequence

from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator

class TemplatedCopyFromExternalStageToSnowflakeOperator(CopyFromExternalStageToSnowflakeOperator):
  template_fields: Sequence[str] = CopyFromExternalStageToSnowflakeOperator.template_fields + ('stage', 'file_format', 'table', 'copy_options')

  def __init__(self, *args, **kwargs):
      super().__init__(*args, **kwargs)