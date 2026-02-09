from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from collections.abc import Sequence
from typing import Any

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils.common import enclose_param

class CopyTransformFromExternalStageToSnowflakeOperator(CopyFromExternalStageToSnowflakeOperator):
  '''
  Docstring for CopyTransformFromExternalStageToSnowflakeOperator
  '''

  template_fields: Sequence[str] = ('transform_columns',)
  template_fields_renderers = {'transform_columns': 'sql'}

  def __init__(
      self,
      transform_columns,
      **kwargs
  ):
    super().__init__(**kwargs)
    self.transform_columns = transform_columns

  
  def execute(self, context: Any) -> None:
        self.hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
        )

        if self.schema:
            into = f'{self.schema}.{self.table}'
        else:
            into = self.table  # type: ignore[assignment]

        if self.columns_array:
            into = f"{into}({', '.join(self.columns_array)})"

        self._sql = f'''
        COPY INTO {into}
             FROM  @{self.stage}/{self.prefix or ""}
        {"FILES=(" + ",".join(map(enclose_param, self.files)) + ")" if self.files else ""}
        {"PATTERN=" + enclose_param(self.pattern) if self.pattern else ""}
        FILE_FORMAT={self.file_format}
        {self.copy_options or ""}
        {self.validation_mode or ""}
        '''
        self.log.info("Executing COPY command...")
        self._result = self.hook.run(  # type: ignore # mypy does not work well with return_dictionaries=True
            sql=self._sql,
            autocommit=self.autocommit,
            handler=lambda x: x.fetchall(),
            return_dictionaries=True,
        )
        self.log.info("COPY command completed")