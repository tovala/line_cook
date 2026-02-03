from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.common.sql.hooks.handlers import fetch_all_handler, return_single_query_results
from airflow.providers.common.sql.operators.sql import BaseSQLOperator

if TYPE_CHECKING:
    import jinja2

    from airflow.providers.common.compat.sdk import Context
    from airflow.providers.openlineage.extractors import OperatorLineage

class SnowflakeReturnSingleResultOperator(BaseSQLOperator):
  template_fields: Sequence[str] = ("sql", "parameters", *BaseSQLOperator.template_fields)
  template_ext: Sequence[str] = (".sql", ".json")
  template_fields_renderers: ClassVar[dict] = {"sql": "sql", "parameters": "json"}
  def __init__(
    self,
    *,
    sql: str | list[str],
    autocommit: bool = False,
    parameters: Mapping | Iterable | None = None,
    handler: Callable[[Any], list[tuple] | None] = fetch_all_handler,
    output_processor: (
      Callable[
        [list[Any], list[Sequence[Sequence] | None]],
        list[Any] | tuple[list[Sequence[Sequence] | None], list],
      ]
      | None
    ) = None,
    conn_id: str | None = None,
    **kwargs,
  ) -> None:
    super().__init__(conn_id=conn_id, database=database, **kwargs)
    self.sql = sql
    self.autocommit = autocommit
    self.parameters = parameters
    self.handler = handler
    self._output_processor = output_processor or default_output_processor
    self.split_statements = split_statements
    self.return_last = return_last
    self.show_return_value_in_logs = show_return_value_in_logs
    self.requires_result_fetch = requires_result_fetch

  def execute(self, context):
    self.log.info("Executing: %s", self.sql)
    hook = self.get_db_hook()
    if self.split_statements is not None:
      extra_kwargs = {"split_statements": self.split_statements}
    else:
      extra_kwargs = {}
    output = hook.run(
      sql=self.sql,
      autocommit=self.autocommit,
      parameters=self.parameters,
      handler=self.handler
      if self._should_run_output_processing() or self.requires_result_fetch
      else None,
      return_last=self.return_last,
      **extra_kwargs,
    )
    if not self._should_run_output_processing():
      return None
    if return_single_query_results(self.sql, self.return_last, self.split_statements):
      # For simplicity, we pass always list as input to _process_output, regardless if
      # single query results are going to be returned, and we return the first element
      # of the list in this case from the (always) list returned by _process_output
      return self._process_output([output], hook.descriptions)[-1]
    result = self._process_output(output, hook.descriptions)
    self.log.info("result: %s", result)
    return result