from cosmos import DbtRunOperationLocalOperator
from cosmos.config import ProfileConfig
from typing import Any, Sequence
from airflow.sdk import Variable, task
from common.dbt_cosmos_config import PROD_DBT_PROFILE_CONFIG, TEST_DBT_PROFILE_CONFIG, DBT_PROJECT_DIR, DBT_EXECUTABLE_PATH

class runOperatorCustom(DbtRunOperationLocalOperator):
    '''
    Executes a dbt core run-operation command command.
    '''

    template_fields: Sequence[str] = DbtRunOperationLocalOperator.template_fields # type: ignore[operator]

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    @classmethod
    @task
    def runInTest(cls, task_id, macro_name):
        test_operator = runOperatorCustom(task_id=task_id, macro_name=macro_name, 
                                profile_config=TEST_DBT_PROFILE_CONFIG, 
                                project_dir=DBT_PROJECT_DIR,
                                dbt_executable_path=DBT_EXECUTABLE_PATH)
        # return test_operator