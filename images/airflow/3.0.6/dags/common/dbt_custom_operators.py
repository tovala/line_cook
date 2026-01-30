from cosmos import DbtRunOperationLocalOperator
from common.dbt_cosmos_config import DBT_PROJECT_CONFIG, PROD_DBT_PROFILE_CONFIG, TEST_DBT_PROFILE_CONFIG, DBT_PROJECT_DIR, DBT_EXECUTABLE_PATH
from collections.abc import Sequence

class runOperatorCustom(DbtRunOperationLocalOperator):
    template_fields: Sequence[str] = DbtRunOperationLocalOperator.template_fields

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def runInTest(cls, task_id, macro_name):
        return cls(
            macro_name=macro_name, # This is an arg so it goes first, dummy
            task_id=task_id,
            env=DBT_PROJECT_CONFIG.env_vars,
            profile_config=TEST_DBT_PROFILE_CONFIG,
            project_dir=DBT_PROJECT_DIR,
            dbt_executable_path=DBT_EXECUTABLE_PATH
        )

    @classmethod
    def runInProd(cls, task_id, macro_name):
        return cls(
            macro_name=macro_name,
            task_id=task_id,
            env=DBT_PROJECT_CONFIG.env_vars,
            profile_config=PROD_DBT_PROFILE_CONFIG,
            project_dir=DBT_PROJECT_DIR,
            dbt_executable_path=DBT_EXECUTABLE_PATH
        )