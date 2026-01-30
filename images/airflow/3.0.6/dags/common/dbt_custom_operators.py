from cosmos import DbtRunOperationLocalOperator
from common.dbt_cosmos_config import DBT_PROJECT_CONFIG, PROD_DBT_PROFILE_CONFIG, TEST_DBT_PROFILE_CONFIG, DBT_PROJECT_DIR, DBT_EXECUTABLE_PATH

class runOperatorCustom(DbtRunOperationLocalOperator):
    template_fields = (*DbtRunOperationLocalOperator.template_fields, "env")

    def __init__(self, env=None, **kwargs):
        super().__init__(**kwargs)
        self.env = env 

    @classmethod
    def runInTest(cls, task_id, macro_name, env_vars=None):
        return cls(
            task_id=task_id,
            macro_name=macro_name,
            env=DBT_PROJECT_CONFIG.env_vars,
            profile_config=TEST_DBT_PROFILE_CONFIG,
            project_dir=DBT_PROJECT_DIR,
            dbt_executable_path=DBT_EXECUTABLE_PATH
        )

    @classmethod
    def runInProd(cls, task_id, macro_name, env_vars=None):
        return cls(
            task_id=task_id,
            macro_name=macro_name,
            env=DBT_PROJECT_CONFIG.env_vars,
            profile_config=PROD_DBT_PROFILE_CONFIG,
            project_dir=DBT_PROJECT_DIR,
            dbt_executable_path=DBT_EXECUTABLE_PATH
        )