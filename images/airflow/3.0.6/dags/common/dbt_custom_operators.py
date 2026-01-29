from cosmos import DbtRunOperationLocalOperator
from cosmos.config import ProfileConfig
from typing import Any, Sequence
from airflow.sdk import Variable

from common.dbt_cosmos_config import PROD_DBT_PROFILE_CONFIG, TEST_DBT_PROFILE_CONFIG, DBT_PROJECT_DIR, DBT_EXECUTABLE_PATH

class runOperatorCustom(DbtRunOperationLocalOperator):
    '''
    Executes a dbt core run-operation command command.
    '''

    template_fields: Sequence[str] = DbtRunOperationLocalOperator.template_fields  # type: ignore[operator]

    # def __init__(self, profile_config: ProfileConfig, *args: Any, **kwargs: Any) -> None:
    #     super().__init__(*args, **kwargs)
    #     self.profile_config=PROD_DBT_PROFILE_CONFIG

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.task_id=None
        self.macro_name=None 
        self.profile_config=None
        self.env=None 
        self.project_dir=None 
        self.dbt_executable_path=None

    @classmethod
    def testicle(cls, task_id, macro_name):
        test_operator = runOperatorCustom(task_id=task_id, macro_name=macro_name, 
                                profile_config=TEST_DBT_PROFILE_CONFIG, 
                                env = {'SF_AWS_KEY': Variable.get('dbt_sf_aws_key'),'SF_AWS_SECRET': Variable.get('dbt_sf_aws_secret')}, 
                                project_dir=DBT_PROJECT_DIR,
                                dbt_executable_path=DBT_EXECUTABLE_PATH)
        return test_operator

    # @staticmethod
    # def runInTest(task_id, macro_name):
    #     return runOperatorCustom(task_id=task_id, macro_name=macro_name, 
    #                             profile_config=TEST_DBT_PROFILE_CONFIG, 
    #                             env = {'SF_AWS_KEY': Variable.get('dbt_sf_aws_key'),'SF_AWS_SECRET': Variable.get('dbt_sf_aws_secret')}, 
    #                             project_dir=DBT_PROJECT_DIR,
    #                             dbt_executable_path=DBT_EXECUTABLE_PATH)

    # @classmethod
    # def runInProd(cls, *args, **kwargs):
    #     '''
    #     Generic configurations to run an operation using --target prod
    #     '''
    #     self.profile_config=PROD_DBT_PROFILE_CONFIG
    #     self.env={
    #         'SF_AWS_KEY': Variable.get('dbt_sf_aws_key'),
    #         'SF_AWS_SECRET': Variable.get('dbt_sf_aws_secret')
    #     }
    #     self.project_dir=DBT_PROJECT_DIR
    #     self.dbt_executable_path=DBT_EXECUTABLE_PATH

    # # @classmethod
    # # def runInTest(cls, *args, **kwargs):
    #     '''
    #     Generic configurations to run an operation using --target test
    #     '''
    #     self.profile_config=TEST_DBT_PROFILE_CONFIG
    #     self.env={
    #         'SF_AWS_KEY': Variable.get('dbt_sf_aws_key'),
    #         'SF_AWS_SECRET': Variable.get('dbt_sf_aws_secret')
    #     }
    #     self.project_dir=DBT_PROJECT_DIR
    #     self.dbt_executable_path=DBT_EXECUTABLE_PATH,  