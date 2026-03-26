from typing import Dict

from pendulum import duration

from airflow.sdk import dag, task, Variable
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.exceptions import AirflowException

from common.slack_notifications import bad_boy, good_boy, slack_param


AWS_CONN_ID = "aws_main_account"


@dag(
    dag_id="sproing",
    on_failure_callback=bad_boy,
    on_success_callback=good_boy,
    schedule=CronTriggerTimetable("0 4 * * *", timezone="America/Chicago"),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": duration(seconds=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": duration(minutes=5),
    },
    tags=["internal"],
    params={
        "channel_name": slack_param(),
    },
)
def sproing():
    """Sproing - SQS Failed Event Backfill

    Description: Reads failed events from Firehose and Micro SQS dead-letter
      queues and reprocesses them via Lambda invocation.

    Schedule: Daily at 4AM

    Dependencies: aws_main_account connection (cross-account to main Tovala account)

    Variables: sproing_firehose_queue_url, sproing_firehose_lambda_arn,
               sproing_micro_queue_url, sproing_micro_lambda_arn
    """

    @task(multiple_outputs=True)
    def get_sproing_config() -> Dict[str, str]:
        """Fetch all Sproing configuration from Airflow Variables at runtime."""
        return {
            "firehose_queue_url": Variable.get("sproing_firehose_queue_url"),
            "firehose_lambda_arn": Variable.get("sproing_firehose_lambda_arn"),
            "micro_queue_url": Variable.get("sproing_micro_queue_url"),
            "micro_lambda_arn": Variable.get("sproing_micro_lambda_arn"),
        }

    @task()
    def process_queue(queue_url: str, lambda_arn: str) -> Dict[str, int]:
        """Drain an SQS queue, invoke Lambda async for each message, delete on success."""
        from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
        from airflow.providers.amazon.aws.hooks.sqs import SqsHook

        sqs_client = SqsHook(aws_conn_id=AWS_CONN_ID).get_conn()
        lambda_hook = LambdaHook(aws_conn_id=AWS_CONN_ID)

        processed = 0
        failed = 0

        while True:
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=5,
                AttributeNames=["All"],
                VisibilityTimeout=300,
            )
            messages = response.get("Messages", [])
            if not messages:
                break

            for message in messages:
                resp = lambda_hook.invoke_lambda(
                    function_name=lambda_arn,
                    invocation_type="Event",
                    payload=message["Body"],
                )
                if resp.get("StatusCode") != 202:
                    failed += 1
                    continue

                sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message["ReceiptHandle"],
                )
                processed += 1

        if failed > 0:
            raise AirflowException(f"{failed} message(s) failed Lambda invocation")

        return {"processed": processed, "failed": failed}

    config = get_sproing_config()

    process_queue.override(task_id="process_firehose")(
        queue_url=config["firehose_queue_url"],
        lambda_arn=config["firehose_lambda_arn"],
    )

    process_queue.override(task_id="process_micro")(
        queue_url=config["micro_queue_url"],
        lambda_arn=config["micro_lambda_arn"],
    )


sproing()
