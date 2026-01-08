from airflow.providers.slack.notifications.slack import SlackNotifier
from typing import Any, Dict

SLACK_WEBHOOK_CONNECTION_ID = 'team-data-notifications'

# Shamelessly poached from: https://github.com/enchant3dmango/lugvloei/blob/65726392386200c2420ee8f70b3682834c1ddaad/utilities/slack.py#L105
def generate_failure_message(context: Dict[str, Any]) ->  Dict[str, Any]:
    dag_id        = context.get('task_instance').dag_id
    dag_owner     = context.get("dag").owner
    task_id       = context.get('task').task_id
    log_url       = context.get('task_instance').log_url
    retry_count   = context.get('task_instance').try_number - 1
    run_id        = context.get('task_instance').run_id
    error_message = (str(context.get("exception"))[:140] + '...') if len(
        str(context.get("exception"))) > 140 else str(context.get("exception"))

    return {
        "text": ":dumpster_fire: *Task Failure Alert!*",
        "attachments": [
            {
                "color": "#B22222",
                "fields": [
                    {
                        "title": "DAG ID:",
                        "value": f"{dag_id}",
                        "short": True
                    },
                    {
                        "title": "Task ID:",
                        "value": f"_<{log_url}|{task_id}>_",
                        "short": True
                    },
                    {
                        "title": "DAG Owner:",
                        "value": f"{dag_owner}",
                        "short": True
                    },
                    {
                        "title": "Retry Count:",
                        "value": f"{retry_count}",
                        "short": True
                    },
                    {
                        "title": "Message:",
                        "value": f"{error_message}",
                        "short": False
                    }
                ],
                "footer": f"Run ID: {run_id}",
            }
        ]
    }

def generate_success_message(context: Dict[str, Any]) ->  Dict[str, Any]:
    dag_id    = context.get('task_instance').dag_id
    dag_owner = context.get("dag").owner
    run_id    = context.get('task_instance').run_id

    return {
        "text": ":party_cat: *Task Success Notification!*",
        "attachments": [
            {
                "color": "#009900",
                "fields": [
                    {
                        "title": "DAG ID:",
                        "value": f"{dag_id}",
                        "short": True
                    },
                    {
                        "title": "DAG Owner:",
                        "value": f"{dag_owner}",
                        "short": True
                    },
                    {
                        "title": "Message:",
                        "value": "Flawless execution! Your DAG has successfully completed its tasks.",
                        "short": False
                    }
                ],
                "footer": f"Run ID: {run_id}",
            }
        ]
    }

def bad_boy(context: Dict[str, Any]) ->  None:
    """ 
    Send alert into Slack channel everytime a DAG failed.
    """

    slack_channel = context.get('params')['channel_name']
    notifier = SlackNotifier(
        slack_conn_id       = SLACK_WEBHOOK_CONNECTION_ID,
        channel             = slack_channel,
        text                = generate_failure_message(context=context)['text'],
        attachments         = generate_failure_message(context=context)['attachments']
    )

    notifier.notify(context)

def good_boy(context: Dict[str, Any]) ->  None:
    """ 
    Send alert into Slack channel everytime a DAG succeeded.
    """

    slack_channel = context.get('params')['channel_name']
    notifier = SlackNotifier(
        slack_conn_id       = SLACK_WEBHOOK_CONNECTION_ID,
        channel             = slack_channel,
        text                = generate_success_message(context=context)['text'],
        attachments         = generate_success_message(context=context)['attachments']
    )

    notifier.notify(context)