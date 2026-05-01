from typing import Any, Dict, Optional

from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.sdk import Param
from airflow.utils.state import TaskInstanceState

SLACK_WEBHOOK_CONNECTION_ID = 'tovala_slack'


def _earliest_failed_task_instance(context: Dict[str, Any]) -> Optional[Any]:
    # DAG-level on_failure_callback hands us a context whose 'task'/'task_instance'
    # often points at the most-recently-finished task (e.g. a teardown or a
    # trigger_rule='one_failed' cleanup task), not the upstream task that actually
    # broke. Pull the real failures out of the DAG run instead.
    dag_run = context.get('dag_run')
    if dag_run is None:
        return None
    try:
        failed = [ti for ti in dag_run.get_task_instances() if ti.state == TaskInstanceState.FAILED]
    except Exception:
        return None
    if not failed:
        return None
    failed.sort(key=lambda ti: (ti.start_date is None, ti.start_date))
    return failed[0]

def slack_param(channel_name: str='#team-data-airflow-notifications'):
    '''
    Returns an Airflow Param object for slack channel name.
    Defaults to #team-data-notifications channel for alerts unless otherwise specified.
    
    :param channel_name: Description
    :type channel_name: str
    '''
    return Param(channel_name, type=['string', 'null'], description='Slack channel to send alerts. If a private channel, the Airflow Notifications slack app MUST be explicitly invited into the channel.')

# Shamelessly poached from: https://github.com/enchant3dmango/lugvloei/blob/65726392386200c2420ee8f70b3682834c1ddaad/utilities/slack.py#L105
def generate_failure_message(context: Dict[str, Any]) ->  Dict[str, Any]:
    ti            = _earliest_failed_task_instance(context) or context.get('task_instance')
    dag           = context.get('dag')
    dag_id        = dag.dag_id if dag else ti.dag_id
    dag_owner     = dag.owner if dag else ''
    task_id       = ti.task_id
    log_url       = ti.log_url
    retry_count   = ti.try_number - 1
    run_id        = ti.run_id
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