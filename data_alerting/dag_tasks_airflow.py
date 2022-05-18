#data-processing/processing/dags/tableau_to_slack/alerting/dag.py/

from datetime import datetime

import pendulum
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from processing.common.operators.slack_publish_image_operator import SlackPublishImageOperator
from processing.common.operators.tableau_view_url_operator import TableauViewToImageOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryValueCheckOperator
from processing.common.utils.happn import HappnCommon

local_tz = pendulum.timezone("Europe/Paris")

def alerting_tasks(alerting_name, slack_channel, query, view_url):
    image_path = '/home/airflow/gcs/data/tableau_to_slack/{}/{}.png'.format('{{ds_nodash}}',
                                                                            f'tableau_to_slack.alerting_'
                                                                            f'{alerting_name}')
    task_tableau_view = TableauViewToImageOperator(
        task_id=f'tableau_view_{alerting_name}',
        site_id='company_site',
        view_url=view_url,
        tableau_conn_id='tableau_connection',
        image_path=image_path,
        on_failure_callback=HappnCommon.slack_dag_failure_notifier,
        retries=0)

    task_valid_check = BigQueryValueCheckOperator(
        task_id=f'data_condition_check_{alerting_name}',
        bigquery_conn_id='google_cloud_default',
        on_failure_callback=None,
        sql=query,
        use_legacy_sql=False,
        pass_value=False,
        retries=0)

    task_slack_publish = SlackPublishImageOperator(
        task_id=f'slack_publish_{alerting_name}',
        image_path=image_path,
        channel=slack_channel,
        text=f":alert: :point_right: <{view_url}| Click to see report in Tableau>",
        trigger_rule=TriggerRule.ONE_FAILED,
        on_failure_callback=HappnCommon.slack_dag_failure_notifier,
        retries=0)

    return task_tableau_view, task_valid_check, task_slack_publish


alerting_dag = DAG(
    dag_id='tableau_to_slack.alerting',
    schedule_interval='0 8 * * *',
    start_date=datetime(2021, 12, 15, tzinfo=local_tz))

with alerting_dag:
    SLACK_CHANNEL = "data_alerting"
    alerting_list = [
        {"alerting_name": "dau",
         "query": "dau.sql", 
         "view_url": 'https://online.tableau.com/company/AlertingtoSlack/dau'
         },
        {"alerting_name": "other_kpi",
         "query": "other_kpi.sql",
         "view_url": 'https://online.tableau.com/company/AlertingtoSlack/other_kpi'
         }
    ]

    external_tasks_sensors = CompanyCommon.convert_tasks_to_task_sensors(
        ["tableau_refreshes.alerting_to_slack$ds"])

    for alerting in alerting_list:
        task_tableau_view, task_valid_check, task_slack_publish = alerting_tasks(slack_channel=SLACK_CHANNEL,
                                                                                 **alerting)
        external_tasks_sensors >> task_tableau_view >> task_valid_check >> task_slack_publish
