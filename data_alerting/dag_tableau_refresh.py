#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- airflow: DAG -*-

from datetime import datetime

import pendulum

from processing.common.dags.tableau_refresh_dag import TableauRefreshDag
from processing.common.utils.happn import HappnCommon
from processing.common.utils.models import CronPresets

local_tz = pendulum.timezone("Europe/Paris")

with TableauRefreshDag(
        dag_id="tableau_refreshes.alerting_to_slack",
        tableau_project="Data Slack Reports",
        tableau_workbook="Alerting to Slack",
        is_incremental=False,
        description="Full Refresh of KPIs Alerting dashboard",
        external_dag_requires=[
            'stats.bi_dau$ds',
            'stats.bi_registered$ds',
            'stats.bi_sender_activity$ds',
            'datamart.dau$ds',
            'datamart.activity$ds',
            'datamart.user_registration$ds',
            'stats.bi_crushes$ds',
            'datamart.crossings$ds'
        ],
        default_args=HappnCommon.get_default_args(retries=1),
        schedule_interval='0 8 * * *',
        start_date=datetime(2021, 10, 17, tzinfo=local_tz)) as dag:
    pass
