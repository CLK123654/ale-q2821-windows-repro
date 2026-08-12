from __future__ import annotations

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime


CHANNEL_TASKS = {
    "api": "deploy_api",
    "web": "deploy_web",
    "mobile": "deploy_mobile",
}


def choose_release_channels(**context):
    channels = context["dag_run"].conf.get("channels", [])
    return [CHANNEL_TASKS[item] for item in channels] if channels else "no_channel"


def run_channel(channel: str, **context):
    result = context["dag_run"].conf["channel_results"][channel]
    if result == "failed":
        raise AirflowFailException(f"{channel}发布失败")
    if result == "skipped":
        raise AirflowSkipException(f"{channel}发布已跳过")
    if result != "success":
        raise ValueError(f"未知发布结果：{result}")
    return result


def risk_allows_release(**context):
    conf = context["dag_run"].conf
    return bool(conf.get("channels")) and conf.get("risk_decision") == "allow"


with DAG(
    dag_id="internet_multichannel_release",
    start_date=datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    load_manifest = EmptyOperator(task_id="load_manifest")
    choose_channels = BranchPythonOperator(
        task_id="choose_channels",
        python_callable=choose_release_channels,
    )
    deploy_api = PythonOperator(
        task_id="deploy_api",
        python_callable=run_channel,
        op_kwargs={"channel": "api"},
    )
    deploy_web = PythonOperator(
        task_id="deploy_web",
        python_callable=run_channel,
        op_kwargs={"channel": "web"},
    )
    deploy_mobile = PythonOperator(
        task_id="deploy_mobile",
        python_callable=run_channel,
        op_kwargs={"channel": "mobile"},
    )
    no_channel = EmptyOperator(task_id="no_channel")
    selected_join = EmptyOperator(
        task_id="selected_join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    risk_gate = ShortCircuitOperator(
        task_id="risk_gate",
        python_callable=risk_allows_release,
        ignore_downstream_trigger_rules=False,
    )
    promote = EmptyOperator(task_id="promote")
    finalize = EmptyOperator(task_id="finalize", trigger_rule=TriggerRule.ALL_DONE)

    load_manifest >> choose_channels
    choose_channels >> [deploy_api, deploy_web, deploy_mobile, no_channel]
    [deploy_api, deploy_web, deploy_mobile, no_channel] >> selected_join
    selected_join >> risk_gate >> promote >> finalize
