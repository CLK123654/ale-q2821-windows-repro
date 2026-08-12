from __future__ import annotations

import csv
import json
import os
import shutil
from pathlib import Path

import pendulum
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session


OUTPUT_ROOT = Path(__file__).resolve().parents[1]
INPUT_ROOT = Path(os.environ.get("ALE_INPUT_ROOT", OUTPUT_ROOT.parent / "input_data")).resolve()
RESULTS_ROOT = Path(os.environ.get("ALE_RESULTS_ROOT", OUTPUT_ROOT / "results")).resolve()
STAGE_ROOT = RESULTS_ROOT.parent / f".{RESULTS_ROOT.name}.stage-{os.getpid()}"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def graph(dag) -> tuple[list[str], list[list[str]]]:
    tasks = [task.task_id for task in dag.tasks]
    edges = sorted(
        [task.task_id, downstream]
        for task in dag.tasks
        for downstream in task.downstream_task_ids
    )
    return tasks, edges


def load_materials():
    policy = json.loads((INPUT_ROOT / "release_policy.json").read_text(encoding="utf-8"))
    batches = read_csv(INPUT_ROOT / "release_batches.csv")
    batch_ids = [row["batch_id"] for row in batches]
    if not batch_ids or len(batch_ids) != len(set(batch_ids)) or any(not item for item in batch_ids):
        raise ValueError("batch_id必须非空且唯一")
    logical_dates = [row["logical_date"] for row in batches]
    if len(logical_dates) != len(set(logical_dates)):
        raise ValueError("logical_date必须唯一")
    dagbag = DagBag(dag_folder=str(OUTPUT_ROOT / "dags"), include_examples=False, safe_mode=False)
    if dagbag.import_errors:
        raise RuntimeError(str(dagbag.import_errors))
    dag = dagbag.get_dag(policy["dag_id"])
    if dag is None:
        raise RuntimeError("未找到交付DAG")
    task_ids, edges = graph(dag)
    if task_ids != policy["task_order"]:
        raise ValueError("DAG任务顺序与发布规则不一致")
    if edges != policy["expected_edges"]:
        raise ValueError("DAG任务依赖与发布规则不一致")
    if dag.schedule_interval != policy["schedule"] or dag.catchup != policy["catchup"]:
        raise ValueError("DAG调度配置与发布规则不一致")
    if dag.max_active_runs != policy["max_active_runs"]:
        raise ValueError("DAG并发配置与发布规则不一致")
    if dag.get_task(policy["join_task"]).trigger_rule != policy["join_trigger_rule"]:
        raise ValueError("汇合触发规则不一致")
    risk_task = dag.get_task(policy["risk_task"])
    if risk_task.ignore_downstream_trigger_rules != policy["short_circuit_ignore_downstream_trigger_rules"]:
        raise ValueError("风险短路配置不一致")
    if dag.get_task(policy["final_task"]).trigger_rule != policy["final_trigger_rule"]:
        raise ValueError("收尾触发规则不一致")
    return policy, batches, dag, task_ids, edges


def batch_conf(row: dict[str, str], policy: dict) -> dict:
    channels = [] if row["channels"] == "none" else row["channels"].split(";")
    unknown = sorted(set(channels) - set(policy["channel_task_map"]))
    if unknown:
        raise ValueError(f"未知发布通道：{','.join(unknown)}")
    results = {channel: row[f"{channel}_result"] for channel in policy["channel_task_map"]}
    for channel, result in results.items():
        if channel in channels and result not in {"success", "failed", "skipped"}:
            raise ValueError(f"已选通道结果无效：{channel}")
        if channel not in channels and result != "not_selected":
            raise ValueError(f"未选通道结果无效：{channel}")
    if row["risk_decision"] not in {"allow", "block"}:
        raise ValueError("risk_decision无效")
    return {
        "batch_id": row["batch_id"],
        "channels": channels,
        "channel_results": results,
        "risk_decision": row["risk_decision"],
        "release_note": row["release_note"],
    }


def main() -> None:
    policy, batches, dag, task_ids, edges = load_materials()
    if STAGE_ROOT.exists():
        shutil.rmtree(STAGE_ROOT)
    STAGE_ROOT.mkdir(parents=True)

    batch_rows = []
    ti_rows = []
    for row in batches:
        logical_date = pendulum.parse(row["logical_date"])
        conf = batch_conf(row, policy)
        dagrun = dag.test(execution_date=logical_date, run_conf=conf, use_executor=False)
        with create_session() as session:
            stored = (
                session.query(DagRun)
                .filter(DagRun.dag_id == dag.dag_id, DagRun.execution_date == logical_date)
                .one()
            )
            instances = (
                session.query(TaskInstance)
                .filter(TaskInstance.dag_id == dag.dag_id, TaskInstance.run_id == stored.run_id)
                .all()
            )
            states = {item.task_id: item.state for item in instances}
            for task_id in task_ids:
                ti_rows.append(
                    {
                        "batch_id": row["batch_id"],
                        "logical_date": row["logical_date"],
                        "task_id": task_id,
                        "state": states.get(task_id),
                    }
                )
            batch_rows.append(
                {
                    **row,
                    "dagrun_state": stored.state,
                    "selected_join_state": states.get(policy["join_task"]),
                    "risk_gate_state": states.get(policy["risk_task"]),
                    "promote_state": states.get(policy["promote_task"]),
                    "finalize_state": states.get(policy["final_task"]),
                }
            )

    with create_session() as session:
        runs = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag.dag_id)
            .order_by(DagRun.execution_date)
            .all()
        )
        dagrun_rows = [
            {
                "run_id": item.run_id,
                "logical_date": item.execution_date.isoformat().replace("+00:00", "Z"),
                "state": item.state,
                "run_type": item.run_type,
                "external_trigger": str(bool(item.external_trigger)).lower(),
            }
            for item in runs
        ]

    write_csv(
        STAGE_ROOT / "batch_status.csv",
        [
            "batch_id", "logical_date", "channels", "api_result", "web_result", "mobile_result",
            "risk_decision", "release_note", "dagrun_state", "selected_join_state",
            "risk_gate_state", "promote_state", "finalize_state",
        ],
        batch_rows,
    )
    write_csv(
        STAGE_ROOT / "task_instance_history.csv",
        ["batch_id", "logical_date", "task_id", "state"],
        ti_rows,
    )
    write_csv(
        STAGE_ROOT / "dagrun_history.csv",
        ["run_id", "logical_date", "state", "run_type", "external_trigger"],
        dagrun_rows,
    )
    structure = {
        "dag_id": dag.dag_id,
        "schedule": dag.schedule_interval,
        "catchup": dag.catchup,
        "max_active_runs": dag.max_active_runs,
        "tasks": task_ids,
        "edges": edges,
        "join_trigger_rule": dag.get_task(policy["join_task"]).trigger_rule,
        "short_circuit_ignore_downstream_trigger_rules": dag.get_task(policy["risk_task"]).ignore_downstream_trigger_rules,
        "final_trigger_rule": dag.get_task(policy["final_task"]).trigger_rule,
    }
    (STAGE_ROOT / "dag_structure.json").write_text(
        json.dumps(structure, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if any(row["finalize_state"] != "success" for row in batch_rows):
        raise RuntimeError("存在未完成收尾的发布批次")
    if RESULTS_ROOT.exists():
        shutil.rmtree(RESULTS_ROOT)
    STAGE_ROOT.rename(RESULTS_ROOT)


if __name__ == "__main__":
    main()
