"""Audited coverage backfill, one child DAG per queue at a time.

The checked-in plan records the gaps found on 2026-11-21 (South Africa time).
Trigger this DAG once. Fertility and health each run one country at a time;
rasters run one country at a time, with one year per country at a time.
Idle sensors also wait for existing/manual runs in the same family, including
the New Zealand age-pyramid retry that was already running when this was queued.

Both sensors and child-DAG waits release executor slots while waiting. A failed
child does not stop the remaining queue, but the final summary fails so errors
are visible. No schedule: this is an explicit maintenance batch.
"""
from collections import Counter
from datetime import datetime, timedelta
import json
from pathlib import Path

from sqlalchemy import or_

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.session import create_session


PLAN = json.loads(Path(__file__).with_name("backfill_remaining_plan.json").read_text())
PREFIXES = {
    "fertility": ["unwpp_asfr_5yr_"],
    "health": ["worldbank_health_", "who_gho_disease_burden_", "opensky_reference_data"],
    "rasters": ["worldpop_age_pyramid_5yr_"],
}
CHILD_TASKS = {}


def _queue_idle(queue: str) -> bool:
    with create_session() as session:
        active = (
            session.query(DagRun.dag_id, DagRun.run_id)
            .filter(
                DagRun.state.in_(["queued", "running"]),
                or_(*(DagRun.dag_id.startswith(p, autoescape=True) for p in PREFIXES[queue])),
            )
            .limit(10)
            .all()
        )
    if active:
        print(f"Waiting for {queue}: {active}")
    return not active


def _summarize(**context):
    states = {
        ti.task_id: ti.state
        for ti in context["dag_run"].get_task_instances()
        if ti.task_id in CHILD_TASKS
    }
    failed = {CHILD_TASKS[k]: states.get(k) for k in CHILD_TASKS if states.get(k) != "success"}
    print(json.dumps({"child_task_states": dict(Counter(states.values())), "unsuccessful": failed}))
    if failed:
        raise AirflowException(f"{len(failed)} child DAGs need follow-up; see the summary above")


with DAG(
    dag_id="backfill_remaining",
    description="Fill audited gaps: one fertility, one health, and one raster job at a time",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    default_args={"owner": "airflow", "retries": 0},
    tags=["maintenance", "backfill", "coverage"],
) as dag:
    children = []
    for queue, prefixes in PREFIXES.items():
        previous = None
        for entry in PLAN[queue]:
            child_id = entry["dag_id"]
            task_id = f"{queue}_{child_id}"
            idle = PythonSensor(
                task_id=f"wait_{task_id}",
                python_callable=_queue_idle,
                op_kwargs={"queue": queue},
                mode="reschedule",
                poke_interval=60,
                timeout=timedelta(days=30).total_seconds(),
                # Continue after a failed child, but always pass a fresh idle
                # check before triggering anything else in this queue.
                trigger_rule="all_done",
            )
            child = TriggerDagRunOperator(
                task_id=task_id,
                trigger_dag_id=child_id,
                trigger_run_id="coverage_batch__{{ run_id }}",
                conf=entry.get("conf", {}),
                wait_for_completion=True,
                deferrable=True,
                poke_interval=60,
                reset_dag_run=False,
                allowed_states=["success"],
                failed_states=["failed"],
                # A failed idle check must never authorize a launch.
                trigger_rule="all_success",
            )
            if previous is not None:
                previous >> idle
            idle >> child
            previous = child
            children.append(child)
            CHILD_TASKS[task_id] = child_id

    summary = PythonOperator(
        task_id="summarize",
        python_callable=_summarize,
        trigger_rule="all_done",
    )
    children >> summary
