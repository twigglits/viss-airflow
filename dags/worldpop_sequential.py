"""Run the per-country WorldPop ingest DAGs strictly one at a time.

Airflow has no "one DAG at a time" setting. `core.parallelism` caps concurrent
*tasks*, but the scheduler still interleaves those tasks across DAGs, so several
countries end up half-finished at once. This DAG is the queue: one
TriggerDagRunOperator per country, chained, each waiting for its child DAG to
finish before the next one fires.

Trigger this DAG (`worldpop_sequential`); leave the `worldpop_ingest_cog_*` DAGs
unpaused but never trigger them by hand while this is running.
"""
from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ponytail: scrape the country list out of the sibling DAG file instead of
# importing it. That module builds 243 DAGs at import time via `with DAG(...)`,
# which Airflow auto-registers -- importing it here would register them twice.
_SRC = Path(__file__).with_name("worldpop_ingest_all.py").read_text()
_LIST = re.search(r"ISO3_CODES[^=]*=\s*\[(.*?)\]", _SRC, re.S)
ISO3_CODES = re.findall(r"'([A-Z]{3})'", _LIST.group(1)) if _LIST else []

# Same env override the child DAGs honour, so a test run can be one country.
_want = os.getenv("WORLDPOP_COUNTRIES")
if _want:
    keep = {c.strip().upper() for c in _want.split(",") if c.strip()}
    ISO3_CODES = [c for c in ISO3_CODES if c in keep]

with DAG(
    dag_id="worldpop_sequential",
    description=f"Queue: runs {len(ISO3_CODES)} worldpop_ingest_cog_* DAGs one at a time",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"owner": "airflow", "retries": 0},
    tags=["worldpop", "orchestrator"],
) as dag:
    previous = None
    for _cc3 in ISO3_CODES:
        _cc3l = _cc3.lower()
        step = TriggerDagRunOperator(
            task_id=f"run_{_cc3l}",
            trigger_dag_id=f"worldpop_ingest_cog_{_cc3l}",
            wait_for_completion=True,
            # Frees the executor slot while waiting. Without this the waiting
            # task holds the only slot and the child it waits for can never
            # start -- a permanent deadlock at low parallelism.
            deferrable=True,
            poke_interval=60,
            reset_dag_run=True,
            allowed_states=["success"],
            failed_states=["failed"],
            # One bad country marks its own step failed but must not stall the
            # remaining queue.
            trigger_rule="all_done",
        )
        if previous is not None:
            previous >> step
        previous = step
