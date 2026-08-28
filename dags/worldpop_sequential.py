"""Run the per-country WorldPop ingest DAGs, WORLDPOP_LANES countries at a time.

Airflow has no "N DAGs at a time" setting. `core.parallelism` caps concurrent
*tasks* but the scheduler still interleaves them across DAGs, so without a queue
every country ends up half-finished at once. `max_active_tasks` cannot do the job
either: these triggers are deferrable, and a deferred task holds no concurrency
slot, so all 243 would fire together.

So the queue is built out of chains: countries are dealt round-robin into
WORLDPOP_LANES chains, and each chain runs strictly one country at a time. Lanes
run independently, so exactly WORLDPOP_LANES countries are in flight. Set
WORLDPOP_LANES=1 for the old strictly-sequential behaviour.

Sizing a lane: a lane costs one gdalwarp, so the ceiling is host RAM and cores
divided by WORLDPOP_GDAL_CACHEMAX_MB + WORLDPOP_GDAL_WM_MB and
WORLDPOP_GDAL_NUM_THREADS respectively. `core.parallelism` must be at least
2 x WORLDPOP_LANES, since each lane needs a slot for the child task plus a slot
to resume its own trigger task into.

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

LANES = max(1, int(os.getenv("WORLDPOP_LANES", "4")))

with DAG(
    dag_id="worldpop_sequential",
    description=f"Queue: runs {len(ISO3_CODES)} worldpop_ingest_cog_* DAGs, {LANES} at a time",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    # Backstop only -- the chains are what actually limit concurrency, since
    # deferred triggers do not count against this.
    max_active_tasks=LANES,
    # Deliberately no execution_timeout. Every task here is a deferred wait on a
    # child DAG, and the queue as a whole is expected to run for days.
    # A ceiling on these would kill the queue, not a hang.
    # The child DAGs carry their own per-task timeouts.
    default_args={"owner": "airflow", "retries": 0},
    tags=["worldpop", "orchestrator"],
) as dag:
    # One tail per lane. Countries are dealt round-robin so the two monsters
    # (RUS, USA) land in different lanes rather than stacking up behind each other.
    previous = [None] * LANES
    for _i, _cc3 in enumerate(ISO3_CODES):
        _cc3l = _cc3.lower()
        _lane = _i % LANES
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
        if previous[_lane] is not None:
            previous[_lane] >> step
        previous[_lane] = step
