#!/usr/bin/env python3
"""Check queue ordering and safe age-pyramid retries in the Airflow image."""
import contextlib
from datetime import timedelta
import io
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

os.environ["AGEPYR_COUNTRIES"] = "RUS"
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
import backfill_remaining as queue  # noqa: E402
import worldpop_age_pyramid as ages  # noqa: E402
from airflow.exceptions import AirflowException, AirflowSkipException  # noqa: E402


class QueueTests(unittest.TestCase):
    def test_every_launch_waits_for_its_predecessor_and_an_idle_check(self):
        roots = []
        for lane in queue.PREFIXES:
            previous = None
            for entry in queue.PLAN[lane]:
                child = queue.dag.get_task(f"{lane}_{entry['dag_id']}")
                idle = queue.dag.get_task(f"wait_{child.task_id}")
                self.assertEqual(child.upstream_task_ids, {idle.task_id})
                self.assertEqual(idle.upstream_task_ids, {previous} if previous else set())
                self.assertEqual(child.trigger_rule, "all_success")
                self.assertEqual(idle.trigger_rule, "all_done")
                self.assertEqual(idle.mode, "reschedule")
                self.assertTrue(child.wait_for_completion)
                self.assertTrue(child._defer)
                if previous is None:
                    roots.append(idle.task_id)
                previous = child.task_id
        self.assertEqual(set(roots), {task.task_id for task in queue.dag.roots})
        self.assertEqual(len(roots), 3)
        self.assertEqual(queue.dag.max_active_runs, 1)

    def test_plan_has_no_duplicate_child_dags(self):
        ids = list(queue.CHILD_TASKS.values())
        self.assertEqual(len(ids), len(set(ids)))
        excluded = {row["dag_id"] for row in queue.PLAN["excluded"]}
        self.assertFalse(set(ids) & excluded)

    def test_existing_work_blocks_a_queue(self):
        session = MagicMock()
        active_query = session.query.return_value.filter.return_value.limit.return_value
        with patch.object(queue, "create_session") as factory:
            factory.return_value.__enter__.return_value = session
            active_query.all.return_value = [("worldpop_age_pyramid_5yr_nzl_2015_2026", "existing")]
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertFalse(queue._queue_idle("rasters"))
            active_query.all.return_value = []
            self.assertTrue(queue._queue_idle("rasters"))

    def test_final_summary_cannot_hide_a_failed_child(self):
        tasks = [SimpleNamespace(task_id=task_id, state="success") for task_id in queue.CHILD_TASKS]
        run = SimpleNamespace(get_task_instances=lambda: tasks)
        with contextlib.redirect_stdout(io.StringIO()):
            queue._summarize(dag_run=run)
            tasks[0].state = "failed"
            with self.assertRaises(AirflowException):
                queue._summarize(dag_run=run)


class AgeRetryTests(unittest.TestCase):
    def test_country_years_run_serially_with_a_longer_timeout(self):
        self.assertEqual(ages.dag.max_active_tasks, 1)
        self.assertEqual(ages.dag.max_active_runs, 1)
        self.assertEqual(ages.TASK_TIMEOUT, timedelta(hours=12))

    def stored(self, rows):
        with patch.object(ages, "PostgresHook") as hook:
            conn = hook.return_value.get_conn.return_value.__enter__.return_value
            conn.cursor.return_value.__enter__.return_value.fetchall.return_value = rows
            return ages._stored_age_pyramid("RUS", 2026)

    def test_only_complete_age_bins_are_skipped(self):
        expected = [(label, float(index)) for index, (label, _) in enumerate(ages._rebin_to_5yr({}))]
        self.assertEqual(self.stored(list(reversed(expected))), expected)
        self.assertEqual(self.stored(expected[:-1]), [])
        self.assertEqual(self.stored(expected[:-1] + [("unexpected", 0.0)]), [])

    def test_unselected_year_does_no_work(self):
        with patch.object(ages, "get_current_context", return_value={"dag_run": SimpleNamespace(conf={"years": [2026]})}), \
                patch.object(ages, "_ensure_db_tables_exist") as ensure:
            with self.assertRaises(AirflowSkipException):
                ages.build_age_pyramid_year("RUS", 2025)
            ensure.assert_not_called()

    def test_malformed_year_selection_is_rejected(self):
        with patch.object(ages, "get_current_context", return_value={"dag_run": SimpleNamespace(conf={"years": "2026"})}):
            with self.assertRaises(ValueError):
                ages.build_age_pyramid_year("RUS", 2026)

    def test_complete_year_avoids_raster_work_and_can_export_csv(self):
        conf = {}
        bins = ages._rebin_to_5yr({})
        with patch.object(ages, "get_current_context", return_value={"dag_run": SimpleNamespace(conf=conf)}), \
                patch.object(ages, "_ensure_db_tables_exist"), \
                patch.object(ages, "_stored_age_pyramid", return_value=bins), \
                patch.object(ages, "_download") as download, \
                patch.object(ages, "_write_csv", return_value="cached.csv") as write:
            with self.assertRaises(AirflowSkipException):
                ages.build_age_pyramid_year("RUS", 2026)
            conf["write_csv"] = True
            self.assertEqual(ages.build_age_pyramid_year("RUS", 2026), "cached.csv")
            write.assert_called_once_with("RUS", 2026, bins)
            download.assert_not_called()

    def test_force_rebuilds_a_complete_year(self):
        with patch.object(ages, "get_current_context", return_value={"dag_run": SimpleNamespace(conf={"force": True})}), \
                patch.object(ages, "_ensure_db_tables_exist"), \
                patch.object(ages, "_ensure_tools"), \
                patch.object(ages, "_stored_age_pyramid") as stored, \
                patch.object(ages, "_download") as download, \
                patch.object(ages, "recompress", return_value=("skip", 1, 1)), \
                patch.object(ages, "_rio_sum_tif", return_value=1.0), \
                patch.object(ages, "_upsert_age_pyramid_5yr") as save, \
                contextlib.redirect_stdout(io.StringIO()):
            ages.build_age_pyramid_year("RUS", 2026)
            stored.assert_not_called()
            self.assertEqual(download.call_count, 20)
            save.assert_called_once()
            self.assertEqual(len(save.call_args.args[2]), 17)


if __name__ == "__main__":
    unittest.main()
