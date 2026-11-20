#!/usr/bin/env python3
"""Regression checks for UN ASFR responses with optional uncertainty bounds.

Run inside the Airflow environment:
    python scripts/test_asfr_variants.py
"""
import contextlib
import io
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

os.environ["ASFR_COUNTRIES"] = "BWA"
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
import unwpp_asfr_5yr as asfr  # noqa: E402


def rows(variant, value, sex_id=3):
    # Match the live UN response: both sexes, mixed-case variant, nine age bins.
    return [
        {"sexId": sex_id, "variantShortName": variant,
         "ageStart": start, "ageEnd": start + 4, "value": value}
        for start in range(10, 55, 5)
    ]


class VariantTests(unittest.TestCase):
    def parse(self, payload):
        with patch.object(asfr, "_get_json", return_value=payload), \
                contextlib.redirect_stdout(io.StringIO()):
            return asfr._fetch_asfr_rows(72, 17, 2026)

    def test_median_only_keeps_seven_reproductive_age_bins(self):
        self.assertEqual(
            self.parse(rows("Median", 42.5)),
            [("MEDIAN", age, age + 4, 42.5) for age in range(15, 50, 5)],
        )

    def test_available_bounds_are_preserved(self):
        output = self.parse(rows("Lower95", 10) + rows("Median", 20) + rows("Upper95", 30))
        self.assertEqual(
            output,
            [(variant, age, age + 4, value)
             for variant, value in [("LOWER95", 10.0), ("MEDIAN", 20.0), ("UPPER95", 30.0)]
             for age in range(15, 50, 5)],
        )

    def test_one_available_bound_is_retained(self):
        output = self.parse(rows("Median", 20) + rows("Upper95", 30))
        self.assertEqual(len(output), 14)
        self.assertEqual({r[0] for r in output}, {"MEDIAN", "UPPER95"})

    def test_bounds_without_median_are_rejected(self):
        self.assertEqual(self.parse(rows("Lower95", 10) + rows("Upper95", 30)), [])

    def test_invalid_median_cannot_leave_only_bounds(self):
        self.assertEqual(self.parse(rows("Median", None) + rows("Upper95", 30)), [])

    def test_estimate_alias_and_zero_are_usable(self):
        output = self.parse(rows("EST", 0))
        self.assertEqual(output, [("MEDIAN", age, age + 4, 0.0) for age in range(15, 50, 5)])

    def test_female_estimates_take_precedence(self):
        output = self.parse(rows("Median", 20, sex_id=3) + rows("Median", 25, sex_id=2))
        self.assertEqual(output, [("MEDIAN", age, age + 4, 25.0) for age in range(15, 50, 5)])


if __name__ == "__main__":
    unittest.main()
