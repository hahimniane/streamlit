"""
Tests for runtime ETA estimation and telemetry persistence.
"""
from __future__ import annotations

import os
import tempfile
import unittest

from core.runtime_eta import (
    EtaRequest,
    StepEta,
    concentration_bin,
    estimate_eta,
    list_analysis_steps,
    record_query_runtime,
)
from core.runtime_store import (
    fetch_prediction_errors_percent,
    fetch_query_elapsed_ms,
    initialize_runtime_store,
    insert_probe_runtime_event,
    insert_query_runtime_event,
    prune_old_events,
)


class TestRuntimeEta(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "runtime_eta_test.sqlite3")
        self._old_env = os.environ.get("SAWGRAPH_RUNTIME_DB_PATH")
        os.environ["SAWGRAPH_RUNTIME_DB_PATH"] = self.db_path
        initialize_runtime_store()

    def tearDown(self):
        if self._old_env is None:
            os.environ.pop("SAWGRAPH_RUNTIME_DB_PATH", None)
        else:
            os.environ["SAWGRAPH_RUNTIME_DB_PATH"] = self._old_env
        self.tmpdir.cleanup()

    def _insert_downstream_step1(self, n: int, state_code: str = "23"):
        for i in range(n):
            insert_query_runtime_event(
                {
                    "analysis_key": "downstream",
                    "step_label": "Step 1: Facilities",
                    "endpoint_key": "federation",
                    "status": "success",
                    "elapsed_ms": 1000 + (i * 5),
                    "region_level": "state",
                    "state_code": state_code,
                    "naics_prefix2": "31",
                    "include_nondetects": 0,
                    "has_substance_filter": 0,
                    "has_material_filter": 0,
                    "conc_bin": "50-500",
                }
            )

    def test_bucket_lookup_falls_back_when_state_differs(self):
        self._insert_downstream_step1(12, state_code="23")

        req_exact = EtaRequest(
            analysis_key="downstream",
            step_labels=["Step 1: Facilities"],
            endpoint_key="federation",
            region_level="state",
            state_code="23",
            naics_prefix2="31",
            include_nondetects=False,
            has_substance_filter=False,
            has_material_filter=False,
            conc_bin="50-500",
        )
        eta_exact = estimate_eta(req_exact)
        self.assertEqual(eta_exact.step_estimates[0].sample_size, 12)
        self.assertEqual(eta_exact.source, "historical")

        req_other_state = EtaRequest(
            analysis_key="downstream",
            step_labels=["Step 1: Facilities"],
            endpoint_key="federation",
            region_level="state",
            state_code="99",
            naics_prefix2="31",
            include_nondetects=False,
            has_substance_filter=False,
            has_material_filter=False,
            conc_bin="50-500",
        )
        eta_fallback = estimate_eta(req_other_state)
        self.assertEqual(eta_fallback.step_estimates[0].sample_size, 12)

    def test_probe_factor_scales_estimate_and_probe_failure_downgrades_confidence(self):
        self._insert_downstream_step1(20, state_code="23")
        for _ in range(20):
            insert_probe_runtime_event(
                {
                    "endpoint_key": "federation",
                    "elapsed_ms": 100.0,
                    "status": "success",
                }
            )

        base_request = EtaRequest(
            analysis_key="downstream",
            step_labels=["Step 1: Facilities"],
            endpoint_key="federation",
            region_level="state",
            state_code="23",
            naics_prefix2="31",
            include_nondetects=False,
            has_substance_filter=False,
            has_material_filter=False,
            conc_bin="50-500",
        )
        base_eta = estimate_eta(base_request)

        probe_request = EtaRequest(
            analysis_key=base_request.analysis_key,
            step_labels=base_request.step_labels,
            endpoint_key=base_request.endpoint_key,
            region_level=base_request.region_level,
            state_code=base_request.state_code,
            naics_prefix2=base_request.naics_prefix2,
            include_nondetects=base_request.include_nondetects,
            has_substance_filter=base_request.has_substance_filter,
            has_material_filter=base_request.has_material_filter,
            conc_bin=base_request.conc_bin,
            probe_elapsed_ms=180.0,
            probe_status="success",
        )
        probe_eta = estimate_eta(probe_request)
        self.assertGreater(probe_eta.total_mid_s, base_eta.total_mid_s * 1.5)

        failed_probe_request = EtaRequest(
            analysis_key=base_request.analysis_key,
            step_labels=base_request.step_labels,
            endpoint_key=base_request.endpoint_key,
            region_level=base_request.region_level,
            state_code=base_request.state_code,
            naics_prefix2=base_request.naics_prefix2,
            include_nondetects=base_request.include_nondetects,
            has_substance_filter=base_request.has_substance_filter,
            has_material_filter=base_request.has_material_filter,
            conc_bin=base_request.conc_bin,
            probe_elapsed_ms=None,
            probe_status="error",
        )
        failed_eta = estimate_eta(failed_probe_request)
        self.assertEqual(failed_eta.step_estimates[0].confidence, "low")

    def test_accuracy_label_switches_to_estimated_when_error_target_met(self):
        for i in range(30):
            insert_query_runtime_event(
                {
                    "analysis_key": "near_facilities",
                    "step_label": "Step 1: Facilities",
                    "endpoint_key": "federation",
                    "status": "success",
                    "elapsed_ms": 1000 + (i % 3) * 20,
                    "region_level": "state",
                    "state_code": "23",
                    "naics_prefix2": "31",
                    "include_nondetects": 0,
                    "has_substance_filter": 0,
                    "has_material_filter": 0,
                    "conc_bin": "50-500",
                    "pred_mid_ms": 1000.0,
                }
            )

        request = EtaRequest(
            analysis_key="near_facilities",
            step_labels=["Step 1: Facilities"],
            endpoint_key="federation",
            region_level="state",
            state_code="23",
            naics_prefix2="31",
            include_nondetects=False,
            has_substance_filter=False,
            has_material_filter=False,
            conc_bin=concentration_bin(0, 500),
        )
        eta = estimate_eta(request)
        self.assertEqual(eta.estimate_label, "Estimated time")

    def test_record_and_prune_runtime_events(self):
        request = EtaRequest(
            analysis_key="upstream",
            step_labels=list_analysis_steps("upstream"),
            endpoint_key="federation",
            region_level="county",
            state_code="23",
            naics_prefix2=None,
            include_nondetects=False,
            has_substance_filter=True,
            has_material_filter=True,
            conc_bin="50-500",
        )
        predicted = StepEta(
            label="Step 1: PFAS Samples",
            low_s=1.0,
            mid_s=1.2,
            high_s=1.5,
            confidence="medium",
            sample_size=20,
        )
        record_query_runtime(
            request=request,
            step_label="Step 1: PFAS Samples",
            status="success",
            elapsed_ms=1500.0,
            row_count=42,
            predicted_step=predicted,
        )
        errors = fetch_prediction_errors_percent("upstream", endpoint_key="federation", days=30)
        self.assertEqual(len(errors), 1)
        self.assertGreater(errors[0], 20.0)

        insert_query_runtime_event(
            {
                "created_at_utc": "2000-01-01T00:00:00Z",
                "analysis_key": "upstream",
                "step_label": "Step 1: PFAS Samples",
                "endpoint_key": "federation",
                "status": "success",
                "elapsed_ms": 999.0,
            }
        )
        prune_old_events(retention_days=90)
        values = fetch_query_elapsed_ms(
            analysis_key="upstream",
            step_label="Step 1: PFAS Samples",
            endpoint_key="federation",
            statuses=("success",),
            days=3650,
        )
        self.assertEqual(len(values), 1)
        self.assertAlmostEqual(values[0], 1500.0, places=2)


if __name__ == "__main__":
    unittest.main()
