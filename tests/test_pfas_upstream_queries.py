"""
Tests for analyses.pfas_upstream.queries (run_upstream: 3 federation-only queries).

Uses unittest and mocks requests to avoid network calls. Run from project root:
  python -m unittest discover -s tests -p 'test_*.py'
  or: python -m pytest tests/ -v
"""
from __future__ import annotations

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

from analyses.pfas_upstream import queries as upstream_queries


def _sparql_json(vars_list: list, bindings: list) -> dict:
    """Build a minimal SPARQL JSON response."""
    return {
        "head": {"vars": vars_list},
        "results": {"bindings": bindings},
    }


def _binding(**kwargs) -> dict:
    """One row: each key is var name, value is plain string (value used as URI/literal)."""
    return {k: {"value": v, "type": "uri"} for k, v in kwargs.items()}


class TestRunUpstream(unittest.TestCase):
    """run_upstream: 3 self-contained federation queries."""

    @staticmethod
    def _set_three_empty_success(mock_post):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = _sparql_json([], [])
        mock_post.side_effect = [response, response, response]

    def test_returns_error_when_region_empty(self):
        samples_df, up_s2, up_fl, facilities_df, executed, err = upstream_queries.run_upstream(
            substance_uri=None,
            material_uri=None,
            min_conc=0,
            max_conc=500,
            region_code="",
            include_nondetects=False,
        )
        self.assertTrue(samples_df.empty)
        self.assertTrue(up_s2.empty)
        self.assertTrue(up_fl.empty)
        self.assertTrue(facilities_df.empty)
        self.assertEqual(executed, [])
        self.assertIn("Region", err)

    @patch("core.sparql.requests.post")
    def test_returns_dataframes_and_executed_queries_on_success(self, mock_post):
        # Step 1: samples (sp, spWKT, s2cell)
        r1 = MagicMock()
        r1.status_code = 200
        r1.json.return_value = _sparql_json(
            ["sp", "spWKT", "s2cell"],
            [
                _binding(
                    sp="http://ex.org/sp1",
                    spWKT="POINT(-70.4 43.6)",
                    s2cell="http://stko-kwg.geog.ucsb.edu/lod/resource/s2.level13.123",
                ),
            ],
        )
        # Step 2: flowlines
        r2 = MagicMock()
        r2.status_code = 200
        r2.json.return_value = _sparql_json(
            ["upstream_flowline", "us_ftype", "upstream_flowlineWKT"],
            [_binding(upstream_flowline="http://ex.org/fl1", us_ftype="460", upstream_flowlineWKT="LINESTRING(...)")],
        )
        # Step 3: facilities
        r3 = MagicMock()
        r3.status_code = 200
        r3.json.return_value = _sparql_json(
            ["facility", "facWKT", "facilityName", "industryCode", "industryName"],
            [
                _binding(
                    facility="http://w3id.org/fio/v1/epa-frs-data#d.FRS-Facility.123",
                    facWKT="POINT(-70.3 43.6)",
                    facilityName="Test Facility",
                    industryCode="http://w3id.org/fio/v1/naics#NAICS-3323",
                    industryName="Fabricated Metal",
                ),
            ],
        )
        mock_post.side_effect = [r1, r2, r3]

        samples_df, up_s2, up_fl, facilities_df, executed, err = upstream_queries.run_upstream(
            substance_uri=None,
            material_uri=None,
            min_conc=0,
            max_conc=500,
            region_code="23",
            include_nondetects=False,
        )

        self.assertIsNone(err)
        self.assertEqual(len(samples_df), 1)
        self.assertIn("s2cell", samples_df.columns)
        self.assertEqual(len(up_fl), 1)
        self.assertEqual(len(facilities_df), 1)
        self.assertTrue(up_s2.empty)
        self.assertEqual(len(executed), 3)
        self.assertEqual(executed[0].get("label"), "Step 1: PFAS Samples")
        self.assertEqual(executed[1].get("label"), "Step 2: Upstream Flowlines")
        self.assertEqual(executed[2].get("label"), "Step 3: Upstream Facilities")
        for i, eq in enumerate(executed):
            self.assertIn("query", eq)
            self.assertIsInstance(eq["query"], str)
            self.assertIn("SELECT", eq["query"])

    @patch("core.sparql.requests.post")
    def test_executed_queries_contain_exact_query_sent(self, mock_post):
        r = MagicMock()
        r.status_code = 200
        r.json.return_value = _sparql_json(["sp", "spWKT", "s2cell"], [])
        mock_post.return_value = r

        _, _, _, _, executed, _ = upstream_queries.run_upstream(
            substance_uri=None,
            material_uri=None,
            min_conc=0,
            max_conc=500,
            region_code="23",
            include_nondetects=False,
        )

        self.assertEqual(mock_post.call_count, 3)
        for call_idx, eq in enumerate(executed):
            sent_data = mock_post.call_args_list[call_idx][1].get("data", {})
            sent_query = sent_data.get("query", "")
            self.assertEqual(eq["query"], sent_query)

    @patch("core.sparql.requests.post")
    def test_returns_error_when_step1_http_error(self, mock_post):
        r = MagicMock()
        r.status_code = 500
        r.text = "Server error"
        mock_post.return_value = r

        samples_df, up_s2, up_fl, facilities_df, executed, err = upstream_queries.run_upstream(
            substance_uri=None,
            material_uri=None,
            min_conc=0,
            max_conc=500,
            region_code="23",
            include_nondetects=False,
        )

        self.assertTrue(samples_df.empty)
        self.assertEqual(len(executed), 1)
        self.assertIn("500", err)

    @patch("core.sparql.requests.post")
    def test_returns_error_on_network_exception(self, mock_post):
        import requests.exceptions

        mock_post.side_effect = requests.exceptions.RequestException("Connection failed")

        samples_df, up_s2, up_fl, facilities_df, executed, err = upstream_queries.run_upstream(
            substance_uri=None,
            material_uri=None,
            min_conc=0,
            max_conc=500,
            region_code="23",
            include_nondetects=False,
        )

        self.assertTrue(samples_df.empty)
        self.assertEqual(len(executed), 1)
        self.assertIn("Network error", err)
        self.assertIsNotNone(executed[0].get("error") or executed[0].get("exception"))

    @patch("core.sparql.requests.post")
    def test_step3_query_has_no_naics_values_when_filter_not_selected(self, mock_post):
        self._set_three_empty_success(mock_post)

        _, _, _, _, executed, err = upstream_queries.run_upstream(
            substance_uri=None,
            material_uri=None,
            min_conc=0,
            max_conc=500,
            region_code="23",
            include_nondetects=False,
            naics_code=None,
        )

        self.assertIsNone(err)
        q3 = executed[2]["query"]
        self.assertNotIn("VALUES ?industrySector", q3)
        self.assertNotIn("VALUES ?industrySubsector", q3)
        self.assertNotIn("VALUES ?industryGroup", q3)
        self.assertNotIn("VALUES ?industryCode {naics:NAICS-", q3)

    @patch("core.sparql.requests.post")
    def test_step3_query_includes_hierarchy_when_sector_filter_selected(self, mock_post):
        self._set_three_empty_success(mock_post)

        _, _, _, _, executed, err = upstream_queries.run_upstream(
            substance_uri=None,
            material_uri=None,
            min_conc=0,
            max_conc=500,
            region_code="23",
            include_nondetects=False,
            naics_code="31",
        )

        self.assertIsNone(err)
        q3 = executed[2]["query"]
        self.assertIn("VALUES ?industrySector {naics:NAICS-31}.", q3)
        self.assertIn("?industryGroup fio:subcodeOf ?industrySubsector .", q3)
        self.assertIn("?industrySubsector fio:subcodeOf ?industrySector .", q3)

    @patch("core.sparql.requests.post")
    def test_step3_query_includes_exact_values_for_industry_code(self, mock_post):
        self._set_three_empty_success(mock_post)

        _, _, _, _, executed, err = upstream_queries.run_upstream(
            substance_uri=None,
            material_uri=None,
            min_conc=0,
            max_conc=500,
            region_code="23",
            include_nondetects=False,
            naics_code="332311",
        )

        self.assertIsNone(err)
        q3 = executed[2]["query"]
        self.assertIn("VALUES ?industryCode {naics:NAICS-332311}.", q3)
        self.assertIn("?industryCode a naics:NAICS-IndustryCode ;", q3)

    @patch("core.sparql.requests.post")
    def test_step3_query_ignores_invalid_non_numeric_naics_code(self, mock_post):
        self._set_three_empty_success(mock_post)

        _, _, _, _, executed, err = upstream_queries.run_upstream(
            substance_uri=None,
            material_uri=None,
            min_conc=0,
            max_conc=500,
            region_code="23",
            include_nondetects=False,
            naics_code="31-33",
        )

        self.assertIsNone(err)
        q3 = executed[2]["query"]
        self.assertNotIn("NAICS-31-33", q3)
        self.assertNotIn("VALUES ?industrySector", q3)
        self.assertNotIn("?industryCode a naics:NAICS-IndustryCode ;", q3)


if __name__ == "__main__":
    unittest.main()
