"""
Tests for core.sparql.post_sparql_with_debug timing metadata.
"""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from core.sparql import post_sparql_with_debug


class TestPostSparqlWithDebugTiming(unittest.TestCase):
    @patch("core.sparql.requests.post")
    def test_success_includes_timing_timeout_and_started_at(self, mock_post):
        response = MagicMock()
        response.status_code = 200
        response.json.return_value = {"head": {"vars": []}, "results": {"bindings": []}}
        mock_post.return_value = response

        result, error, debug = post_sparql_with_debug("federation", "SELECT * WHERE { ?s ?p ?o } LIMIT 1", timeout=7)

        self.assertIsNone(error)
        self.assertIsNotNone(result)
        self.assertEqual(debug.get("timeout_sec"), 7)
        self.assertIn("started_at_utc", debug)
        self.assertTrue(str(debug.get("started_at_utc")).endswith("Z"))
        self.assertIn("elapsed_ms", debug)
        self.assertGreaterEqual(float(debug.get("elapsed_ms")), 0.0)

    @patch("core.sparql.requests.post")
    def test_http_error_includes_elapsed_ms(self, mock_post):
        response = MagicMock()
        response.status_code = 500
        response.text = "server error"
        mock_post.return_value = response

        result, error, debug = post_sparql_with_debug("federation", "ASK { ?s ?p ?o }", timeout=8)

        self.assertIsNone(result)
        self.assertIn("500", str(error))
        self.assertEqual(debug.get("timeout_sec"), 8)
        self.assertIn("elapsed_ms", debug)
        self.assertGreaterEqual(float(debug.get("elapsed_ms")), 0.0)

    @patch("core.sparql.requests.post")
    def test_network_error_includes_elapsed_ms(self, mock_post):
        import requests.exceptions

        mock_post.side_effect = requests.exceptions.RequestException("Connection failed")

        result, error, debug = post_sparql_with_debug("federation", "ASK { ?s ?p ?o }", timeout=5)

        self.assertIsNone(result)
        self.assertIn("Network error", str(error))
        self.assertIn("exception", debug)
        self.assertIn("elapsed_ms", debug)
        self.assertGreaterEqual(float(debug.get("elapsed_ms")), 0.0)


if __name__ == "__main__":
    unittest.main()
