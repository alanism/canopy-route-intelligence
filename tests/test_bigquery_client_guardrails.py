import unittest
from unittest.mock import patch

from services.bigquery_client import dry_run_sql
from services.request_context import reset_request_id, set_request_id


class BigQueryClientGuardrailTests(unittest.TestCase):
    def test_request_scoped_bigquery_is_blocked_by_default(self):
        token = set_request_id("req_guardrail")
        try:
            with self.assertRaises(RuntimeError):
                dry_run_sql(
                    "SELECT transaction_hash FROM dataset.table",
                    query_name="context_graph_edges_ethereum_1h",
                    query_family="context_graph_edges",
                    maximum_bytes_billed=1000,
                    query_classification="derived",
                )
        finally:
            reset_request_id(token)

    @patch("services.bigquery_client.DEV_ONLY_BIGQUERY_ENABLED", False)
    def test_dev_only_query_requires_explicit_runtime_opt_in(self):
        with self.assertRaises(RuntimeError):
            dry_run_sql(
                "SELECT transaction_hash FROM dataset.table",
                query_name="audit_context_graph_live_parity_ethereum_1h",
                query_family="audit_context_graph_parity",
                maximum_bytes_billed=1000,
                query_classification="dev_only",
            )


if __name__ == "__main__":
    unittest.main()
