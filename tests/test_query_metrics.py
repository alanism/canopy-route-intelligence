import unittest

from services.query_metrics import (
    get_query_metrics_snapshot,
    record_query_metric,
    reset_query_metrics,
)


class QueryMetricsTests(unittest.TestCase):
    def setUp(self):
        reset_query_metrics()

    def test_records_metrics_by_query_family(self):
        record_query_metric(
            phase="dry_run",
            query_name="measured_fee_extraction_ethereum_usdc_24h",
            query_classification="measured",
            bytes_processed=100,
            maximum_bytes_billed=400,
        )
        record_query_metric(
            phase="execution",
            query_name="context_graph_edges_ethereum_1h",
            query_classification="derived",
            bytes_processed=200,
            maximum_bytes_billed=500,
            execution_time=1.25,
        )

        snapshot = get_query_metrics_snapshot()

        self.assertEqual(snapshot["overall"]["dry_run_count"], 1)
        self.assertEqual(snapshot["overall"]["execution_count"], 1)
        self.assertEqual(snapshot["families"]["fee_activity"]["dry_run_bytes"], 100)
        self.assertEqual(snapshot["families"]["context_graph_edges"]["execution_bytes"], 200)
        self.assertEqual(snapshot["families"]["context_graph_edges"]["avg_execution_time"], 1.25)
        self.assertEqual(snapshot["families"]["fee_activity"]["max_budget_utilization"], 0.25)


if __name__ == "__main__":
    unittest.main()
