import unittest

from services.query_validator import validate_query


class QueryValidatorTests(unittest.TestCase):
    def test_measured_query_requires_partition_filter(self):
        issues = validate_query(
            "SELECT transaction_hash FROM dataset.table WHERE token = 'USDC'",
            classification="measured",
            query_name="measured_fee_extraction_ethereum_usdc_24h",
            query_family="fee_activity",
            maximum_bytes_billed=1000,
        )
        self.assertTrue(any(issue.code == "missing_partition_filter" for issue in issues))

    def test_measured_query_rejects_derived_patterns(self):
        issues = validate_query(
            """
            SELECT
              transaction_hash,
              SUM(value) AS volume_usdc,
              AVG(fee) AS avg_fee_usd
            FROM dataset.table
            WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            GROUP BY transaction_hash
            """,
            classification="measured",
            query_name="measured_fee_extraction_ethereum_usdc_24h",
            query_family="fee_activity",
            maximum_bytes_billed=1000,
        )
        self.assertTrue(any(issue.code == "forbidden_measured_pattern" for issue in issues))

    def test_derived_query_is_not_blocked_by_measured_rules(self):
        issues = validate_query(
            "SELECT AVG(value) AS avg_fee_usd FROM dataset.table",
            classification="derived",
            query_name="context_graph_edges_ethereum_1h",
            query_family="context_graph_edges",
            maximum_bytes_billed=1000,
        )
        self.assertEqual(issues, [])

    def test_measured_query_rejects_join_before_prefilter(self):
        issues = validate_query(
            """
            SELECT t.hash, r.gas_used
            FROM dataset.transactions t
            JOIN dataset.receipts r
              ON t.hash = r.transaction_hash
            WHERE t.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            """,
            classification="measured",
            query_name="measured_fee_extraction_ethereum_usdc_24h",
            query_family="fee_activity",
            maximum_bytes_billed=1000,
        )
        self.assertTrue(any(issue.code == "join_before_filter" for issue in issues))
        self.assertTrue(any(issue.code == "missing_prefiltered_join_keys" for issue in issues))

    def test_measured_query_requires_bounded_window_interval(self):
        issues = validate_query(
            """
            WITH transfer_hashes AS (
              SELECT transaction_hash
              FROM dataset.events
              WHERE block_timestamp >= some_runtime_value
            )
            SELECT transaction_hash FROM transfer_hashes
            """,
            classification="measured",
            query_name="measured_fee_extraction_ethereum_usdc_24h",
            query_family="fee_activity",
            maximum_bytes_billed=1000,
        )
        self.assertTrue(any(issue.code == "unbounded_window" for issue in issues))

    def test_measured_query_allows_prefiltered_join_shape(self):
        issues = validate_query(
            """
            WITH transfer_hashes AS (
              SELECT DISTINCT transaction_hash
              FROM dataset.events
              WHERE block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            ),
            tx_context AS (
              SELECT t.transaction_hash
              FROM transfer_hashes hashes
              JOIN dataset.transactions t
                ON hashes.transaction_hash = t.transaction_hash
              WHERE t.block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            )
            SELECT transaction_hash FROM tx_context
            """,
            classification="measured",
            query_name="measured_fee_extraction_ethereum_usdc_24h",
            query_family="fee_activity",
            maximum_bytes_billed=1000,
        )
        self.assertFalse(any(issue.code == "join_before_filter" for issue in issues))
        self.assertFalse(any(issue.code == "missing_prefiltered_join_keys" for issue in issues))

    def test_query_requires_metadata(self):
        issues = validate_query("SELECT 1", classification="derived")
        self.assertTrue(any(issue.code == "missing_query_name" for issue in issues))
        self.assertTrue(any(issue.code == "missing_query_family" for issue in issues))
        self.assertTrue(any(issue.code == "missing_max_bytes_billed" for issue in issues))

    def test_derived_query_rejects_request_scoped_execution(self):
        issues = validate_query(
            "SELECT transaction_hash FROM dataset.table",
            classification="derived",
            query_name="context_graph_edges_ethereum_1h",
            query_family="context_graph_edges",
            maximum_bytes_billed=1000,
            request_scoped=True,
        )
        self.assertTrue(any(issue.code == "derived_request_path_forbidden" for issue in issues))


if __name__ == "__main__":
    unittest.main()
