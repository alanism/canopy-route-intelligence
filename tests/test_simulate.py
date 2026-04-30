import asyncio
import unittest

from models.request_models import SimulateRequest
from services.routing_engine import build_route_graph, select_best_execution_route
from services.state_mirror import SNAPSHOT_TTL_SECONDS, get_state_snapshot
from api.simulate import run_simulation


class StateMirrorTests(unittest.TestCase):
    def test_snapshot_is_reused_within_ttl(self):
        first = get_state_snapshot(force_refresh=True)
        second = get_state_snapshot()
        self.assertEqual(first["snapshot_id"], second["snapshot_id"])
        self.assertLessEqual(second["data_freshness"]["snapshot_age_sec"], SNAPSHOT_TTL_SECONDS)


class RoutingEngineTests(unittest.TestCase):
    def test_bellman_ford_is_used_when_negative_edge_exists(self):
        snapshot = get_state_snapshot(force_refresh=True)
        snapshot["bridge_config"]["LayerZero"]["incentive_usd"] = 200.0
        request = SimulateRequest(
            amount=5_000,
            token="USDC",
            source_chain="Ethereum",
            destination_chain="Arbitrum",
            preference="cheapest",
        )
        edges, _, _ = build_route_graph(request, snapshot)
        self.assertTrue(any(edge.weight < 0 for edge in edges))
        result = select_best_execution_route(request, snapshot)
        self.assertEqual(result["algorithm_used"], "bellman-ford")


class SimulateApiTests(unittest.TestCase):
    def test_simulation_response_contains_quote_and_execution_plan(self):
        request = SimulateRequest(
            amount=10_000,
            token="USDC",
            source_chain="Ethereum",
            destination_chain="Polygon",
            slippage_tolerance=0.01,
            preference="balanced",
        )
        response = asyncio.run(run_simulation(request))
        payload = response.model_dump()

        self.assertIn("simulation_id", payload)
        self.assertIn("quote", payload)
        self.assertIn("execution_plan", payload)
        self.assertIn("risk_profile", payload)
        self.assertIn("data_freshness", payload)
        self.assertEqual(payload["quote"]["quote_ttl_seconds"], 60)
        self.assertGreater(payload["execution_plan"]["total_received"], 0)
        self.assertTrue(payload["execution_plan"]["route"])


if __name__ == "__main__":
    unittest.main()
