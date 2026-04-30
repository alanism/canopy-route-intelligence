"""Gas estimation helpers for execution simulation."""

from __future__ import annotations


GAS_UNITS_BY_OPERATION = {
    "hold": 10_000,
    "swap": 150_000,
    "bridge": 350_000,
    "deposit": 90_000,
}

GAS_SPEED_MULTIPLIERS = {
    "slow": 0.92,
    "medium": 1.0,
    "fast": 1.18,
}


def estimate_gas_cost(
    *,
    snapshot: dict,
    chain: str,
    operation_type: str,
    gas_speed: str,
) -> dict:
    gas_units = GAS_UNITS_BY_OPERATION.get(operation_type, 120_000)
    speed_multiplier = GAS_SPEED_MULTIPLIERS.get(gas_speed, 1.0)
    gas_price_gwei = float(snapshot["gas_prices_gwei"].get(chain, 1.0)) * speed_multiplier
    moving_average = float(snapshot["gas_moving_average_gwei"].get(chain, gas_price_gwei))
    native_token_price = float(snapshot["native_token_prices_usd"].get(chain, 1.0))
    gas_age_seconds = int(snapshot["data_freshness"].get("gas_age_sec", 0))

    gas_cost_native = (gas_price_gwei * gas_units) / 1_000_000_000
    gas_cost_usd = gas_cost_native * native_token_price
    gas_spike = gas_price_gwei > (moving_average * 3)

    return {
        "gas_units": gas_units,
        "gas_price_gwei": round(gas_price_gwei, 6),
        "gas_cost_usd": round(gas_cost_usd, 6),
        "gas_age_seconds": gas_age_seconds,
        "gas_spike_detected": gas_spike,
        "flag": "GAS_SPIKE_DETECTION" if gas_spike else None,
        "confirmation_seconds": int(
            snapshot["block_times"].get(chain, 2) * snapshot["confirmations"].get(chain, 10)
        ),
    }
