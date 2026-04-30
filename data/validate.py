"""
data/validate.py — Data pipeline validation script.

Run before Day 2 to confirm BigQuery queries return real data.
Exits 0 if all checks PASS, exits 1 if any FAIL.

Usage:
    python -m data.validate
"""

import sys
import os
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

from data.query import run_chain_query, CHAIN_CONFIGS
from api.eth_price import get_native_prices


def main():
    print("=" * 78)
    print("  SCI-Agent Data Pipeline Validator")
    print("=" * 78)
    print()

    prices, is_live = get_native_prices()
    source = "live" if is_live else "fallback"
    print(
        f"ETH Price: ${prices['ethereum']:,.0f} | "
        f"POL Price: ${prices['polygon']:,.4f} ({source})"
    )
    print()

    # ── Step 2: Query both chains ───────────────────────────────────────────
    results = {}
    for chain_name in ["Polygon", "Ethereum"]:
        print(f"Querying {chain_name}...")
        try:
            native_price = prices["polygon"] if chain_name == "Polygon" else prices["ethereum"]
            result = run_chain_query(CHAIN_CONFIGS[chain_name], native_price)
            results[chain_name] = result
            if result:
                print(f"  ✓ {chain_name}: {result['transfer_count']} transfers found")
            else:
                print(f"  ✗ {chain_name}: No data returned")
        except Exception as e:
            print(f"  ✗ {chain_name}: Error — {e}")
            results[chain_name] = None

    print()

    # ── Step 3: Print comparison table ──────────────────────────────────────
    header = f"{'Chain':<12} {'Avg Fee':>10} {'Median':>10} {'P90':>10} {'Transfers':>12} {'Volume USDC':>16} {'Window':>8} {'Freshness':>20}"
    print(header)
    print("─" * len(header))

    for chain_name in ["Polygon", "Ethereum"]:
        r = results.get(chain_name)
        if r is None:
            print(f"{chain_name:<12} {'—':>10} {'—':>10} {'—':>10} {'—':>12} {'—':>16} {'—':>8} {'—':>20}")
            continue

        vol_str = f"${r['volume_usdc']:,.0f}" if r['volume_usdc'] is not None else "null"
        fresh_str = r['freshness_timestamp'][:19] if r['freshness_timestamp'] else "—"

        print(
            f"{r['chain']:<12} "
            f"${r['avg_fee_usd']:>8.4f} "
            f"${r['median_fee_usd']:>8.4f} "
            f"${r['p90_fee_usd']:>8.4f} "
            f"{r['transfer_count']:>11,} "
            f"{vol_str:>16} "
            f"{r['window_used']:>8} "
            f"{fresh_str:>20}"
        )

    print()

    # ── Step 4: Sanity checks ───────────────────────────────────────────────
    checks_passed = 0
    checks_total = 4

    polygon = results.get("Polygon")
    eth = results.get("Ethereum")

    # Check 1: Polygon avg_fee < $1.00
    if polygon and polygon["avg_fee_usd"] < 1.00:
        print(f"[PASS] Polygon avg_fee_usd (${polygon['avg_fee_usd']:.4f}) < $1.00")
        checks_passed += 1
    else:
        val = f"${polygon['avg_fee_usd']:.4f}" if polygon else "no data"
        print(f"[FAIL] Polygon avg_fee_usd ({val}) — expected < $1.00")

    # Check 2: Ethereum avg_fee > Polygon avg_fee
    if polygon and eth and eth["avg_fee_usd"] > polygon["avg_fee_usd"]:
        print(
            f"[PASS] Ethereum avg_fee_usd (${eth['avg_fee_usd']:.4f}) "
            f"> Polygon avg_fee_usd (${polygon['avg_fee_usd']:.4f})"
        )
        checks_passed += 1
    else:
        print("[FAIL] Ethereum avg_fee_usd should be > Polygon avg_fee_usd")

    # Check 3: Both transfer_count > 0
    if polygon and eth and polygon["transfer_count"] > 0 and eth["transfer_count"] > 0:
        print(
            f"[PASS] Both transfer_count > 0 "
            f"(Polygon: {polygon['transfer_count']:,}, Ethereum: {eth['transfer_count']:,})"
        )
        checks_passed += 1
    else:
        print("[FAIL] Both chains should have transfer_count > 0")

    # Check 4: Both freshness within 48h
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=48)
    fresh_ok = True
    for name, r in [("Polygon", polygon), ("Ethereum", eth)]:
        if r is None or r["freshness_timestamp"] is None:
            fresh_ok = False
            continue
        ts_str = r["freshness_timestamp"]
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts < cutoff:
                fresh_ok = False
        except (ValueError, TypeError):
            fresh_ok = False

    if fresh_ok:
        print("[PASS] Both freshness_timestamp within last 48h")
        checks_passed += 1
    else:
        print("[FAIL] Both freshness_timestamp should be within last 48h")

    print()
    if checks_passed == checks_total:
        print(f"✅ All {checks_total} checks passed. Proceed to Day 2.")
        sys.exit(0)
    else:
        print(
            f"❌ {checks_passed}/{checks_total} checks passed. "
            f"Fix issues before proceeding."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
