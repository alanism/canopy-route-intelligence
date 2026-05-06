#!/usr/bin/env python3
"""Materialize Phase 17 Solana corridor intelligence from Phase 16.5 evidence."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.solana.corridor_intelligence import (
    build_corridor_intelligence_from_shadow_report,
    write_corridor_intelligence,
)


def _load_shadow_runner():
    script_path = ROOT / "scripts" / "run_solana_shadow_validation.py"
    spec = importlib.util.spec_from_file_location("run_solana_shadow_validation", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _load_report(path: str | None) -> dict:
    if path:
        with Path(path).open("r", encoding="utf-8") as fh:
            return json.load(fh)
    shadow_runner = _load_shadow_runner()
    return shadow_runner.run_shadow_validation()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a materialized Solana corridor intelligence artifact."
    )
    parser.add_argument("--from-shadow-json", default=None, help="Use a saved Phase 16.5 shadow report.")
    parser.add_argument("--output", default=None, help="Output JSON path. Defaults to data/solana_corridor_intelligence.json.")
    parser.add_argument("--corridor-id", default="SOLANA-WATCHED")
    parser.add_argument("--corridor-label", default="Solana watched-source corridor")
    parser.add_argument("--token", default="USDC")
    args = parser.parse_args()

    shadow_report = _load_report(args.from_shadow_json)
    payload = build_corridor_intelligence_from_shadow_report(
        shadow_report,
        corridor_id=args.corridor_id,
        corridor_label=args.corridor_label,
        token=args.token,
    )
    output_path = write_corridor_intelligence(payload, path=args.output)
    print(json.dumps({
        "status": payload["status"],
        "signal_state": payload["signal_state"],
        "claim_level": payload["claim_level"],
        "output_path": str(output_path),
        "missing_fields": payload["missing_fields"],
        "open_risks": payload["open_risks"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
