#!/usr/bin/env python3
"""Run the internal BigQuery audit parity checks and write report artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.bigquery_audit import run_and_write_audit_report


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Canopy BigQuery audit parity checks.")
    parser.add_argument(
        "--output-dir",
        default="audit",
        help="Directory for generated JSON and Markdown audit reports.",
    )
    args = parser.parse_args()

    result = run_and_write_audit_report(output_dir=Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["report"]["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
