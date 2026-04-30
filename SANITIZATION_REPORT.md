# Sanitization Report

## 1. Summary

Prepared the repository for a public Solana Frontier Hackathon submission under the product name Canopy Route Intelligence and technical engine name Project DG. The pass removed private partner framing, deleted obsolete private planning documents, rewrote the public README, added judge-facing methodology and architecture docs, sanitized UI metadata/copy, and tightened credential ignore rules.

## 2. Files Changed

Key changed or added files:

- `README.md`
- `.env.example`
- `.gitignore`
- `api/main.py`
- `api/demo_store.py`
- `api/router.py`
- `services/payroll_demo.py`
- `services/logging_utils.py`
- `services/request_context.py`
- `configs/corridors.v1.json`
- `ui/index.html`
- `docs/architecture.md`
- `docs/solana-integration.md`
- `docs/methodology.md`
- `docs/data-quality.md`
- `SANITIZATION_REPORT.md`

Removed obsolete/private-context docs including old partner/pilot PRDs, current-product packets, private deployment notes, and research notes that were not appropriate for a public hackathon repository.

## 3. Product Naming Changes

- Public product name is now Canopy Route Intelligence.
- Technical engine name Project DG is used in public docs.
- UI metadata and page title now use Canopy Route Intelligence.
- Old private partner naming was replaced with sample/demo language.

## 4. Private References Removed

Removed or rewrote references to the forbidden partner name, private pilot framing, private deployment URLs, private local paths, and partner-specific planning language.

The final audit found no remaining hits for:

- `forbidden partner name`
- `forbidden_partner_name`
- `private partner project`
- private company domain pattern
- local macOS absolute path pattern
- local Windows absolute path pattern
- personal email pattern
- `@forbidden_partner_name`
- company email pattern

## 5. Secrets And Credential Checks

Performed repository searches for:

- `secret`
- `api_key`
- `apikey`
- `token`
- `bearer`
- `private_key`
- `service_account`
- `credentials`
- `client_secret`
- `gcloud`
- `GOOGLE_APPLICATION_CREDENTIALS`

No committed real `.env` file, private key, OAuth secret, bearer token, service-account JSON, or credential file was found in the working tree.

Remaining safe hits:

- `token` appears throughout the codebase as stablecoin/token terminology.
- `credentials`, `service_account`, `GOOGLE_APPLICATION_CREDENTIALS`, and `gcloud` appear in BigQuery authentication code or placeholder setup docs.
- `.env.example` contains placeholders only.

`.gitignore` now protects `.env`, `.env.*`, key/certificate files, service-account JSON patterns, credentials JSON patterns, private data folders, private screenshots, local virtualenvs, cache/build outputs, SQLite demo databases, and OS artifacts.

## 6. Overclaims Corrected

Public docs now use conservative benchmark language:

- observed route cost, not true facilitator margin
- route intelligence benchmark, not routing oracle
- observed on-chain settlement health, not proof of full settlement reliability
- hackathon benchmark and demo API, not production router
- demo/sample dataset, not private partner dataset

The README and methodology explicitly state:

- This project does not custody funds.
- This project does not execute payments.
- This project does not claim true facilitator margin.
- This project does not claim full production autonomous routing.
- This project does not claim complete off-chain x402 payment visibility.
- This project benchmarks observed stablecoin route behavior from available chain data.

## 7. Docs Added Or Updated

- `README.md`: public judge-facing overview, setup, architecture, limitations, and hackathon notes.
- `docs/architecture.md`: ingestion, normalization, validation, freshness gates, API, and dashboard flow.
- `docs/solana-integration.md`: Solana-specific primitives, normalization approach, implemented vs limited scope.
- `docs/methodology.md`: observed route cost, route share, freshness, and observed settlement-health definitions.
- `docs/data-quality.md`: freshness gates, reconciliation, supply parity checks, stale data behavior, and sample field interpretation.

## 8. Commands Run

```bash
rg --files -uu
git status --short
rg -n -i ...final audit terms...
find . -maxdepth 3 ...credential patterns...
python3 -m py_compile $(find api services models data forecasting scripts -name '*.py' -not -path '*/__pycache__/*')
python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt
./.venv/bin/pip install pytest
./.venv/bin/python -m pytest
./.venv/bin/python -m py_compile $(find api services models data forecasting scripts -name '*.py' -not -path '*/__pycache__/*')
```

## 9. Failed Commands

Initial test attempt:

```bash
python3 -m pytest
```

Failed because the system Python did not have `pytest` installed.

Full virtualenv test run:

```bash
./.venv/bin/python -m pytest
```

Result: 103 passed, 2 failed, 30 warnings.

Failures:

- `tests/test_receipt_contract.py::ReceiptContractTests::test_decision_flip_conditions_and_alternative_paths`
- `tests/test_receipt_contract.py::ReceiptContractTests::test_operational_context_rendering`

Reason: the receipt fixture now evaluates against the current runtime date/freshness context and renders the sample Philippines run as approval-ready, while the assertions expect an older evidence-limited hold state and specific flip-condition copy. This appears to be a fixture/time-sensitivity issue rather than a syntax or import failure.

## 10. Remaining Risks Or Manual Review Items

- The dashboard still contains a payroll-readiness workflow surface. It has been sanitized to sample/demo language, but a product review should decide whether that workflow should remain in the final hackathon demo or be hidden behind a route-intelligence-first landing state.
- The public docs describe Solana normalization design, but the current codebase does not include a complete Solana indexer implementation. This is documented as cut/limited scope.
- Some internal schema names still use historical terms such as payroll or route-fit because renaming database tables and all route fields would be a larger behavioral migration.
- The removed private planning docs should stay out of the public repository history if this is published as a fresh public repo.
- Test fixtures should be made date-stable before public submission.

## 11. Recommended Next Steps

- Re-run tests with a frozen clock or update the receipt fixture so readiness expectations do not drift with wall-clock time.
- Review the dashboard first screen and consider making route intelligence metrics the primary default view.
- If Solana code is added before submission, update `docs/solana-integration.md` from design/limited status to implemented status only for the parts that actually land.
- Publish as a fresh repository without private git history.
- Do one final secret scan in the target public repository after copying files.
