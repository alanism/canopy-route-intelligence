# Local Test Dependencies

Repository-wide pytest collection currently requires the packages pinned in `requirements.txt`.

If `.venv/bin/python -m pytest -q` fails during collection with missing imports such as `pandas`, `google`, `dotenv`, `fastapi`, or `pydantic`, install the pinned runtime dependencies into the active virtual environment:

```bash
.venv/bin/python -m pip install -r requirements.txt
```

After installation, rerun:

```bash
.venv/bin/python -m pytest -q
```

Phase 15 Solana validation does not require live GCP or BigQuery access. The blocked repo-wide run observed on 2026-05-05 failed before test execution during local dependency import collection.
