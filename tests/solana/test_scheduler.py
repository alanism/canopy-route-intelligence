"""Phase 15 — durable scheduler and checkpoint safety tests."""

from __future__ import annotations

import importlib.util
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "run_solana_ingestion.py"


def _load_scheduler():
    spec = importlib.util.spec_from_file_location("run_solana_ingestion", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class TestCheckpointBackendHierarchy:
    def test_development_defaults_to_local_file(self, monkeypatch):
        scheduler = _load_scheduler()
        monkeypatch.setenv("ENV", "development")
        monkeypatch.delenv("SOLANA_CHECKPOINT_BACKEND", raising=False)

        env_mode, backend = scheduler._resolve_checkpoint_backend()

        assert env_mode == "development"
        assert isinstance(backend, scheduler.LocalFileCheckpointBackend)
        assert backend.name == "local_file"

    @pytest.mark.parametrize("backend_name,class_name", [
        ("gcs", "GCSCheckpointBackend"),
        ("bigquery_metadata", "BigQueryMetadataCheckpointBackend"),
    ])
    def test_remote_backend_names_resolve(self, monkeypatch, backend_name, class_name):
        scheduler = _load_scheduler()
        monkeypatch.setenv("ENV", "production")
        monkeypatch.setenv("SOLANA_CHECKPOINT_BACKEND", backend_name)

        env_mode, backend = scheduler._resolve_checkpoint_backend()

        assert env_mode == "production"
        assert backend.name == backend_name
        assert backend.__class__.__name__ == class_name

    def test_remote_backend_build_is_explicitly_not_wired(self, monkeypatch):
        scheduler = _load_scheduler()
        monkeypatch.setenv("ENV", "production")
        monkeypatch.setenv("SOLANA_CHECKPOINT_BACKEND", "gcs")
        _, backend = scheduler._resolve_checkpoint_backend()

        with pytest.raises(scheduler.RemoteCheckpointBackendNotConfigured):
            backend.build_store()

    def test_production_local_file_raises_named_error(self, monkeypatch):
        scheduler = _load_scheduler()
        monkeypatch.setenv("ENV", "production")
        monkeypatch.setenv("SOLANA_CHECKPOINT_BACKEND", "local_file")

        with pytest.raises(scheduler.ProductionCheckpointError):
            scheduler._resolve_checkpoint_backend()

    def test_unknown_backend_rejected(self, monkeypatch):
        scheduler = _load_scheduler()
        monkeypatch.setenv("ENV", "development")
        monkeypatch.setenv("SOLANA_CHECKPOINT_BACKEND", "sqlite")

        with pytest.raises(scheduler.UnsupportedCheckpointBackendError):
            scheduler._resolve_checkpoint_backend()


class TestCheckpointAdvanceInvariant:
    def _result(self, *, run_status="ok", ingestion_state="succeeded", observation_state="observed"):
        return SimpleNamespace(
            run_status=run_status,
            ingestion_state=ingestion_state,
            observation_state=observation_state,
        )

    def test_all_success_conditions_allow_advance(self):
        scheduler = _load_scheduler()
        assert scheduler._checkpoint_advance_allowed(
            self._result(), validation_approved=True, write_succeeded=True
        ) is True

    @pytest.mark.parametrize("validation_approved,write_succeeded", [
        (False, True),
        (True, False),
        (False, False),
    ])
    def test_no_advance_on_failed_validation_or_write(self, validation_approved, write_succeeded):
        scheduler = _load_scheduler()
        assert scheduler._checkpoint_advance_allowed(
            self._result(),
            validation_approved=validation_approved,
            write_succeeded=write_succeeded,
        ) is False

    @pytest.mark.parametrize("ingestion_state", [
        "circuit_open",
        "provider_lagging",
        "failed",
        "unavailable",
    ])
    def test_no_advance_on_unsafe_ingestion_state(self, ingestion_state):
        scheduler = _load_scheduler()
        assert scheduler._checkpoint_advance_allowed(
            self._result(ingestion_state=ingestion_state),
            validation_approved=True,
            write_succeeded=True,
        ) is False

    def test_no_advance_on_ambiguous_empty(self):
        scheduler = _load_scheduler()
        assert scheduler._checkpoint_advance_allowed(
            self._result(observation_state="ambiguous_empty"),
            validation_approved=True,
            write_succeeded=True,
        ) is False


class TestSchedulerRuntime:
    def test_once_dry_run_emits_structured_log(self, monkeypatch, capsys):
        scheduler = _load_scheduler()
        monkeypatch.setenv("ENV", "development")
        monkeypatch.setenv("SOLANA_CHECKPOINT_BACKEND", "local_file")
        monkeypatch.setattr(sys, "argv", ["run_solana_ingestion.py", "--once", "--dry-run"])

        code = scheduler.main()
        lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]

        assert code == 0
        assert lines[0]["event"] == "startup"
        assert lines[0]["checkpoint_backend"] == "local_file"
        assert lines[1]["event"] == "solana_ingestion_run"
        assert lines[1]["dry_run"] is True
        assert lines[1]["checkpoint_advance_allowed"] is False

    def test_loop_dry_run_exits_cleanly_on_sigterm(self):
        env = {
            **os.environ,
            "ENV": "development",
            "SOLANA_CHECKPOINT_BACKEND": "local_file",
            "SOLANA_INGESTION_INTERVAL_SECONDS": "30",
        }
        proc = subprocess.Popen(
            [sys.executable, str(SCRIPT_PATH), "--loop", "--dry-run"],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            start = time.time()
            lines = []
            while time.time() - start < 5:
                line = proc.stdout.readline()
                if line:
                    lines.append(json.loads(line))
                if any(item.get("event") == "solana_ingestion_run" for item in lines):
                    break
            proc.send_signal(signal.SIGTERM)
            stdout, stderr = proc.communicate(timeout=5)
            lines.extend(json.loads(line) for line in stdout.splitlines() if line.strip())
        finally:
            if proc.poll() is None:
                proc.kill()

        assert proc.returncode == 0
        assert "Traceback" not in stderr
        assert any(item.get("event") == "shutdown_signal" for item in lines)
        assert any(item.get("event") == "shutdown_complete" for item in lines)
