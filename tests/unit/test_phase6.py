"""Phase 6 단위 테스트 — StorageBackend, REST API, CLI"""

from __future__ import annotations

import argparse
from unittest.mock import patch

import pytest

# ── StorageBackend ──


class TestStorageBackend:
    def test_abc_cannot_instantiate(self):
        from demiurge_testdata.storage.backend import StorageBackend

        with pytest.raises(TypeError):
            StorageBackend()

    def test_adapter_storage_backend_init(self):
        pytest.importorskip("aiofiles")
        import demiurge_testdata.adapters.storage.local_fs  # noqa: F401
        from demiurge_testdata.storage.backend import AdapterStorageBackend

        backend = AdapterStorageBackend("local_fs", base_path="/tmp/test")
        assert backend._adapter_key == "local_fs"


# ── REST API ──


class TestRESTAPI:
    @pytest.fixture
    def client(self):
        fastapi = pytest.importorskip("fastapi")  # noqa: F841
        from fastapi.testclient import TestClient

        import demiurge_testdata.generators.relational.home_credit  # noqa: F401
        from demiurge_testdata.api.rest.app import app

        return TestClient(app)

    def test_health(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_list_generators(self, client):
        r = client.get("/generators")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        assert len(data) > 0
        assert "key" in data[0]
        assert "class_name" in data[0]

    def test_get_generator_found(self, client):
        r = client.get("/generators/home_credit")
        assert r.status_code == 200
        assert r.json()["key"] == "home_credit"

    def test_get_generator_not_found(self, client):
        r = client.get("/generators/nonexistent")
        assert r.status_code == 404

    def test_list_adapters(self, client):
        pytest.importorskip("asyncpg")
        import demiurge_testdata.adapters.rdbms.postgresql  # noqa: F401

        r = client.get("/adapters")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    def test_get_adapter_not_found(self, client):
        r = client.get("/adapters/nonexistent")
        assert r.status_code == 404


# ── CLI ──


class TestCLI:
    def test_main_no_args(self):
        from demiurge_testdata.__main__ import main

        with pytest.raises(SystemExit) as exc_info, patch("sys.argv", ["demiurge-testdata"]):
            main()
        assert exc_info.value.code == 1

    def test_cmd_list(self, capsys):
        import demiurge_testdata.generators.relational.home_credit  # noqa: F401
        from demiurge_testdata.__main__ import cmd_list

        args = argparse.Namespace(target="generators")
        cmd_list(args)
        captured = capsys.readouterr()
        assert "home_credit" in captured.out

    def test_cmd_list_all(self, capsys):
        from demiurge_testdata.__main__ import cmd_list

        args = argparse.Namespace(target="all")
        cmd_list(args)
        captured = capsys.readouterr()
        assert "generators" in captured.out

    def test_cmd_list_unknown(self):
        from demiurge_testdata.__main__ import cmd_list

        args = argparse.Namespace(target="unknown")
        with pytest.raises(SystemExit):
            cmd_list(args)
