from __future__ import annotations

from ecommerce_pipeline.filesystem import _storage_options, ensure_parent, exists, open_text


def test_filesystem_helpers(tmp_path) -> None:
    path = str(tmp_path / "nested" / "file.txt")

    ensure_parent(path)
    with open_text(path, "w") as handle:
        handle.write("ok")

    assert exists(path)
    with open_text(path, "r") as handle:
        assert handle.read() == "ok"


def test_storage_options_for_account_key(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_STORAGE_AUTH_TYPE", "account_key")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "lakeacct")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", "secret-key")

    assert _storage_options("abfss://lakehouse@lakeacct.dfs.core.windows.net/path") == {
        "account_name": "lakeacct",
        "account_key": "secret-key",
    }


def test_storage_options_for_sas(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_STORAGE_AUTH_TYPE", "sas")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "lakeacct")
    monkeypatch.setenv("AZURE_STORAGE_SAS_TOKEN", "sas-token")

    assert _storage_options("abfss://lakehouse@lakeacct.dfs.core.windows.net/path") == {
        "account_name": "lakeacct",
        "sas_token": "sas-token",
    }


def test_storage_options_for_service_principal(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_STORAGE_AUTH_TYPE", "service_principal")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "lakeacct")
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")

    assert _storage_options("abfss://lakehouse@lakeacct.dfs.core.windows.net/path") == {
        "account_name": "lakeacct",
        "tenant_id": "tenant",
        "client_id": "client",
        "client_secret": "secret",
    }


def test_storage_options_ignores_local_paths(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "lakeacct")

    assert _storage_options("/tmp/lake/table") == {}
