from __future__ import annotations

from dataclasses import replace

import pytest

from ecommerce_pipeline.secrets import get_secret


def test_get_secret_prefers_explicit_env_var(monkeypatch, local_config) -> None:
    monkeypatch.setenv("PG_PASSWORD", "from-env-var")

    assert get_secret(local_config, "ignored", env_var="PG_PASSWORD") == "from-env-var"


def test_get_secret_reads_named_env_for_env_provider(monkeypatch, local_config) -> None:
    monkeypatch.setenv("PIPELINE_SECRET", "secret-value")

    assert get_secret(local_config, "PIPELINE_SECRET") == "secret-value"


def test_get_secret_azure_without_vault_url_falls_back_to_env(monkeypatch, local_config) -> None:
    config = replace(local_config, secrets=replace(local_config.secrets, provider="azure_key_vault", key_vault_url=None))
    monkeypatch.setenv("AZURE_SECRET", "fallback")

    assert get_secret(config, "AZURE_SECRET") == "fallback"


def test_get_secret_rejects_unknown_provider(local_config) -> None:
    config = replace(local_config, secrets=replace(local_config.secrets, provider="unknown"))

    with pytest.raises(ValueError, match="Unsupported secrets provider"):
        get_secret(config, "anything")
