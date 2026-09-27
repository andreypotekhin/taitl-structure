import pytest
from integration.pyspark.support.compiled_artifacts import CompiledArtifacts, reuse_mode


def test_compiled_artifacts_reuse_mode_defaults_to_module(monkeypatch) -> None:
    monkeypatch.delenv("STRUCTURE_COMPILED_ARTIFACT_REUSE", raising=False)

    assert reuse_mode() == "module"


def test_compiled_artifacts_rejects_unknown_reuse_mode(monkeypatch) -> None:
    monkeypatch.setenv("STRUCTURE_COMPILED_ARTIFACT_REUSE", "sometimes")

    with pytest.raises(ValueError, match="STRUCTURE_COMPILED_ARTIFACT_REUSE"):
        CompiledArtifacts()


def test_compiled_artifacts_module_owner_shares_and_releases_pool() -> None:
    owner = CompiledArtifacts("module")

    source_pool = owner.pool("source preparation")
    runtime_pool = owner.pool("runtime")
    assert source_pool is runtime_pool
    assert owner.totals() == {"requests": 0, "hits": 0, "misses": 0, "elapsed": 0}

    owner.close()

    assert source_pool.status().entries == 0


def test_compiled_artifacts_off_owner_creates_isolated_pools() -> None:
    owner = CompiledArtifacts("off")

    assert owner.pool("source preparation") is not owner.pool("runtime")

    owner.close()
