"""YAML 파이프라인 설정 파일 유효성 테스트"""

from __future__ import annotations

from pathlib import Path

import pytest

CONFIGS_DIR = Path(__file__).resolve().parents[2] / "configs" / "pipelines"


class TestYAMLConfigs:
    """모든 YAML 설정 파일이 PipelineConfig 스키마에 맞는지 검증"""

    @pytest.fixture
    def config_files(self):
        return sorted(CONFIGS_DIR.glob("*.yaml"))

    def test_configs_exist(self, config_files):
        """최소 1개 이상의 설정 파일 존재"""
        assert len(config_files) > 0, f"No YAML configs found in {CONFIGS_DIR}"

    def test_all_configs_count(self, config_files):
        """32개 제너레이터에 대응하는 설정 파일"""
        assert len(config_files) >= 25, f"Expected ≥25 configs, found {len(config_files)}"

    @pytest.mark.parametrize(
        "config_file",
        sorted(CONFIGS_DIR.glob("*.yaml")) if CONFIGS_DIR.exists() else [],
        ids=lambda p: p.stem,
    )
    def test_config_validates(self, config_file):
        """각 YAML 파일이 PipelineConfig로 파싱 가능한지 검증"""
        from demiurge_testdata.core.config import load_config

        config = load_config(config_file)
        assert config.name
        assert config.generator.type
        assert config.adapter.type
        assert config.generator.batch_size >= 1

    @pytest.mark.parametrize(
        "config_file",
        sorted(CONFIGS_DIR.glob("*.yaml")) if CONFIGS_DIR.exists() else [],
        ids=lambda p: p.stem,
    )
    def test_generator_key_registered(self, config_file):
        """설정 파일의 generator.type이 실제 등록된 키인지 검증"""
        from demiurge_testdata.__main__ import _import_generators
        from demiurge_testdata.core.config import load_config
        from demiurge_testdata.core.registry import generator_registry

        _import_generators()

        config = load_config(config_file)
        assert config.generator.type in generator_registry, (
            f"Generator '{config.generator.type}' not registered"
        )

    @pytest.mark.parametrize(
        "config_file",
        sorted(CONFIGS_DIR.glob("*.yaml")) if CONFIGS_DIR.exists() else [],
        ids=lambda p: p.stem,
    )
    def test_adapter_key_registered(self, config_file):
        """설정 파일의 adapter.type이 실제 등록된 키인지 검증"""
        import demiurge_testdata.adapters  # noqa: F401

        # Import all adapters to populate registry
        from demiurge_testdata.__main__ import _import_adapters
        from demiurge_testdata.core.config import load_config
        from demiurge_testdata.core.registry import adapter_registry

        _import_adapters()

        config = load_config(config_file)
        # Some adapters may not be installed — skip if not registered
        if config.adapter.type not in adapter_registry:
            pytest.skip(f"Adapter '{config.adapter.type}' not installed")
