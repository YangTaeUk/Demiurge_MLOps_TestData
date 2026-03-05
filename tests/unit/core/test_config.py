"""Config 단위 테스트 — YAML → Pydantic 로딩, 검증"""

import pytest

from demiurge_testdata.core.config import (
    AdapterConfig,
    GeneratorConfig,
    HandlerConfig,
    PipelineConfig,
    load_config,
)
from demiurge_testdata.core.enums import CompressionType, FormatType, GenerationMode
from demiurge_testdata.core.exceptions import ConfigError


class TestPipelineConfig:
    def test_minimal_config(self):
        config = PipelineConfig(
            generator=GeneratorConfig(type="home_credit"),
            adapter=AdapterConfig(type="postgresql"),
        )
        assert config.name == "default"
        assert config.mode == GenerationMode.BATCH
        assert config.handler.format == FormatType.JSON
        assert config.handler.compression == CompressionType.NONE

    def test_full_config(self):
        config = PipelineConfig(
            name="test-pipeline",
            mode=GenerationMode.STREAM,
            generator=GeneratorConfig(
                type="ieee_fraud",
                batch_size=500,
                seed=42,
            ),
            handler=HandlerConfig(
                format=FormatType.AVRO,
                compression=CompressionType.SNAPPY,
            ),
            adapter=AdapterConfig(
                type="kafka",
                host="kafka-host",
                port=9092,
            ),
        )
        assert config.name == "test-pipeline"
        assert config.mode == GenerationMode.STREAM
        assert config.generator.batch_size == 500
        assert config.handler.format == FormatType.AVRO


class TestGeneratorConfig:
    def test_defaults(self):
        config = GeneratorConfig(type="olist")
        assert config.batch_size == 1000
        assert config.max_records is None
        assert config.seed is None

    def test_batch_size_validation(self):
        with pytest.raises(ValueError):
            GeneratorConfig(type="test", batch_size=0)

        with pytest.raises(ValueError):
            GeneratorConfig(type="test", batch_size=2_000_000)


class TestHandlerConfig:
    def test_defaults(self):
        config = HandlerConfig()
        assert config.format == FormatType.JSON
        assert config.compression == CompressionType.NONE


class TestLoadConfig:
    def test_valid_yaml(self, tmp_path):
        yaml_content = """\
name: test
mode: batch
generator:
  type: home_credit
  batch_size: 100
handler:
  format: json
  compression: gzip
adapter:
  type: postgresql
  host: localhost
  port: 5434
"""
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml_content)

        config = load_config(config_file)
        assert config.name == "test"
        assert config.generator.type == "home_credit"
        assert config.generator.batch_size == 100
        assert config.handler.format == FormatType.JSON
        assert config.handler.compression == CompressionType.GZIP
        assert config.adapter.port == 5434

    def test_file_not_found(self):
        with pytest.raises(ConfigError, match="not found"):
            load_config("/nonexistent/path.yaml")

    def test_invalid_yaml(self, tmp_path):
        config_file = tmp_path / "bad.yaml"
        config_file.write_text("{{invalid yaml")

        with pytest.raises(ConfigError, match="YAML parse error"):
            load_config(config_file)

    def test_non_mapping_yaml(self, tmp_path):
        config_file = tmp_path / "list.yaml"
        config_file.write_text("- item1\n- item2\n")

        with pytest.raises(ConfigError, match="YAML mapping"):
            load_config(config_file)

    def test_validation_error(self, tmp_path):
        yaml_content = """\
generator:
  type: test
  batch_size: -1
adapter:
  type: pg
"""
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text(yaml_content)

        with pytest.raises(ConfigError, match="validation error"):
            load_config(config_file)
