"""Enum 단위 테스트 — 값 일관성, 멤버 수 확인"""

from demiurge_testdata.core.enums import (
    AdapterCategory,
    AdapterType,
    CompressionType,
    DatasetCategory,
    FormatType,
    GenerationMode,
)


class TestAdapterType:
    def test_rdbms_count(self):
        rdbms = [
            AdapterType.POSTGRESQL,
            AdapterType.MYSQL,
            AdapterType.MARIADB,
            AdapterType.MSSQL,
            AdapterType.ORACLE,
            AdapterType.SQLITE,
            AdapterType.COCKROACHDB,
            AdapterType.BIGQUERY,
        ]
        assert len(rdbms) == 8

    def test_total_count(self):
        assert len(AdapterType) == 22

    def test_string_values(self):
        assert AdapterType.POSTGRESQL == "postgresql"
        assert AdapterType.KAFKA == "kafka"
        assert AdapterType.S3 == "s3"
        assert AdapterType.FTP == "ftp"


class TestFormatType:
    def test_count(self):
        assert len(FormatType) == 10

    def test_values(self):
        assert FormatType.JSON == "json"
        assert FormatType.PARQUET == "parquet"
        assert FormatType.MSGPACK == "msgpack"


class TestCompressionType:
    def test_count(self):
        """6종 + none = 7종"""
        assert len(CompressionType) == 7

    def test_none_value(self):
        assert CompressionType.NONE == "none"

    def test_values(self):
        assert CompressionType.GZIP == "gzip"
        assert CompressionType.ZSTD == "zstd"
        assert CompressionType.LZ4 == "lz4"


class TestAdapterCategory:
    def test_count(self):
        assert len(AdapterCategory) == 5

    def test_values(self):
        assert AdapterCategory.RDBMS == "rdbms"
        assert AdapterCategory.FILE_TRANSFER == "filetransfer"


class TestGenerationMode:
    def test_count(self):
        assert len(GenerationMode) == 3

    def test_values(self):
        assert GenerationMode.STREAM == "stream"
        assert GenerationMode.BATCH == "batch"
        assert GenerationMode.API == "api"


class TestDatasetCategory:
    def test_count(self):
        assert len(DatasetCategory) == 6
