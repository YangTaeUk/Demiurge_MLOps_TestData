"""Format Handler 단위 테스트 — 10종 포맷 핸들러 encode/decode 검증"""

from __future__ import annotations

import pytest

# ── 테스트 데이터 ──

SIMPLE_RECORDS = [
    {"id": 1, "name": "alice", "score": 95.5},
    {"id": 2, "name": "bob", "score": 87.3},
    {"id": 3, "name": "charlie", "score": 92.1},
]

SINGLE_RECORD = [{"key": "value"}]


# ── JSON (orjson) ──


class TestJsonHandler:
    @pytest.fixture
    def handler(self):
        orjson = pytest.importorskip("orjson")  # noqa: F841
        from demiurge_testdata.handlers.formats.json_handler import JsonFormatHandler

        return JsonFormatHandler()

    async def test_encode_returns_bytes(self, handler):
        result = await handler.encode(SIMPLE_RECORDS)
        assert isinstance(result, bytes)
        assert len(result) > 0

    async def test_roundtrip(self, handler):
        encoded = await handler.encode(SIMPLE_RECORDS)
        decoded = await handler.decode(encoded)
        assert decoded == SIMPLE_RECORDS

    async def test_empty_records(self, handler):
        encoded = await handler.encode([])
        decoded = await handler.decode(encoded)
        assert decoded == []

    def test_properties(self, handler):
        assert handler.format_name == "json"
        assert handler.content_type == "application/json"
        assert handler.file_extension == ".json"


# ── JSONL (orjson) ──


class TestJsonlHandler:
    @pytest.fixture
    def handler(self):
        orjson = pytest.importorskip("orjson")  # noqa: F841
        from demiurge_testdata.handlers.formats.jsonl_handler import JsonlFormatHandler

        return JsonlFormatHandler()

    async def test_encode_newline_separated(self, handler):
        encoded = await handler.encode(SIMPLE_RECORDS)
        lines = encoded.split(b"\n")
        assert len(lines) == 3

    async def test_roundtrip(self, handler):
        encoded = await handler.encode(SIMPLE_RECORDS)
        decoded = await handler.decode(encoded)
        assert decoded == SIMPLE_RECORDS

    async def test_single_record(self, handler):
        encoded = await handler.encode(SINGLE_RECORD)
        decoded = await handler.decode(encoded)
        assert decoded == SINGLE_RECORD

    def test_properties(self, handler):
        assert handler.format_name == "jsonl"
        assert handler.content_type == "application/x-ndjson"
        assert handler.file_extension == ".jsonl"


# ── CSV (stdlib) ──


class TestCsvHandler:
    @pytest.fixture
    def handler(self):
        from demiurge_testdata.handlers.formats.csv_handler import CsvFormatHandler

        return CsvFormatHandler()

    async def test_roundtrip(self, handler):
        encoded = await handler.encode(SIMPLE_RECORDS)
        decoded = await handler.decode(encoded)
        # CSV deserializes all values as strings
        assert len(decoded) == 3
        assert decoded[0]["name"] == "alice"

    async def test_empty_records(self, handler):
        encoded = await handler.encode([])
        assert encoded == b""

    async def test_header_present(self, handler):
        encoded = await handler.encode(SIMPLE_RECORDS)
        first_line = encoded.decode("utf-8").split("\r\n")[0]
        assert "id" in first_line
        assert "name" in first_line

    def test_properties(self, handler):
        assert handler.format_name == "csv"
        assert handler.content_type == "text/csv"
        assert handler.file_extension == ".csv"


# ── Parquet (pyarrow) ──


class TestParquetHandler:
    @pytest.fixture
    def handler(self):
        pytest.importorskip("pyarrow")
        from demiurge_testdata.handlers.formats.parquet_handler import ParquetFormatHandler

        return ParquetFormatHandler()

    async def test_roundtrip(self, handler):
        encoded = await handler.encode(SIMPLE_RECORDS)
        decoded = await handler.decode(encoded)
        assert len(decoded) == 3
        assert decoded[0]["name"] == "alice"

    async def test_encode_returns_bytes(self, handler):
        result = await handler.encode(SIMPLE_RECORDS)
        assert isinstance(result, bytes)
        # Parquet magic bytes: PAR1
        assert result[:4] == b"PAR1"

    def test_properties(self, handler):
        assert handler.format_name == "parquet"
        assert handler.file_extension == ".parquet"


# ── Avro (fastavro) ──


class TestAvroHandler:
    @pytest.fixture
    def handler(self):
        pytest.importorskip("fastavro")
        from demiurge_testdata.handlers.formats.avro_handler import AvroFormatHandler

        return AvroFormatHandler()

    async def test_roundtrip(self, handler):
        encoded = await handler.encode(SIMPLE_RECORDS)
        decoded = await handler.decode(encoded)
        assert len(decoded) == 3
        assert decoded[0]["name"] == "alice"

    async def test_encode_returns_bytes(self, handler):
        result = await handler.encode(SIMPLE_RECORDS)
        assert isinstance(result, bytes)
        # Avro magic bytes: Obj\x01
        assert result[:4] == b"Obj\x01"

    async def test_with_explicit_schema(self):
        pytest.importorskip("fastavro")
        from demiurge_testdata.handlers.formats.avro_handler import AvroFormatHandler

        schema = {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
            ],
        }
        handler = AvroFormatHandler(schema=schema)
        records = [{"id": 1, "name": "test"}]
        encoded = await handler.encode(records)
        decoded = await handler.decode(encoded)
        assert decoded[0]["name"] == "test"

    def test_properties(self, handler):
        assert handler.format_name == "avro"
        assert handler.file_extension == ".avro"


# ── ORC (pyarrow.orc) ──


class TestOrcHandler:
    @pytest.fixture
    def handler(self):
        pytest.importorskip("pyarrow.orc")
        from demiurge_testdata.handlers.formats.orc_handler import OrcFormatHandler

        return OrcFormatHandler()

    async def test_roundtrip(self, handler):
        encoded = await handler.encode(SIMPLE_RECORDS)
        decoded = await handler.decode(encoded)
        assert len(decoded) == 3
        assert decoded[0]["name"] == "alice"

    async def test_encode_returns_bytes(self, handler):
        result = await handler.encode(SIMPLE_RECORDS)
        assert isinstance(result, bytes)
        # ORC magic bytes
        assert result[:3] == b"ORC"

    def test_properties(self, handler):
        assert handler.format_name == "orc"
        assert handler.file_extension == ".orc"


# ── MessagePack (msgspec) ──


class TestMsgpackHandler:
    @pytest.fixture
    def handler(self):
        pytest.importorskip("msgspec")
        from demiurge_testdata.handlers.formats.msgpack_handler import MsgpackFormatHandler

        return MsgpackFormatHandler()

    async def test_roundtrip(self, handler):
        encoded = await handler.encode(SIMPLE_RECORDS)
        decoded = await handler.decode(encoded)
        assert decoded == SIMPLE_RECORDS

    async def test_encode_returns_bytes(self, handler):
        result = await handler.encode(SIMPLE_RECORDS)
        assert isinstance(result, bytes)
        assert len(result) > 0

    async def test_compact_vs_json(self, handler):
        pytest.importorskip("orjson")
        from demiurge_testdata.handlers.formats.json_handler import JsonFormatHandler

        json_handler = JsonFormatHandler()
        msgpack_encoded = await handler.encode(SIMPLE_RECORDS)
        json_encoded = await json_handler.encode(SIMPLE_RECORDS)
        # MessagePack should be more compact
        assert len(msgpack_encoded) < len(json_encoded)

    def test_properties(self, handler):
        assert handler.format_name == "msgpack"
        assert handler.content_type == "application/x-msgpack"
        assert handler.file_extension == ".msgpack"


# ── Arrow IPC (pyarrow) ──


class TestArrowHandler:
    @pytest.fixture
    def handler(self):
        pytest.importorskip("pyarrow")
        from demiurge_testdata.handlers.formats.arrow_handler import ArrowFormatHandler

        return ArrowFormatHandler()

    async def test_roundtrip(self, handler):
        encoded = await handler.encode(SIMPLE_RECORDS)
        decoded = await handler.decode(encoded)
        assert len(decoded) == 3
        assert decoded[0]["name"] == "alice"

    async def test_encode_returns_bytes(self, handler):
        result = await handler.encode(SIMPLE_RECORDS)
        assert isinstance(result, bytes)
        assert len(result) > 0

    def test_properties(self, handler):
        assert handler.format_name == "arrow"
        assert handler.file_extension == ".arrow"


# ── XML (lxml) ──


class TestXmlHandler:
    @pytest.fixture
    def handler(self):
        pytest.importorskip("lxml")
        from demiurge_testdata.handlers.formats.xml_handler import XmlFormatHandler

        return XmlFormatHandler()

    async def test_roundtrip(self, handler):
        # XML serializes all values as strings
        records = [{"id": "1", "name": "alice"}, {"id": "2", "name": "bob"}]
        encoded = await handler.encode(records)
        decoded = await handler.decode(encoded)
        assert len(decoded) == 2
        assert decoded[0]["name"] == "alice"

    async def test_xml_declaration(self, handler):
        encoded = await handler.encode(SINGLE_RECORD)
        assert encoded.startswith(b"<?xml")

    async def test_custom_tags(self):
        pytest.importorskip("lxml")
        from demiurge_testdata.handlers.formats.xml_handler import XmlFormatHandler

        handler = XmlFormatHandler(root_tag="data", item_tag="item")
        records = [{"k": "v"}]
        encoded = await handler.encode(records)
        assert b"<data>" in encoded
        assert b"<item>" in encoded

    def test_properties(self, handler):
        assert handler.format_name == "xml"
        assert handler.content_type == "application/xml"
        assert handler.file_extension == ".xml"


# ── YAML (pyyaml) ──


class TestYamlHandler:
    @pytest.fixture
    def handler(self):
        from demiurge_testdata.handlers.formats.yaml_handler import YamlFormatHandler

        return YamlFormatHandler()

    async def test_roundtrip(self, handler):
        encoded = await handler.encode(SIMPLE_RECORDS)
        decoded = await handler.decode(encoded)
        assert decoded == SIMPLE_RECORDS

    async def test_encode_returns_bytes(self, handler):
        result = await handler.encode(SIMPLE_RECORDS)
        assert isinstance(result, bytes)
        text = result.decode("utf-8")
        assert "alice" in text

    async def test_unicode_support(self, handler):
        records = [{"name": "test"}]
        encoded = await handler.encode(records)
        decoded = await handler.decode(encoded)
        assert decoded[0]["name"] == "test"

    def test_properties(self, handler):
        assert handler.format_name == "yaml"
        assert handler.content_type == "text/yaml"
        assert handler.file_extension == ".yaml"
