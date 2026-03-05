"""CsvGenerator 기반 공통 기능 테스트 — CSV 로딩, 셔플, 모드별 동작"""

from __future__ import annotations

import csv

import pytest

from demiurge_testdata.core.config import GeneratorConfig
from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.generators.csv_generator import CsvGenerator

# ── 테스트용 구체 Generator ──


class SampleGenerator(CsvGenerator):
    @property
    def dataset_name(self) -> str:
        return "sample"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.RELATIONAL


SAMPLE_RECORDS = [
    {"id": "1", "name": "alice", "score": "95"},
    {"id": "2", "name": "bob", "score": "87"},
    {"id": "3", "name": "charlie", "score": "92"},
    {"id": "4", "name": "diana", "score": "88"},
    {"id": "5", "name": "eve", "score": "91"},
]


@pytest.fixture
def config():
    return GeneratorConfig(type="sample")


@pytest.fixture
def generator(config):
    return SampleGenerator(config, records=SAMPLE_RECORDS)


# ── Batch 모드 ──


class TestBatchMode:
    async def test_batch_returns_all(self, generator):
        result = await generator.batch(batch_size=10)
        assert len(result) == 5

    async def test_batch_limits_size(self, generator):
        result = await generator.batch(batch_size=3)
        assert len(result) == 3

    async def test_batch_returns_dicts(self, generator):
        result = await generator.batch(batch_size=2)
        assert all(isinstance(r, dict) for r in result)


# ── Stream 모드 ──


class TestStreamMode:
    async def test_stream_yields_all(self, generator):
        results = [r async for r in generator.stream()]
        assert len(results) == 5

    async def test_stream_yields_dicts(self, generator):
        async for record in generator.stream():
            assert isinstance(record, dict)
            break

    async def test_stream_with_max_records(self, config):
        cfg = config.model_copy(update={"max_records": 2})
        gen = SampleGenerator(cfg, records=SAMPLE_RECORDS)
        results = [r async for r in gen.stream()]
        assert len(results) == 2


# ── API (fetch) 모드 ──


class TestFetchMode:
    async def test_fetch_offset_limit(self, generator):
        result = await generator.fetch(offset=1, limit=2)
        assert len(result) == 2

    async def test_fetch_offset_zero(self, generator):
        result = await generator.fetch(offset=0, limit=3)
        assert len(result) == 3

    async def test_fetch_beyond_range(self, generator):
        result = await generator.fetch(offset=10, limit=5)
        assert len(result) == 0


# ── 셔플 & 시드 재현성 ──


class TestShuffleAndSeed:
    async def test_seed_reproducibility(self, config):
        cfg = config.model_copy(update={"seed": 42, "shuffle": True})
        gen1 = SampleGenerator(cfg, records=SAMPLE_RECORDS.copy())
        gen2 = SampleGenerator(cfg, records=SAMPLE_RECORDS.copy())
        r1 = await gen1.batch(batch_size=5)
        r2 = await gen2.batch(batch_size=5)
        assert r1 == r2

    async def test_no_shuffle_preserves_order(self, config):
        cfg = config.model_copy(update={"shuffle": False})
        gen = SampleGenerator(cfg, records=SAMPLE_RECORDS)
        result = await gen.batch(batch_size=5)
        assert result[0]["name"] == "alice"
        assert result[-1]["name"] == "eve"


# ── CSV 파일 로딩 ──


class TestCsvLoading:
    def test_csv_loading_from_file(self, tmp_path):
        csv_path = tmp_path / "sample.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "score"])
            writer.writeheader()
            for row in SAMPLE_RECORDS:
                writer.writerow(row)

        config = GeneratorConfig(type="sample", data_path=str(csv_path), shuffle=False)
        gen = SampleGenerator(config)
        records = gen._get_records()
        assert len(records) == 5
        assert records[0]["name"] == "alice"

    def test_csv_loading_with_max_records(self, tmp_path):
        csv_path = tmp_path / "sample.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "name", "score"])
            writer.writeheader()
            for row in SAMPLE_RECORDS:
                writer.writerow(row)

        config = GeneratorConfig(
            type="sample", data_path=str(csv_path), shuffle=False, max_records=2
        )
        gen = SampleGenerator(config)
        records = gen._get_records()
        assert len(records) == 2

    def test_missing_data_path_raises(self):
        config = GeneratorConfig(type="sample")
        gen = SampleGenerator(config)
        with pytest.raises(Exception, match="data_path"):
            gen._get_records()

    def test_missing_csv_file_raises(self, tmp_path):
        config = GeneratorConfig(
            type="sample", data_path=str(tmp_path / "nonexistent.csv"), shuffle=False
        )
        gen = SampleGenerator(config)
        with pytest.raises(Exception, match="CSV not found"):
            gen._get_records()


# ── 프로퍼티 ──


class TestProperties:
    def test_dataset_name(self, generator):
        assert generator.dataset_name == "sample"

    def test_category(self, generator):
        assert generator.category == DatasetCategory.RELATIONAL

    def test_supported_modes(self, generator):
        assert len(generator.supported_modes) == 3

    def test_total_records(self, generator):
        assert generator.total_records == 5
