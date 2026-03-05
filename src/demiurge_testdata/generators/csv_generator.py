"""CsvGenerator — CSV 기반 제너레이터 공통 구현"""

from __future__ import annotations

import asyncio
import csv
import random
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from demiurge_testdata.core.config import GeneratorConfig
from demiurge_testdata.core.exceptions import GeneratorError
from demiurge_testdata.generators.base import BaseGenerator


def safe_int(value: Any) -> int | None:
    """안전한 int 변환. 실패 시 None 반환."""
    if value in ("", None, "nan", "NaN"):
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def safe_float(value: Any) -> float | None:
    """안전한 float 변환. 실패 시 None 반환."""
    if value in ("", None, "nan", "NaN"):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


class CsvGenerator(BaseGenerator):
    """CSV 파일 기반 제너레이터 공통 구현.

    모든 CSV 기반 제너레이터는 이 클래스를 상속하여:
    - dataset_name, category 프로퍼티만 정의
    - _transform() 오버라이드로 데이터셋 고유 변환 적용
    - _csv_files 프로퍼티로 읽을 CSV 파일명 지정
    """

    def __init__(
        self,
        config: GeneratorConfig,
        *,
        records: list[dict[str, Any]] | None = None,
    ):
        self._config = config
        self._records_override = records
        self._loaded_records: list[dict[str, Any]] | None = None

    @property
    def _csv_files(self) -> list[str]:
        """읽을 CSV 파일명 목록. 서브클래스에서 오버라이드."""
        return [f"{self.dataset_name}.csv"]

    def _get_records(self) -> list[dict[str, Any]]:
        """레코드를 반환한다 (lazy loading + caching)."""
        if self._records_override is not None:
            return self._records_override

        if self._loaded_records is None:
            self._loaded_records = self._load_csv()
        return self._loaded_records

    def _load_csv(self) -> list[dict[str, Any]]:
        """CSV 파일을 읽어 list[dict]로 반환한다."""
        if not self._config.data_path:
            raise GeneratorError(f"[{self.dataset_name}] data_path is required for CSV loading")

        base_path = Path(self._config.data_path)
        all_records: list[dict[str, Any]] = []

        for csv_file in self._csv_files:
            file_path = base_path / csv_file if not base_path.suffix else base_path
            if not file_path.exists():
                raise GeneratorError(f"[{self.dataset_name}] CSV not found: {file_path}")

            with open(file_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    all_records.append(dict(row))

                    # max_records 제한
                    if self._config.max_records and len(all_records) >= self._config.max_records:
                        break

            if self._config.max_records and len(all_records) >= self._config.max_records:
                break

        # 셔플
        if self._config.shuffle:
            rng = random.Random(self._config.seed)
            rng.shuffle(all_records)

        # max_records 최종 적용
        if self._config.max_records:
            all_records = all_records[: self._config.max_records]

        return all_records

    def _transform(self, record: dict[str, Any]) -> dict[str, Any]:
        """데이터셋 고유 변환. 서브클래스에서 오버라이드."""
        return record

    def _apply_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """변환을 적용한 레코드 목록 반환."""
        return [self._transform(r) for r in records]

    async def batch(self, batch_size: int = 1000) -> list[dict]:
        """Batch 모드: batch_size만큼 레코드를 변환하여 반환."""
        records = self._get_records()[:batch_size]
        return self._apply_records(records)

    async def stream(self) -> AsyncIterator[dict]:
        """Stream 모드: interval_ms 간격으로 레코드를 하나씩 생산."""
        interval = self._config.stream_interval_ms / 1000
        max_records = self._config.max_records

        for i, record in enumerate(self._get_records()):
            if max_records and i >= max_records:
                break
            yield self._transform(record)
            if interval > 0:
                await asyncio.sleep(interval)

    async def fetch(self, offset: int = 0, limit: int = 100) -> list[dict]:
        """API 모드: offset/limit 기반 페이지네이션."""
        records = self._get_records()[offset : offset + limit]
        return self._apply_records(records)

    @property
    def total_records(self) -> int:
        """로드된 전체 레코드 수."""
        return len(self._get_records())
