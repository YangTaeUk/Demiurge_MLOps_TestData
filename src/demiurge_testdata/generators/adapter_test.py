"""AdapterTestGenerator -- DL 어댑터 통합 테스트용 공통 스키마 데이터 생성기.

Kaggle 다운로드 없이 자체적으로 6컬럼 공통 스키마 레코드를 생성한다.
요청서 스펙: id(INT), name(VARCHAR), value(FLOAT), category(VARCHAR),
            created_at(TIMESTAMP), is_active(BOOLEAN)
"""

from __future__ import annotations

import random
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from typing import Any

from demiurge_testdata.core.enums import DatasetCategory
from demiurge_testdata.core.registry import generator_registry
from demiurge_testdata.generators.base import BaseGenerator

# ── 한글 이름 풀 (UTF-8 검증용) ──
_KOREAN_NAMES: list[str] = [
    "홍길동", "김철수", "이영희", "박민수", "정수연",
    "최지훈", "강서윤", "조현우", "윤미래", "장도윤",
    "한소희", "임재현", "오서준", "배수지", "신동욱",
    "권나은", "유정호", "문서영", "황민정", "송태양",
]

_CATEGORIES: list[str] = ["A", "B", "C", "D", "E"]

# ── 공통 스키마 SQL 타입 정의 (seed-test에서 DDL 생성 시 사용) ──
ADAPTER_TEST_COLUMNS: dict[str, str] = {
    "id": "INTEGER",
    "name": "VARCHAR(100)",
    "value": "DOUBLE PRECISION",
    "category": "VARCHAR(50)",
    "created_at": "TIMESTAMP",
    "is_active": "BOOLEAN",
}


def generate_records(
    count: int,
    *,
    seed: int | None = None,
    include_zero_date: bool = False,
    start_id: int = 1,
) -> list[dict[str, Any]]:
    """공통 스키마 레코드를 생성한다.

    Args:
        count: 생성할 레코드 수 (0 이상)
        seed: 랜덤 시드 (재현성)
        include_zero_date: True이면 마지막 행에 zero-date 삽입 (MySQL 엣지 케이스)
        start_id: 시작 ID 값

    Raises:
        ValueError: count가 음수인 경우
    """
    if count < 0:
        raise ValueError(f"count는 0 이상이어야 합니다: {count}")

    rng = random.Random(seed)
    base_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    records: list[dict[str, Any]] = []

    for i in range(count):
        record_id = start_id + i
        dt = base_date + timedelta(
            days=rng.randint(0, 365),
            hours=rng.randint(0, 23),
            minutes=rng.randint(0, 59),
            seconds=rng.randint(0, 59),
        )
        records.append({
            "id": record_id,
            "name": rng.choice(_KOREAN_NAMES),
            "value": round(rng.uniform(0.01, 99999.99), 2),
            "category": rng.choice(_CATEGORIES),
            "created_at": dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "is_active": rng.choice([True, False]),
        })

    if include_zero_date and records:
        records[-1]["created_at"] = "0000-00-00 00:00:00"

    return records


@generator_registry.register("adapter_test")
class AdapterTestGenerator(BaseGenerator):
    """DL 어댑터 통합 테스트용 공통 스키마 생성기.

    CsvGenerator를 상속하지 않음 -- Kaggle CSV 없이 자체 데이터 생성.
    """

    def __init__(
        self,
        config: Any = None,
        *,
        count: int = 500,
        seed: int | None = 42,
        include_zero_date: bool = False,
    ):
        self._count = count
        self._seed = seed
        self._include_zero_date = include_zero_date
        # config 객체가 있으면 batch_size, stream_interval_ms 참조
        self._batch_size = getattr(config, "batch_size", count)
        self._stream_interval_ms = getattr(config, "stream_interval_ms", 100)

    @property
    def dataset_name(self) -> str:
        return "adapter_test"

    @property
    def category(self) -> DatasetCategory:
        return DatasetCategory.RELATIONAL

    async def batch(self, batch_size: int = 1000) -> list[dict]:
        """지정 건수의 공통 스키마 레코드를 반환한다."""
        effective = min(batch_size, self._count)
        return generate_records(
            effective,
            seed=self._seed,
            include_zero_date=self._include_zero_date,
        )

    async def stream(self) -> AsyncIterator[dict]:
        """레코드를 하나씩 비동기로 yield한다."""
        import asyncio

        records = generate_records(
            self._count,
            seed=self._seed,
            include_zero_date=self._include_zero_date,
        )
        interval = self._stream_interval_ms / 1000.0
        for rec in records:
            yield rec
            await asyncio.sleep(interval)
