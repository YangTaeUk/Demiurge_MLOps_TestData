"""SeedPipeline — CSV 원본 데이터를 인프라에 구조화된 형태로 시딩한다.

push(bytes)와 달리, 어댑터 카테고리에 따라 적재 전략이 달라진다:
- RDBMS:     create_table(schema) → bulk_insert(records)
- NoSQL:     insert_documents(collection, documents)
- Storage:   HandlerChain.encode() → adapter.write(key, bytes)
- Streaming: HandlerChain.encode() → adapter.publish(topic, msg)
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

from demiurge_testdata.adapters.base import (
    BaseNoSQLAdapter,
    BaseRDBMSAdapter,
    BaseStorageAdapter,
    BaseStreamAdapter,
)
from demiurge_testdata.core.exceptions import BulkInsertError, SchemaInferenceError
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.handlers.chain import HandlerChain

logger = logging.getLogger(__name__)


# ── CSV 로딩 ──


def load_csv_records(
    path: Path,
    *,
    limit: int | None = None,
    encoding: str = "utf-8",
) -> list[dict[str, Any]]:
    """CSV 파일을 읽어 list[dict]로 반환한다.

    Args:
        path: CSV 파일 경로
        limit: 최대 읽을 행 수 (None이면 전체)
        encoding: 파일 인코딩 (기본 utf-8, 실패 시 latin-1 재시도)
    """
    try:
        return _read_csv(path, limit=limit, encoding=encoding)
    except UnicodeDecodeError:
        logger.warning("%s: utf-8 실패 → latin-1 재시도", path)
        return _read_csv(path, limit=limit, encoding="latin-1")


def _read_csv(
    path: Path, *, limit: int | None, encoding: str
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with open(path, newline="", encoding=encoding) as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
            if limit and len(records) >= limit:
                break
    return records


def load_sqlite_records(
    path: Path,
    *,
    limit: int | None = None,
    table: str | None = None,
) -> list[dict[str, Any]]:
    """SQLite 파일에서 레코드를 읽어 list[dict]로 반환한다.

    Args:
        path: SQLite 파일 경로
        limit: 최대 읽을 행 수
        table: 읽을 테이블명 (None이면 가장 큰 테이블 자동 선택)
    """
    import sqlite3

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        if table is None:
            # 가장 행이 많은 테이블 선택
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            tables = [r[0] for r in cur.fetchall()]
            if not tables:
                return []
            best, best_count = tables[0], 0
            for t in tables:
                cnt = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]  # noqa: S608
                if cnt > best_count:
                    best, best_count = t, cnt
            table = best

        limit_clause = f" LIMIT {limit}" if limit else ""
        cur = conn.execute(f"SELECT * FROM [{table}]{limit_clause}")  # noqa: S608
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


# ── 스키마 추론 ──


_SAMPLE_SIZE = 1000

_DATE_PATTERNS = (
    r"\d{4}-\d{2}-\d{2}",  # 2020-01-15
    r"\d{2}/\d{2}/\d{4}",  # 01/15/2020
)


def infer_columns(
    records: list[dict[str, Any]], sample_size: int = _SAMPLE_SIZE
) -> dict[str, str]:
    """레코드에서 SQL 컬럼 타입을 추론한다.

    Returns:
        {"column_name": "SQL_TYPE", ...}
        예: {"sk_id_curr": "BIGINT", "amt_income_total": "DOUBLE PRECISION"}
    """
    import re

    if not records:
        raise SchemaInferenceError("빈 레코드에서 스키마를 추론할 수 없습니다")

    sample = records[:sample_size]
    columns: dict[str, str] = {}

    for col in sample[0]:
        values = [r.get(col) for r in sample if r.get(col) not in ("", None, "nan", "NaN")]

        if not values:
            columns[col] = "TEXT"
            continue

        # 정수 검사
        if all(_is_int(v) for v in values):
            max_val = max(abs(int(float(v))) for v in values)
            columns[col] = "BIGINT" if max_val > 2_147_483_647 else "INTEGER"
            continue

        # 실수 검사
        if all(_is_float(v) for v in values):
            columns[col] = "DOUBLE PRECISION"
            continue

        # 날짜 검사
        str_values = [str(v) for v in values[:100]]
        if any(
            all(re.match(pat, sv) for sv in str_values)
            for pat in _DATE_PATTERNS
        ):
            columns[col] = "TIMESTAMP"
            continue

        # 문자열 길이 기반
        max_len = max(len(str(v)) for v in values)
        columns[col] = "TEXT" if max_len > 255 else "VARCHAR(255)"

    return columns


def _is_int(v: Any) -> bool:
    try:
        f = float(v)
        return f == int(f)
    except (ValueError, TypeError):
        return False


def _is_float(v: Any) -> bool:
    try:
        float(v)
        return True
    except (ValueError, TypeError):
        return False


# ── 타입 캐스팅 ──


def _cast_records(
    records: list[dict[str, Any]], columns: dict[str, str]
) -> list[dict[str, Any]]:
    """CSV 문자열 값을 SQL 타입에 맞는 Python 타입으로 변환한다."""
    from datetime import datetime

    _TYPE_CASTERS: dict[str, Any] = {
        "INTEGER": lambda v: int(float(v)) if v not in ("", None) else None,
        "BIGINT": lambda v: int(float(v)) if v not in ("", None) else None,
        "DOUBLE PRECISION": lambda v: float(v) if v not in ("", None) else None,
        "TIMESTAMP": lambda v: datetime.fromisoformat(v.replace("/", "-")) if v not in ("", None) else None,
    }

    casted = []
    for rec in records:
        new_rec: dict[str, Any] = {}
        for col, val in rec.items():
            sql_type = columns.get(col, "TEXT")
            caster = _TYPE_CASTERS.get(sql_type)
            if caster and val not in ("", None, "nan", "NaN"):
                try:
                    new_rec[col] = caster(val)
                except (ValueError, TypeError):
                    new_rec[col] = val
            else:
                new_rec[col] = val if val not in ("", None) else None
        casted.append(new_rec)
    return casted


# ── SeedPipeline ──


class SeedPipeline:
    """CSV 원본 데이터를 인프라에 구조화된 형태로 시딩한다."""

    def __init__(
        self,
        adapter_type: str,
        adapter_config: dict[str, Any],
        handler_chain: HandlerChain | None = None,
    ):
        self._adapter = adapter_registry.create(adapter_type, **adapter_config)
        self._adapter_type = adapter_type
        self._handler_chain = handler_chain

    async def seed_rdbms(
        self,
        table: str,
        records: list[dict[str, Any]],
        *,
        columns: dict[str, str] | None = None,
        batch_size: int = 5000,
        drop_existing: bool = False,
    ) -> int:
        """RDBMS에 구조화된 테이블을 생성하고 레코드를 벌크 삽입한다."""
        if not isinstance(self._adapter, BaseRDBMSAdapter):
            raise TypeError(
                f"RDBMS 시딩에는 BaseRDBMSAdapter가 필요합니다 (got {type(self._adapter).__name__})"
            )

        if columns is None:
            columns = infer_columns(records)

        adapter = self._adapter
        async with adapter:
            if drop_existing:
                await adapter.execute_sql(f"DROP TABLE IF EXISTS {table}")

            await adapter.create_table(table, columns)

            # CSV 문자열 → Python 타입 캐스팅
            records = _cast_records(records, columns)

            total = 0
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                try:
                    inserted = await adapter.bulk_insert(table, batch)
                    total += inserted
                except Exception as exc:
                    raise BulkInsertError(
                        adapter=self._adapter_type,
                        table=table,
                        batch_index=i // batch_size,
                        cause=exc,
                    ) from exc
                logger.info(
                    "[%s] %s: %d / %d rows inserted",
                    self._adapter_type, table, total, len(records),
                )
            return total

    async def seed_nosql(
        self,
        collection: str,
        documents: list[dict[str, Any]],
        *,
        batch_size: int = 1000,
    ) -> int:
        """NoSQL에 JSON 문서를 삽입한다."""
        if not isinstance(self._adapter, BaseNoSQLAdapter):
            raise TypeError(
                f"NoSQL 시딩에는 BaseNoSQLAdapter가 필요합니다 (got {type(self._adapter).__name__})"
            )

        adapter = self._adapter
        async with adapter:
            total = 0
            for i in range(0, len(documents), batch_size):
                batch = documents[i : i + batch_size]
                count = await adapter.insert_documents(collection, batch)
                total += count
                logger.info(
                    "[%s] %s: %d / %d docs inserted",
                    self._adapter_type, collection, total, len(documents),
                )
            return total

    async def seed_storage(
        self,
        key: str,
        records: list[dict[str, Any]],
    ) -> int:
        """Storage에 직렬화된 파일을 업로드한다."""
        if not isinstance(self._adapter, BaseStorageAdapter):
            raise TypeError(
                f"Storage 시딩에는 BaseStorageAdapter가 필요합니다 (got {type(self._adapter).__name__})"
            )
        if self._handler_chain is None:
            raise ValueError("Storage 시딩에는 HandlerChain이 필요합니다")

        adapter = self._adapter
        async with adapter:
            encoded = await self._handler_chain.encode(records)
            await adapter.write(key, encoded)
            logger.info(
                "[%s] %s: %d records → %d bytes written",
                self._adapter_type, key, len(records), len(encoded),
            )
            return len(records)

    async def seed_streaming(
        self,
        topic: str,
        records: list[dict[str, Any]],
        *,
        batch_size: int = 100,
    ) -> int:
        """Streaming 브로커에 메시지를 발행한다."""
        if not isinstance(self._adapter, BaseStreamAdapter):
            raise TypeError(
                f"Streaming 시딩에는 BaseStreamAdapter가 필요합니다 (got {type(self._adapter).__name__})"
            )
        if self._handler_chain is None:
            raise ValueError("Streaming 시딩에는 HandlerChain이 필요합니다")

        adapter = self._adapter
        async with adapter:
            total = 0
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                encoded = await self._handler_chain.encode(batch)
                await adapter.publish(topic, encoded)
                total += len(batch)
            logger.info(
                "[%s] %s: %d messages published",
                self._adapter_type, topic, total,
            )
            return total
