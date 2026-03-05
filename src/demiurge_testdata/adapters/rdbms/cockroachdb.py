"""CockroachDBAdapter — asyncpg 기반 RDBMS 어댑터 (PostgreSQL 호환)"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.adapters.rdbms.postgresql import PostgreSQLAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import RDBMSAdapterConfig


@adapter_registry.register("cockroachdb")
class CockroachDBAdapter(PostgreSQLAdapter):
    """CockroachDB 어댑터.

    PostgreSQL 와이어 프로토콜 호환이므로 PostgreSQLAdapter를 상속한다.
    기본 포트(26257)와 DSN만 변경한다.
    """

    def __init__(self, config: RDBMSAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = RDBMSAdapterConfig(port=26257, **kwargs)
        super().__init__(config=config)

    @property
    def dsn(self) -> str:
        c = self._config
        return f"cockroachdb+asyncpg://{c.user}:{c.password}@{c.host}:{c.port}/{c.database}"
