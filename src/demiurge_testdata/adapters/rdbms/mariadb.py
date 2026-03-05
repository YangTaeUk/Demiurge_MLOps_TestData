"""MariaDBAdapter — aiomysql 기반 RDBMS 어댑터 (MySQL 호환)"""

from __future__ import annotations

from typing import Any

from demiurge_testdata.adapters.rdbms.mysql import MySQLAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import RDBMSAdapterConfig


@adapter_registry.register("mariadb")
class MariaDBAdapter(MySQLAdapter):
    """MariaDB 어댑터.

    MySQL과 프로토콜 호환이므로 MySQLAdapter를 상속하여 DSN만 변경한다.
    """

    def __init__(self, config: RDBMSAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = RDBMSAdapterConfig(port=3307, **kwargs)
        super().__init__(config=config)

    @property
    def dsn(self) -> str:
        c = self._config
        return f"mysql+aiomysql://{c.user}:{c.password}@{c.host}:{c.port}/{c.database}"
