"""BigQueryAdapter — google-cloud-bigquery 기반 RDBMS 어댑터"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from google.cloud import bigquery

from demiurge_testdata.adapters.base import BaseRDBMSAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import BigQueryAdapterConfig


@adapter_registry.register("bigquery")
class BigQueryAdapter(BaseRDBMSAdapter):
    """BigQuery 어댑터.

    google-cloud-bigquery를 사용하여 BigQuery에 연결한다.
    에뮬레이터(bigquery-emulator) 지원을 포함한다.
    """

    def __init__(self, config: BigQueryAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = BigQueryAdapterConfig(**kwargs)
        self._config = config
        self._client: bigquery.Client | None = None

    async def connect(self) -> None:
        self._client = bigquery.Client(
            project=self._config.project_id,
        )

    async def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        table_id = metadata.get("table", "raw_data")
        dataset = metadata.get("dataset", self._config.dataset_id)
        full_table_id = f"{self._config.project_id}.{dataset}.{table_id}"

        rows = [
            {
                "data": data.decode("utf-8", errors="replace"),
                "format": metadata.get("format", "unknown"),
                "compression": metadata.get("compression", "none"),
                "record_count": metadata.get("record_count", 0),
            }
        ]
        errors = self._client.insert_rows_json(full_table_id, rows)
        if errors:
            msg = f"BigQuery insert errors: {errors}"
            raise RuntimeError(msg)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        table_id = query.get("table", "raw_data")
        dataset = query.get("dataset", self._config.dataset_id)
        sql = f"SELECT * FROM `{self._config.project_id}.{dataset}.{table_id}`"
        if limit:
            sql += f" LIMIT {limit}"

        query_job = self._client.query(sql)
        for row in query_job:
            yield json.dumps(dict(row), default=str).encode("utf-8")

    async def health_check(self) -> bool:
        if not self._client:
            return False
        try:
            query_job = self._client.query("SELECT 1")
            results = list(query_job)
            return len(results) == 1
        except Exception:
            return False

    async def execute_sql(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        job_config = None
        if params:
            query_params = [
                bigquery.ScalarQueryParameter(k, "STRING", str(v)) for k, v in params.items()
            ]
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        query_job = self._client.query(query, job_config=job_config)
        return [dict(row) for row in query_job]

    async def create_table(self, name: str, columns: dict[str, str]) -> None:
        dataset = self._config.dataset_id
        table_id = f"{self._config.project_id}.{dataset}.{name}"

        bq_type_map = {
            "TEXT": "STRING",
            "VARCHAR": "STRING",
            "INT": "INTEGER",
            "INTEGER": "INTEGER",
            "FLOAT": "FLOAT64",
            "BOOLEAN": "BOOL",
            "TIMESTAMP": "TIMESTAMP",
            "BYTEA": "BYTES",
            "BLOB": "BYTES",
        }
        schema = []
        for col, dtype in columns.items():
            bq_type = bq_type_map.get(dtype.upper().split("(")[0], "STRING")
            schema.append(bigquery.SchemaField(col, bq_type))

        table = bigquery.Table(table_id, schema=schema)
        self._client.create_table(table, exists_ok=True)

    async def bulk_insert(self, table: str, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        dataset = self._config.dataset_id
        full_table_id = f"{self._config.project_id}.{dataset}.{table}"

        errors = self._client.insert_rows_json(full_table_id, records)
        if errors:
            msg = f"BigQuery bulk insert errors: {errors}"
            raise RuntimeError(msg)
        return len(records)
