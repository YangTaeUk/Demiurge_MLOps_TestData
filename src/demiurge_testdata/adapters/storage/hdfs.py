"""HDFSAdapter — fsspec 기반 HDFS Storage 어댑터"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import fsspec

from demiurge_testdata.adapters.base import BaseStorageAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import StorageAdapterConfig


@adapter_registry.register("hdfs")
class HDFSAdapter(BaseStorageAdapter):
    """HDFS 어댑터.

    fsspec을 사용하여 HDFS에 연결한다.
    WebHDFS 프로토콜을 기본으로 사용한다.
    """

    def __init__(self, config: StorageAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = StorageAdapterConfig(**kwargs)
        self._config = config
        self._fs: fsspec.AbstractFileSystem | None = None

    async def connect(self) -> None:
        host = self._config.endpoint or "http://localhost:9870"
        self._fs = fsspec.filesystem("webhdfs", host=host)

    async def disconnect(self) -> None:
        self._fs = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        key = metadata.get("key", "data.bin")
        base = self._config.base_path or "/demiurge_testdata"
        full_path = f"{base}/{self._config.prefix}{key}"

        await asyncio.to_thread(self._fs.makedirs, f"{base}/{self._config.prefix}", exist_ok=True)
        await asyncio.to_thread(self._fs.pipe_file, full_path, data)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        key = query.get("key")
        base = self._config.base_path or "/demiurge_testdata"

        if key:
            full_path = f"{base}/{self._config.prefix}{key}"
            data = await asyncio.to_thread(self._fs.cat_file, full_path)
            yield data
        else:
            prefix = query.get("prefix", "")
            full_prefix = f"{base}/{self._config.prefix}{prefix}"
            files = await asyncio.to_thread(self._fs.ls, full_prefix, detail=False)
            for i, f in enumerate(files):
                if limit and i >= limit:
                    break
                data = await asyncio.to_thread(self._fs.cat_file, f)
                yield data

    async def health_check(self) -> bool:
        if not self._fs:
            return False
        try:
            await asyncio.to_thread(self._fs.ls, "/")
            return True
        except Exception:
            return False

    async def write(self, key: str, data: bytes) -> None:
        base = self._config.base_path or "/demiurge_testdata"
        full_path = f"{base}/{self._config.prefix}{key}"
        parent = "/".join(full_path.rsplit("/", 1)[:-1])
        await asyncio.to_thread(self._fs.makedirs, parent, exist_ok=True)
        await asyncio.to_thread(self._fs.pipe_file, full_path, data)

    async def read(self, key: str) -> bytes:
        base = self._config.base_path or "/demiurge_testdata"
        full_path = f"{base}/{self._config.prefix}{key}"
        return await asyncio.to_thread(self._fs.cat_file, full_path)

    async def list_keys(self, prefix: str = "") -> list[str]:
        base = self._config.base_path or "/demiurge_testdata"
        full_prefix = f"{base}/{self._config.prefix}{prefix}"
        try:
            files = await asyncio.to_thread(self._fs.ls, full_prefix, detail=False)
            base_prefix = f"{base}/"
            return [f.removeprefix(base_prefix) for f in files]
        except FileNotFoundError:
            return []

    async def delete(self, key: str) -> None:
        base = self._config.base_path or "/demiurge_testdata"
        full_path = f"{base}/{self._config.prefix}{key}"
        await asyncio.to_thread(self._fs.rm, full_path)
