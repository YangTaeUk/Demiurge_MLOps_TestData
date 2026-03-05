"""LocalFSAdapter — 로컬 파일시스템 기반 Storage 어댑터"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import aiofiles

from demiurge_testdata.adapters.base import BaseStorageAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import StorageAdapterConfig


@adapter_registry.register("local_fs")
class LocalFSAdapter(BaseStorageAdapter):
    """로컬 파일시스템 어댑터.

    aiofiles를 사용하여 비동기 파일 I/O를 제공한다.
    Docker 없이 로컬 디렉토리로 동작한다.
    """

    def __init__(self, config: StorageAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = StorageAdapterConfig(**kwargs)
        self._config = config
        self._base_path = Path(config.base_path or "/tmp/demiurge_testdata")

    async def connect(self) -> None:
        self._base_path.mkdir(parents=True, exist_ok=True)

    async def disconnect(self) -> None:
        pass  # No persistent connection to close

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        key = metadata.get("key", "data.bin")
        full_path = self._base_path / self._config.prefix / key
        full_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(full_path, "wb") as f:
            await f.write(data)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        key = query.get("key")
        if key:
            full_path = self._base_path / self._config.prefix / key
            if full_path.exists():
                async with aiofiles.open(full_path, "rb") as f:
                    yield await f.read()
        else:
            prefix = query.get("prefix", "")
            search_dir = self._base_path / self._config.prefix / prefix
            if search_dir.exists():
                count = 0
                for p in sorted(search_dir.rglob("*")):
                    if p.is_file():
                        if limit and count >= limit:
                            break
                        async with aiofiles.open(p, "rb") as f:
                            yield await f.read()
                        count += 1

    async def health_check(self) -> bool:
        return self._base_path.exists()

    async def write(self, key: str, data: bytes) -> None:
        full_path = self._base_path / self._config.prefix / key
        full_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(full_path, "wb") as f:
            await f.write(data)

    async def read(self, key: str) -> bytes:
        full_path = self._base_path / self._config.prefix / key
        async with aiofiles.open(full_path, "rb") as f:
            return await f.read()

    async def list_keys(self, prefix: str = "") -> list[str]:
        search_dir = self._base_path / self._config.prefix / prefix
        if not search_dir.exists():
            return []
        base = self._base_path / self._config.prefix
        return [str(p.relative_to(base)) for p in sorted(search_dir.rglob("*")) if p.is_file()]

    async def delete(self, key: str) -> None:
        full_path = self._base_path / self._config.prefix / key
        if full_path.exists():
            os.remove(full_path)
