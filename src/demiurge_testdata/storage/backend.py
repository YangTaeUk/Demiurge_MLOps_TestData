"""StorageBackend ABC — 위치 투명성을 제공하는 스토리지 추상화 레이어"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from demiurge_testdata.core.registry import adapter_registry


class StorageBackend(ABC):
    """스토리지 백엔드 ABC.

    위치 투명성(location transparency)을 제공하여
    어댑터 구현에 독립적인 스토리지 접근을 가능하게 한다.
    """

    @abstractmethod
    async def save(self, key: str, data: bytes, metadata: dict[str, Any] | None = None) -> None:
        """데이터를 저장한다."""

    @abstractmethod
    async def load(self, key: str) -> bytes:
        """데이터를 로드한다."""

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """키 존재 여부를 확인한다."""

    @abstractmethod
    async def delete(self, key: str) -> None:
        """데이터를 삭제한다."""

    @abstractmethod
    async def list_keys(self, prefix: str = "") -> list[str]:
        """키 목록을 조회한다."""


class AdapterStorageBackend(StorageBackend):
    """어댑터 기반 스토리지 백엔드.

    BaseStorageAdapter를 래핑하여 StorageBackend 인터페이스를 제공한다.
    """

    def __init__(self, adapter_key: str, **kwargs: Any):
        self._adapter_key = adapter_key
        self._kwargs = kwargs
        self._adapter = None

    async def _get_adapter(self):
        if self._adapter is None:
            self._adapter = adapter_registry.create(self._adapter_key, **self._kwargs)
            await self._adapter.connect()
        return self._adapter

    async def save(self, key: str, data: bytes, metadata: dict[str, Any] | None = None) -> None:
        adapter = await self._get_adapter()
        await adapter.write(key, data)

    async def load(self, key: str) -> bytes:
        adapter = await self._get_adapter()
        return await adapter.read(key)

    async def exists(self, key: str) -> bool:
        adapter = await self._get_adapter()
        keys = await adapter.list_keys(key)
        return len(keys) > 0

    async def delete(self, key: str) -> None:
        adapter = await self._get_adapter()
        await adapter.delete(key)

    async def list_keys(self, prefix: str = "") -> list[str]:
        adapter = await self._get_adapter()
        return await adapter.list_keys(prefix)

    async def close(self) -> None:
        if self._adapter:
            await self._adapter.disconnect()
            self._adapter = None
