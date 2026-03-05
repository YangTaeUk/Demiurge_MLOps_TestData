"""S3Adapter — fsspec + s3fs 기반 Storage 어댑터"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import s3fs

from demiurge_testdata.adapters.base import BaseStorageAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import StorageAdapterConfig


@adapter_registry.register("s3")
class S3Adapter(BaseStorageAdapter):
    """S3/MinIO 호환 스토리지 어댑터.

    fsspec + s3fs를 사용하여 S3 호환 API에 연결한다.
    push()는 metadata의 key에 지정된 경로에 bytes를 저장한다.
    """

    def __init__(self, config: StorageAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = StorageAdapterConfig(**kwargs)
        self._config = config
        self._fs: s3fs.S3FileSystem | None = None

    async def connect(self) -> None:
        client_kwargs = {}
        if self._config.endpoint:
            client_kwargs["endpoint_url"] = self._config.endpoint

        self._fs = s3fs.S3FileSystem(
            key=self._config.access_key,
            secret=self._config.secret_key,
            client_kwargs=client_kwargs,
        )

        # Ensure bucket exists
        bucket = self._config.bucket
        if bucket:
            exists = await asyncio.to_thread(self._fs.exists, bucket)
            if not exists:
                await asyncio.to_thread(self._fs.mkdir, bucket)

    async def disconnect(self) -> None:
        self._fs = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        key = metadata.get("key", "data.bin")
        bucket = metadata.get("bucket", self._config.bucket or "testdata")
        full_path = f"{bucket}/{self._config.prefix}{key}"

        await asyncio.to_thread(self._fs.pipe_file, full_path, data)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        key = query.get("key")
        bucket = query.get("bucket", self._config.bucket or "testdata")

        if key:
            full_path = f"{bucket}/{self._config.prefix}{key}"
            data = await asyncio.to_thread(self._fs.cat_file, full_path)
            yield data
        else:
            prefix = query.get("prefix", "")
            full_prefix = f"{bucket}/{self._config.prefix}{prefix}"
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
            await asyncio.to_thread(self._fs.ls, "")
            return True
        except Exception:
            return False

    async def write(self, key: str, data: bytes) -> None:
        bucket = self._config.bucket or "testdata"
        full_path = f"{bucket}/{self._config.prefix}{key}"
        await asyncio.to_thread(self._fs.pipe_file, full_path, data)

    async def read(self, key: str) -> bytes:
        bucket = self._config.bucket or "testdata"
        full_path = f"{bucket}/{self._config.prefix}{key}"
        return await asyncio.to_thread(self._fs.cat_file, full_path)

    async def list_keys(self, prefix: str = "") -> list[str]:
        bucket = self._config.bucket or "testdata"
        full_prefix = f"{bucket}/{self._config.prefix}{prefix}"
        files = await asyncio.to_thread(self._fs.ls, full_prefix, detail=False)
        # Strip bucket prefix for clean keys
        bucket_prefix = f"{bucket}/"
        return [f.removeprefix(bucket_prefix) for f in files]

    async def delete(self, key: str) -> None:
        bucket = self._config.bucket or "testdata"
        full_path = f"{bucket}/{self._config.prefix}{key}"
        await asyncio.to_thread(self._fs.rm, full_path)
