"""FTPAdapter — aioftp 기반 FileTransfer 어댑터"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import Any

import aioftp

from demiurge_testdata.adapters.base import BaseFileTransferAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import FileTransferAdapterConfig


@adapter_registry.register("ftp")
class FTPAdapter(BaseFileTransferAdapter):
    """FTP 어댑터.

    aioftp를 사용하여 비동기 FTP 연결을 제공한다.
    """

    def __init__(self, config: FileTransferAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = FileTransferAdapterConfig(**kwargs)
        self._config = config
        self._client: aioftp.Client | None = None

    async def connect(self) -> None:
        self._client = aioftp.Client()
        await self._client.connect(
            host=self._config.host,
            port=self._config.port,
        )
        await self._client.login(
            user=self._config.username,
            password=self._config.password or "",
        )
        if self._config.passive_mode:
            self._client.passive_commands = {"stor", "retr", "list", "nlst", "mlsd"}

    async def disconnect(self) -> None:
        if self._client:
            await self._client.quit()
            self._client = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        remote_path = metadata.get(
            "key",
            f"{self._config.remote_base_path}/data.bin",
        )
        # Ensure parent directory exists
        parent = "/".join(remote_path.rsplit("/", 1)[:-1])
        if parent:
            with contextlib.suppress(aioftp.StatusCodeError):
                await self._client.make_directory(parent)

        async with self._client.upload_stream(remote_path) as stream:
            await stream.write(data)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        key = query.get("key")
        if key:
            chunks = []
            async with self._client.download_stream(key) as stream:
                async for block in stream.iter_by_block():
                    chunks.append(block)
            yield b"".join(chunks)

    async def health_check(self) -> bool:
        if not self._client:
            return False
        try:
            await self._client.list(self._config.remote_base_path)
            return True
        except Exception:
            return False

    async def upload(self, local_path: str, remote_path: str) -> None:
        await self._client.upload(local_path, remote_path)

    async def download(self, remote_path: str, local_path: str) -> None:
        await self._client.download(remote_path, local_path)

    async def list_files(self, remote_dir: str) -> list[str]:
        result = []
        async for path, _info in self._client.list(remote_dir):
            result.append(str(path))
        return result
