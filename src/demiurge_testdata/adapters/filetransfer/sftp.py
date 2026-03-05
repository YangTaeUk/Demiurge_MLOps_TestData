"""SFTPAdapter — asyncssh/paramiko 기반 FileTransfer 어댑터"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from typing import Any

import paramiko

from demiurge_testdata.adapters.base import BaseFileTransferAdapter
from demiurge_testdata.core.registry import adapter_registry
from demiurge_testdata.schemas.config import FileTransferAdapterConfig


@adapter_registry.register("sftp")
class SFTPAdapter(BaseFileTransferAdapter):
    """SFTP 어댑터.

    paramiko를 사용하여 SSH/SFTP 연결을 제공한다.
    동기 드라이버이므로 asyncio.to_thread()로 비동기 래핑한다.
    """

    def __init__(self, config: FileTransferAdapterConfig | None = None, **kwargs: Any):
        if config is None:
            config = FileTransferAdapterConfig(port=22, **kwargs)
        self._config = config
        self._transport: paramiko.Transport | None = None
        self._sftp: paramiko.SFTPClient | None = None

    async def connect(self) -> None:
        self._transport = paramiko.Transport((self._config.host, self._config.port))
        if self._config.private_key_path:
            pkey = await asyncio.to_thread(
                paramiko.RSAKey.from_private_key_file,
                self._config.private_key_path,
            )
            await asyncio.to_thread(
                self._transport.connect,
                username=self._config.username,
                pkey=pkey,
            )
        else:
            await asyncio.to_thread(
                self._transport.connect,
                username=self._config.username,
                password=self._config.password or "",
            )
        self._sftp = await asyncio.to_thread(paramiko.SFTPClient.from_transport, self._transport)

    async def disconnect(self) -> None:
        if self._sftp:
            await asyncio.to_thread(self._sftp.close)
            self._sftp = None
        if self._transport:
            await asyncio.to_thread(self._transport.close)
            self._transport = None

    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        remote_path = metadata.get(
            "key",
            f"{self._config.remote_base_path}/data.bin",
        )
        # Ensure parent directory exists
        parent = os.path.dirname(remote_path)
        if parent:
            try:
                await asyncio.to_thread(self._sftp.stat, parent)
            except FileNotFoundError:
                await asyncio.to_thread(self._sftp.mkdir, parent)

        with await asyncio.to_thread(self._sftp.open, remote_path, "wb") as f:
            await asyncio.to_thread(f.write, data)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        key = query.get("key")
        if key:
            with await asyncio.to_thread(self._sftp.open, key, "rb") as f:
                data = await asyncio.to_thread(f.read)
                yield data

    async def health_check(self) -> bool:
        if not self._sftp:
            return False
        try:
            await asyncio.to_thread(self._sftp.listdir, self._config.remote_base_path)
            return True
        except Exception:
            return False

    async def upload(self, local_path: str, remote_path: str) -> None:
        await asyncio.to_thread(self._sftp.put, local_path, remote_path)

    async def download(self, remote_path: str, local_path: str) -> None:
        await asyncio.to_thread(self._sftp.get, remote_path, local_path)

    async def list_files(self, remote_dir: str) -> list[str]:
        return await asyncio.to_thread(self._sftp.listdir, remote_dir)
