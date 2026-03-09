"""SFTPAdapter — paramiko 기반 FileTransfer 어댑터"""

from __future__ import annotations

import asyncio
import io
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
            kwargs.setdefault("port", 22)
            config = FileTransferAdapterConfig(**kwargs)
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
        # 상위 디렉토리 재귀 생성
        await self._ensure_remote_dir(os.path.dirname(remote_path))

        # paramiko SFTPFile을 스레드에서 열고 쓰기
        def _write_file() -> None:
            with self._sftp.open(remote_path, "wb") as f:
                f.write(data)

        await asyncio.to_thread(_write_file)

    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        key = query.get("key")
        if key:
            def _read_file() -> bytes:
                with self._sftp.open(key, "rb") as f:
                    return f.read()

            data = await asyncio.to_thread(_read_file)
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

    async def _ensure_remote_dir(self, path: str) -> None:
        """원격 디렉토리를 재귀적으로 생성한다."""
        if not path or path == "/" or path == ".":
            return

        def _mkdir_p() -> None:
            dirs_to_create: list[str] = []
            current = path
            while current and current not in ("/", "."):
                try:
                    self._sftp.stat(current)
                    break  # 존재하면 중단
                except (FileNotFoundError, IOError):
                    dirs_to_create.append(current)
                    current = os.path.dirname(current)
            for d in reversed(dirs_to_create):
                try:
                    self._sftp.mkdir(d)
                except IOError:
                    pass  # 이미 존재하거나 상위 마운트 — 무시

        await asyncio.to_thread(_mkdir_p)
