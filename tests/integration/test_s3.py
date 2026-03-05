"""S3/MinIO 통합 테스트 — Docker 필요"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration

s3fs = pytest.importorskip("s3fs")

from demiurge_testdata.adapters.storage.s3 import S3Adapter
from demiurge_testdata.schemas.config import StorageAdapterConfig


@pytest.fixture
async def s3_adapter():
    config = StorageAdapterConfig(
        endpoint="http://localhost:9000",
        bucket="testdata",
        access_key="testdata",
        secret_key="testdata_dev_password",
    )
    adapter = S3Adapter(config=config)
    await adapter.connect()
    yield adapter
    # Cleanup
    try:
        await adapter.delete("test/hello.txt")
        await adapter.delete("test/data.bin")
    except Exception:
        pass
    await adapter.disconnect()


class TestS3Integration:
    async def test_connect_and_health(self, s3_adapter):
        assert await s3_adapter.health_check() is True

    async def test_write_and_read(self, s3_adapter):
        await s3_adapter.write("test/hello.txt", b"hello minio")
        data = await s3_adapter.read("test/hello.txt")
        assert data == b"hello minio"

    async def test_push_and_fetch(self, s3_adapter):
        payload = b"binary data payload"
        metadata = {
            "key": "test/data.bin",
            "format": "parquet",
            "compression": "zstd",
        }
        await s3_adapter.push(payload, metadata)

        results = []
        async for chunk in s3_adapter.fetch({"key": "test/data.bin"}):
            results.append(chunk)

        assert len(results) == 1
        assert results[0] == payload

    async def test_list_keys(self, s3_adapter):
        await s3_adapter.write("test/a.txt", b"a")
        await s3_adapter.write("test/b.txt", b"b")
        keys = await s3_adapter.list_keys("test/")
        assert len(keys) >= 2

    async def test_delete(self, s3_adapter):
        await s3_adapter.write("test/delete_me.txt", b"bye")
        await s3_adapter.delete("test/delete_me.txt")
        with pytest.raises(FileNotFoundError):
            await s3_adapter.read("test/delete_me.txt")

    async def test_disconnect(self, s3_adapter):
        assert await s3_adapter.health_check() is True
        await s3_adapter.disconnect()
        assert await s3_adapter.health_check() is False
