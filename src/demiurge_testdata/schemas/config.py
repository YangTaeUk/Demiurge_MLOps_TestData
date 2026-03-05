"""어댑터 카테고리별 설정 스키마"""

from __future__ import annotations

from pydantic import BaseModel, Field


class RDBMSAdapterConfig(BaseModel):
    host: str = "localhost"
    port: int = 5432
    user: str = "testdata"
    password: str = "testdata_dev"
    database: str = "testdata"
    schema_name: str = "public"
    pool_size: int = Field(default=5, ge=1, le=50)
    pool_overflow: int = Field(default=10, ge=0, le=50)


class NoSQLAdapterConfig(BaseModel):
    host: str = "localhost"
    port: int = 27017
    username: str | None = None
    password: str | None = None
    database: str = "testdata"
    collection: str = "default"


class StreamAdapterConfig(BaseModel):
    host: str = "localhost"
    port: int = 9092
    topic: str = "testdata"
    group_id: str = "demiurge-testdata"
    username: str | None = None
    password: str | None = None


class StorageAdapterConfig(BaseModel):
    endpoint: str | None = None
    bucket: str | None = None
    prefix: str = ""
    access_key: str | None = None
    secret_key: str | None = None
    region: str = "us-east-1"
    base_path: str | None = None


class BigQueryAdapterConfig(BaseModel):
    project_id: str = "test-project"
    dataset_id: str = "testdata"
    emulator_host: str = "localhost:9050"
    credentials_path: str | None = None


class NATSAdapterConfig(BaseModel):
    host: str = "localhost"
    port: int = 4222
    monitoring_port: int = 8222
    stream_name: str = "testdata"
    subject: str = "testdata.events"
    username: str | None = None
    password: str | None = None


class FileTransferAdapterConfig(BaseModel):
    host: str = "localhost"
    port: int = 21
    username: str = "testdata"
    password: str | None = None
    private_key_path: str | None = None
    remote_base_path: str = "/data"
    passive_mode: bool = True
