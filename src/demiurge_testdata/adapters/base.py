"""BaseAdapter + 5개 Category Base ABC — 22개 어댑터의 3단계 계층 기반"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from typing import Any, Self


class BaseAdapter(ABC):
    """모든 어댑터의 기본 인터페이스.

    Pipeline은 BaseAdapter만 알면 된다.
    push(bytes) — HandlerChain이 이미 직렬화+압축을 완료했으므로 bytes만 수신한다.
    """

    @abstractmethod
    async def connect(self) -> None:
        """인프라에 연결"""

    @abstractmethod
    async def disconnect(self) -> None:
        """연결 해제 및 리소스 정리"""

    @abstractmethod
    async def push(self, data: bytes, metadata: dict[str, Any]) -> None:
        """직렬화+압축된 데이터를 대상 인프라에 전송"""

    @abstractmethod
    async def fetch(self, query: dict[str, Any], limit: int | None = None) -> AsyncIterator[bytes]:
        """대상 인프라에서 데이터를 조회"""
        yield b""  # pragma: no cover — abstract

    @abstractmethod
    async def health_check(self) -> bool:
        """인프라 연결 상태 확인"""

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.disconnect()


# ── Category Base Classes ──


class BaseRDBMSAdapter(BaseAdapter):
    """RDBMS 카테고리 공통: SQL 실행, 테이블 생성, 벌크 INSERT"""

    @abstractmethod
    async def execute_sql(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """임의 SQL 실행"""

    @abstractmethod
    async def create_table(self, name: str, columns: dict[str, str]) -> None:
        """DDL 생성 및 실행"""

    @abstractmethod
    async def bulk_insert(self, table: str, records: list[dict[str, Any]]) -> int:
        """대량 INSERT 최적화. 삽입된 행 수를 반환한다."""


class BaseNoSQLAdapter(BaseAdapter):
    """NoSQL 카테고리 공통: 문서 삽입, 쿼리"""

    @abstractmethod
    async def insert_documents(self, collection: str, documents: list[dict[str, Any]]) -> int:
        """문서 다건 삽입. 삽입된 건수를 반환한다."""

    @abstractmethod
    async def query_documents(
        self,
        collection: str,
        filter: dict[str, Any] | None = None,
        projection: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """문서 조회"""


class BaseStreamAdapter(BaseAdapter):
    """Streaming 카테고리 공통: 이벤트 발행, 구독"""

    @abstractmethod
    async def publish(self, topic: str, message: bytes) -> None:
        """단건 이벤트 발행"""

    @abstractmethod
    async def publish_batch(self, topic: str, messages: list[bytes]) -> None:
        """다건 이벤트 일괄 발행"""

    @abstractmethod
    async def subscribe(self, topic: str, callback: Any) -> None:
        """이벤트 구독"""


class BaseStorageAdapter(BaseAdapter):
    """Storage 카테고리 공통: 객체/파일 CRUD"""

    @abstractmethod
    async def write(self, key: str, data: bytes) -> None:
        """객체/파일 쓰기"""

    @abstractmethod
    async def read(self, key: str) -> bytes:
        """객체/파일 읽기"""

    @abstractmethod
    async def list_keys(self, prefix: str = "") -> list[str]:
        """키/경로 목록 조회"""

    @abstractmethod
    async def delete(self, key: str) -> None:
        """객체/파일 삭제"""


class BaseFileTransferAdapter(BaseAdapter):
    """FileTransfer 카테고리 공통: 파일 업로드, 다운로드"""

    @abstractmethod
    async def upload(self, local_path: str, remote_path: str) -> None:
        """파일 업로드"""

    @abstractmethod
    async def download(self, remote_path: str, local_path: str) -> None:
        """파일 다운로드"""

    @abstractmethod
    async def list_files(self, remote_dir: str) -> list[str]:
        """원격 파일 목록 조회"""
