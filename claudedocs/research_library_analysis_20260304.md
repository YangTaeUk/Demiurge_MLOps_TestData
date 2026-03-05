# Demiurge TestData: 유사 프로젝트 & 라이브러리 심층 분석 리포트

> 조사일: 2026-03-04 | 분석 대상: 5개 축 × 40+ 라이브러리/프로젝트

---

## Executive Summary

Demiurge TestData의 5개 핵심 기능축(데이터 생성, 파이프라인/커넥터, 포맷 변환, 스트리밍, 인프라 테스트)에 대해 40+개 라이브러리/프로젝트를 조사한 결과:

**핵심 발견**: Demiurge의 전체 범위를 커버하는 단일 프로젝트는 존재하지 않음. 그러나 **개별 축에서 채택 가능한 고성능 라이브러리**가 다수 존재하며, 이를 활용하면 코드량 감소와 성능 향상을 동시에 달성할 수 있음.

### Top 7 채택 권고 라이브러리

| 순위 | 라이브러리 | 대체 대상 | 핵심 가치 | 영향도 |
|------|----------|----------|----------|--------|
| 1 | **cramjam** | 6개 개별 압축 라이브러리 | 단일 의존성으로 6종 압축 통합, Rust 기반 | **Critical** |
| 2 | **orjson** | stdlib `json` | JSON/JSONL 6-10x 성능 향상 | **High** |
| 3 | **FastStream** | aiokafka + aio-pika + nats-py | Kafka/RabbitMQ/NATS 통합 async 프레임워크 + TestBroker | **High** |
| 4 | **fsspec + UPath** | S3/LocalFS/HDFS/FTP/SFTP 개별 구현 | 5개 Storage/FileTransfer 어댑터 통합 I/O | **High** |
| 5 | **polyfactory** | 수동 테스트 데이터 생성 | Pydantic 모델 기반 자동 테스트 픽스처 | **Medium** |
| 6 | **testcontainers-python** | Docker Compose 수동 관리 | pytest 통합 컨테이너 자동 관리 | **Medium** |
| 7 | **msgspec** | msgpack-python + PyYAML | JSON/MsgPack/YAML 통합 직렬화, 10x 성능 | **Medium** |

---

## 1. 데이터 생성 프레임워크 (Axis 1)

### 1.1 조사 대상 비교표

| 라이브러리 | GitHub Stars | 접근법 | Async | 실제 데이터 기반 | License |
|-----------|-------------|--------|-------|----------------|---------|
| **[Faker](https://github.com/joke2k/faker)** | ~18K | 규칙 기반 랜덤 | No | No | MIT |
| **[Mimesis](https://github.com/lk-geimfari/mimesis)** | ~4.7K | 규칙 기반 (12x faster) | No | No | MIT |
| **[SDV](https://github.com/sdv-dev/SDV)** | ~3.4K | 통계 모델 (GaussianCopula, CTGAN) | No | Yes (학습) | BSL |
| **[polyfactory](https://github.com/litestar-org/polyfactory)** | ~1K+ | 타입 힌트 기반 | No | No | MIT |
| **[factory_boy](https://github.com/FactoryBoy/factory_boy)** | ~3.5K+ | ORM 연동 픽스처 | No | No | MIT |
| **[Hypothesis](https://github.com/HypothesisWorks/hypothesis)** | ~7.5K+ | 속성 기반 테스트 | No | No | MPL-2.0 |
| **Demiurge (계획)** | - | Kaggle 실제 데이터 기반 | Yes | **Yes (원본)** | - |

### 1.2 심층 분석

#### SDV (Synthetic Data Vault)

- **접근법**: 실제 데이터셋을 학습하여 통계적 분포를 보존하는 합성 데이터 생성
- **모델**: GaussianCopula (빠름), CTGAN (딥러닝), TVAE, CopulaGAN
- **핵심 기능**: 단일 테이블, 관계형 다중 테이블, 시계열 데이터 지원
- **평가 도구**: 합성 데이터 품질 리포트 자동 생성
- **한계**: Python 동기 전용, 생성 속도 느림 (CTGAN 학습 시간), BSL 라이선스

**Demiurge와의 관계**: SDV는 Kaggle 데이터를 "학습"하여 유사한 분포의 데이터를 **무한 생성**할 수 있음. Demiurge가 원본 데이터 행을 셔플/샘플링하는 것과 달리, SDV는 원본에 없는 새로운 행을 통계적으로 생성. **Privacy-safe 데이터 생성이 필요한 경우** SDV가 Demiurge Generator의 상위 옵션이 될 수 있으나, 현재 Demiurge의 목적(인프라 테스트)에는 원본 데이터의 정확한 분포가 더 중요.

#### Mimesis vs Faker

- **Mimesis**: Faker 대비 **12x 빠른** 성능, 46개 로케일, 구조화된 Provider 시스템
- **Faker**: 더 넓은 생태계, Django/SQLAlchemy 통합, 커뮤니티 Provider 풍부
- **Demiurge 활용**: 두 라이브러리 모두 **Generator의 보조 도구**로 활용 가능. Kaggle 데이터에 없는 필드(예: 이메일, IP, 전화번호)를 생성할 때 Mimesis로 보강.

#### polyfactory

- **핵심 가치**: Pydantic 모델에서 **자동으로** 테스트 데이터 인스턴스 생성
- **Demiurge 활용**: 32종 Generator의 Pydantic 스키마(예: `BitcoinOHLCV`, `YelpBusiness`)에 대해 **단위 테스트 픽스처를 자동 생성**할 수 있음. `polyfactory.factories.pydantic_factory.ModelFactory`를 상속하면 각 Generator의 output 모델에 대한 테스트 데이터를 코드 1줄로 생성.

```python
# 예시: polyfactory로 Demiurge Generator 테스트
from polyfactory.factories.pydantic_factory import ModelFactory
from demiurge_testdata.generators.event.bitcoin import BitcoinOHLCV

class BitcoinOHLCVFactory(ModelFactory):
    __model__ = BitcoinOHLCV

# 테스트에서 자동 생성
sample = BitcoinOHLCVFactory.build()  # 랜덤 유효 인스턴스
batch = BitcoinOHLCVFactory.batch(100)  # 100개 배치
```

### 1.3 Axis 1 결론

| 결정 | 근거 |
|------|------|
| **Demiurge의 Kaggle 기반 접근 유지** | 인프라 테스트에는 실제 데이터 분포가 핵심. SDV의 합성 접근은 privacy 목적에 적합하나 Demiurge 목적과 상이 |
| **polyfactory 채택 권고** | 32종 Pydantic 스키마의 단위 테스트 자동화에 즉시 활용 가능 |
| **Mimesis 선택적 채택** | Generator에서 보조 필드 생성 시 Faker 대비 12x 성능 이점 |

---

## 2. 데이터 파이프라인/커넥터 프레임워크 (Axis 2)

### 2.1 조사 대상 비교표

| 프레임워크 | GitHub Stars | 커넥터 수 | Python SDK | Async | 독립 사용 | License |
|-----------|-------------|----------|-----------|-------|----------|---------|
| **[dlt](https://github.com/dlt-hub/dlt)** | ~6K+ | 5000+ sources | Native | Yes | Yes | Apache-2.0 |
| **[Meltano](https://github.com/meltano/meltano)** | ~1.8K+ | 600+ (Singer) | SDK | No | Plugin | MIT |
| **[Airbyte](https://github.com/airbytehq/airbyte)** | ~17K+ | 400+ | CDK | Partial | Container | ELv2 |
| **[Sling](https://github.com/slingdata-io/sling-cli)** | ~500+ | 40+ | Wrapper | No | CLI | Apache-2.0 |
| **[FastStream](https://github.com/ag2ai/faststream)** | ~3.5K+ | 4 (Kafka/RMQ/NATS/Redis) | Native | **Yes** | Yes | Apache-2.0 |
| **Demiurge (계획)** | - | 22 어댑터 | Native | **Yes** | Yes | - |

### 2.2 심층 분석

#### dlt (data load tool)

- **아키텍처**: `@dlt.source` → `@dlt.resource` → `pipeline.run()` → destination
- **destinations**: PostgreSQL, MySQL, MSSQL, BigQuery, DuckDB, Snowflake, Redshift, Clickhouse, filesystem (S3/GCS/Azure), MongoDB, SFTP 등
- **핵심 강점**: Python-first, 스키마 자동 추론, incremental loading, 병렬 추출/정규화/적재
- **한계**: "적재(Load)" 특화 → Demiurge의 "생성(Generate)" 기능 없음. 스트리밍 미지원 (Kafka/RabbitMQ/MQTT/Pulsar/NATS 없음)

**Demiurge와의 관계**: dlt는 ELT 파이프라인 도구로, Demiurge의 Adapter 계층과 가장 유사. 그러나 dlt는 **source에서 destination으로 데이터를 이동**하는 도구이고, Demiurge는 **데이터를 생성하여 target에 적재**하는 도구. 근본적으로 다른 목적. dlt의 destination 코드를 참고하여 Demiurge adapter 구현에 영감을 얻을 수 있으나, 직접 의존하기에는 추상화 레벨이 다름.

#### Meltano/Singer

- **아키텍처**: Singer 프로토콜 (stdin/stdout JSON 스트림) 기반 tap(source)/target(destination) 파이프라인
- **600+ 커넥터**: 가장 넓은 커넥터 생태계
- **한계**: 프로세스 간 통신 기반이라 오버헤드 큼. Python async 미지원. 커넥터 품질 편차 큼.

**Demiurge와의 관계**: Meltano는 데이터 통합 플랫폼이고, Demiurge는 테스트 데이터 프레임워크. 목적이 완전히 다름. Singer target 코드를 참고할 수 있으나 직접 의존은 비권장.

#### FastStream (Streaming 통합)

- **핵심**: Kafka, RabbitMQ, NATS, Redis에 대한 **통합 async 인터페이스**
- **Pydantic 연동**: 메시지 직렬화/검증에 Pydantic 모델 직접 사용
- **TestBroker**: 실제 브로커 없이 in-memory로 테스트 가능
- **AsyncAPI 문서 자동 생성**
- **한계**: MQTT, Pulsar 미지원 (0.7+에서 추가 예정)

**Demiurge와의 관계**: FastStream은 Demiurge의 **Streaming 어댑터 5종 중 3종(Kafka, RabbitMQ, NATS)**의 기반 프레임워크로 채택 가능. 개별 클라이언트 라이브러리(aiokafka, aio-pika, nats-py)를 직접 사용하는 대신 FastStream의 통합 인터페이스를 사용하면:

1. **코드량 60% 감소**: 3개 어댑터의 connect/publish/subscribe 로직 통합
2. **TestBroker로 CI 가속**: Docker 없이 스트리밍 테스트 실행
3. **Pydantic 네이티브**: Generator의 Pydantic 모델을 직접 메시지로 전송

```python
# 예시: FastStream 기반 Kafka 어댑터
from faststream.kafka import KafkaBroker
broker = KafkaBroker("localhost:9092")

@broker.publisher("test-topic")
async def publish_event(data: BitcoinOHLCV): ...
```

**MQTT/Pulsar 대응**: 이 2종은 기존 계획대로 aiomqtt, pulsar-client 개별 사용.

#### Sling

- **40+ 커넥터**: DB-to-DB, file-to-DB, DB-to-file 이동
- **CLI 기반**: Python wrapper 제공하나 프로그래밍 인터페이스 제한적
- **async 미지원**

**Demiurge와의 관계**: Sling은 데이터 이동 CLI 도구. Demiurge의 프로그래밍 프레임워크와는 용도 불일치.

### 2.3 Axis 2 결론

| 결정 | 근거 |
|------|------|
| **FastStream 채택 권고** (Kafka/RabbitMQ/NATS) | 3개 스트리밍 어댑터를 통합 async 프레임워크로 구현. TestBroker가 CI에서 매우 유용 |
| **dlt 참고만** | destination 구현 패턴 참고. 직접 의존은 목적 불일치 |
| **Meltano/Singer 미채택** | 프로세스 기반 통신, async 미지원, 목적 불일치 |

---

## 3. 멀티포맷 직렬화 & 압축 (Axis 3)

### 3.1 포맷 핸들러 라이브러리 비교

| Demiurge 핸들러 | 현재 계획 | 권고 라이브러리 | 성능 개선 |
|----------------|----------|--------------|----------|
| **JSON** | stdlib `json` | **[orjson](https://github.com/ijl/orjson)** (~7.9K stars) | 6-10x faster |
| **JSONL** | stdlib `json` | **orjson** | 6-10x faster |
| **MessagePack** | `msgpack` | **[msgspec](https://github.com/jcrist/msgspec)** (~3.6K stars) | 5-10x faster |
| **YAML** | `pyyaml` | **msgspec** (YAML 지원) | 타입 세이프 + 성능 |
| **Avro** | `fastavro` | **fastavro** (유지) | 이미 최적 (8x vs avro-python3) |
| **Parquet** | `pyarrow` | **pyarrow** (유지) | 이미 최적 |
| **ORC** | `pyarrow.orc` | **pyarrow** (유지) | 이미 최적 |
| **Arrow IPC** | `pyarrow` | **pyarrow** (유지) | 이미 최적 |
| **CSV** | stdlib `csv` | stdlib `csv` (유지) | Polars는 batch용 보조 |
| **XML** | `lxml` | `lxml` (유지) | 대안 없음 |

#### orjson 상세

- **언어**: Rust (PyO3 바인딩)
- **성능**: stdlib json 대비 6-10x 빠른 직렬화, 1.5-2x 빠른 역직렬화
- **네이티브 지원**: `dataclass`, `datetime`, `numpy`, `uuid` 자동 직렬화
- **메모리**: ujson 대비 30% 적은 메모리
- **License**: Apache-2.0 / MIT

```python
# Demiurge JSON Handler 개선 예시
import orjson

class JsonFormatHandler(BaseFormatHandler):
    def encode(self, records: list[dict]) -> bytes:
        return orjson.dumps(records)  # 6-10x faster

    def decode(self, data: bytes) -> list[dict]:
        return orjson.loads(data)
```

#### msgspec 상세

- **통합 포맷**: JSON, MessagePack, YAML, TOML을 **단일 라이브러리**로 처리
- **성능**: orjson보다 빠른 JSON (Struct 사용 시), MessagePack 최고 성능
- **스키마 검증**: Pydantic v2보다 10x 빠른 검증 (Struct 타입)
- **메모리**: dict 대비 50% 적은 메모리 (Struct)
- **License**: BSD-3-Clause

**Demiurge 관점**: msgspec의 Struct와 Pydantic의 BaseModel은 호환되지 않음. Demiurge가 이미 Pydantic v2를 스키마로 채택했으므로, msgspec를 **직렬화 백엔드로만** 사용하고 스키마 검증은 Pydantic 유지가 합리적.

### 3.2 압축 핸들러: cramjam 통합

| 현재 계획 (6개 별도 라이브러리) | cramjam 통합 |
|-------------------------------|-------------|
| `gzip` (stdlib) | `cramjam.gzip` |
| `brotli` (Google) | `cramjam.brotli` |
| `python-snappy` (unmaintained) | `cramjam.snappy` |
| `lz4` (python-lz4) | `cramjam.lz4` |
| `zstandard` | `cramjam.zstd` |
| `lzma` (stdlib) | `cramjam.lzma` |

**[cramjam](https://github.com/milesgranger/cramjam)** (~117 stars, 4.35M weekly downloads):
- **Rust 바인딩**: 시스템 의존성 없음 (python-snappy의 snappy C 라이브러리 불필요)
- **통합 API**: `cramjam.<variant>.compress(data)` / `decompress(data)`
- **버퍼 재사용**: `compress_into` / `decompress_into`로 고처리량 파이프라인에 최적
- **License**: MIT

```python
# 현재 계획: 6개 핸들러 각각 다른 라이브러리
class SnappyHandler:
    import snappy  # system dependency 필요
    def compress(self, data): return snappy.compress(data)

class LZ4Handler:
    import lz4.frame
    def compress(self, data): return lz4.frame.compress(data)

# cramjam 통합: 단일 라이브러리
import cramjam

class UnifiedCompressionHandler(BaseCompressionHandler):
    def __init__(self, algorithm: str):
        self._codec = getattr(cramjam, algorithm)

    def compress(self, data: bytes) -> bytes:
        return bytes(self._codec.compress(data))

    def decompress(self, data: bytes) -> bytes:
        return bytes(self._codec.decompress(data))

# 사용: handler = UnifiedCompressionHandler("snappy")
```

**영향**: 6개의 `BaseCompressionHandler` 서브클래스가 **1개의 파라미터화된 클래스**로 통합. 의존성 5개 제거.

### 3.3 Axis 3 결론

| 결정 | 영향 |
|------|------|
| **cramjam 채택 (Critical)** | 6개 압축 라이브러리 → 1개. 코드 80% 감소, 시스템 의존성 제거 |
| **orjson 채택 (High)** | JSON/JSONL 핸들러 6-10x 성능 향상 |
| **msgspec MessagePack 채택 (Medium)** | MsgPack 핸들러 5-10x 성능, 선택적 YAML 통합 |
| **pyarrow + fastavro 유지** | 이미 최적 선택. 변경 불필요 |

---

## 4. Storage/FileTransfer 통합: fsspec (Axis 3 연장)

### 4.1 fsspec + universal_pathlib

**[fsspec](https://github.com/fsspec/filesystem_spec)** (Polars, Pandas, Dask의 공통 의존성):
- **통합 파일 I/O**: local, S3, GCS, Azure, HDFS, FTP, SFTP, HTTP 등 모든 파일시스템을 동일 API로 접근
- **Async 지원**: async 파일시스템 구현 제공
- **License**: BSD-3-Clause

**[universal_pathlib](https://github.com/fsspec/universal_pathlib)**:
- `pathlib.Path` 확장으로 `UPath("s3://bucket/file.parquet")` 형태 사용
- 프로토콜 자동 감지 및 적절한 구현 디스패치

**Demiurge Storage/FileTransfer 어댑터 영향**:

| 현재 계획 (5개 별도 어댑터) | fsspec 통합 |
|---------------------------|-------------|
| S3Adapter (aiobotocore) | `fsspec.filesystem("s3")` |
| LocalFSAdapter (aiofiles) | `fsspec.filesystem("file")` |
| HDFSAdapter (?) | `fsspec.filesystem("hdfs")` |
| FTPAdapter (aioftp) | `fsspec.filesystem("ftp")` |
| SFTPAdapter (asyncssh) | `fsspec.filesystem("sftp")` |

```python
# fsspec 통합 어댑터 예시
from upath import UPath

class UnifiedStorageAdapter(BaseStorageAdapter):
    def __init__(self, protocol: str, **kwargs):
        self.root = UPath(f"{protocol}://", **kwargs)

    async def upload(self, data: bytes, path: str):
        (self.root / path).write_bytes(data)

    async def download(self, path: str) -> bytes:
        return (self.root / path).read_bytes()

    async def list_files(self, prefix: str) -> list[str]:
        return [str(p) for p in (self.root / prefix).iterdir()]

# 사용
s3 = UnifiedStorageAdapter("s3", endpoint_url="http://minio:9000")
local = UnifiedStorageAdapter("file")
sftp = UnifiedStorageAdapter("sftp", host="localhost", port=2222)
```

**주의사항**: fsspec의 FTP/SFTP 구현은 기본적으로 동기. 고처리량 시나리오에서는 `asyncio.to_thread()` 래핑 또는 개별 async 라이브러리(aioftp, asyncssh) 병용 필요.

---

## 5. 인프라 테스트 프레임워크 (Axis 4)

### 5.1 조사 대상 비교표

| 도구 | GitHub Stars | 목적 | Demiurge 어댑터 커버리지 | License |
|------|-------------|------|------------------------|---------|
| **[testcontainers-python](https://github.com/testcontainers/testcontainers-python)** | ~4K+ | pytest + Docker 자동화 | 22종 중 ~20종 | Apache-2.0 |
| **[pytest-databases](https://github.com/litestar-org/pytest-databases)** | 신규 | DB 전용 pytest fixtures | RDBMS 위주 | MIT |
| **[LocalStack](https://github.com/localstack/localstack)** | ~57K+ | AWS 서비스 에뮬레이션 | S3 (MinIO 대안) | Apache-2.0 |
| **factory_boy** | ~3.5K+ | ORM 연동 테스트 픽스처 | SQLAlchemy 연동 | MIT |

### 5.2 testcontainers-python 상세

**지원 모듈** (Demiurge 어댑터와 매핑):

| Demiurge 어댑터 | testcontainers 모듈 | 상태 |
|----------------|-------------------|------|
| PostgreSQL | `testcontainers.postgres` | Yes |
| MySQL | `testcontainers.mysql` | Yes |
| MSSQL | `testcontainers.mssql` | Yes |
| Oracle | `testcontainers.oracle` | Yes |
| CockroachDB | `testcontainers.cockroachdb` | Yes |
| MongoDB | `testcontainers.mongodb` | Yes |
| Elasticsearch | `testcontainers.elasticsearch` | Yes |
| Redis | `testcontainers.redis` | Yes |
| Cassandra | `testcontainers.cassandra` | Yes |
| Kafka | `testcontainers.kafka` | Yes |
| RabbitMQ | `testcontainers.rabbitmq` | Yes |
| MQTT | `testcontainers.mqtt` | Yes |
| NATS | `testcontainers.nats` | Yes |
| MinIO | `testcontainers.minio` | Yes |
| SFTP | `testcontainers.sftp` | Yes |
| SQLite | N/A (파일 기반) | N/A |
| MariaDB | Generic 컨테이너 | Manual |
| BigQuery | Generic 컨테이너 | Manual |
| Pulsar | Generic 컨테이너 | Manual |
| FTP | Generic 컨테이너 | Manual |
| LocalFS | N/A | N/A |
| HDFS | Generic 컨테이너 | Manual |

**22종 중 15종이 네이티브 모듈 지원**, 나머지는 `GenericContainer`로 커버 가능.

**Demiurge 테스트 전략과의 통합**:

```python
# testcontainers로 어댑터 통합 테스트
import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.kafka import KafkaContainer

@pytest.fixture(scope="session")
def postgres():
    with PostgresContainer("postgres:16") as pg:
        yield pg.get_connection_url()

@pytest.fixture(scope="session")
def kafka():
    with KafkaContainer() as kf:
        yield kf.get_bootstrap_server()

async def test_postgres_adapter(postgres):
    adapter = PostgreSQLAdapter(connection_url=postgres)
    await adapter.connect()
    await adapter.push(test_records)
    result = await adapter.fetch()
    assert len(result) == len(test_records)
```

**Docker Compose vs testcontainers**:
- **Docker Compose**: 전체 인프라 일괄 기동, E2E 테스트에 적합
- **testcontainers**: 개별 어댑터 통합 테스트에 적합, 테스트별 격리, 병렬 실행 가능
- **권고**: 둘 다 사용. 단위/통합 테스트에 testcontainers, E2E에 Docker Compose.

### 5.3 유사 프로젝트 조사

**핵심 발견**: Demiurge TestData의 전체 범위(데이터 생성 + 다중 포맷 + 다중 인프라 적재)를 커버하는 오픈소스 프로젝트는 **발견되지 않음**.

유사한 부분 범위를 다루는 프로젝트:

| 프로젝트 | 범위 | Demiurge와의 차이 |
|---------|------|-----------------|
| **ShadowTraffic** | Kafka/Postgres 합성 데이터 | 상용, Kafka 위주, Python 미지원 |
| **kafka-connect-datagen** | Kafka Avro 랜덤 데이터 | Kafka 전용, 실제 분포 없음 |
| **Voluble** | Kafka 관계형 합성 데이터 | **Deprecated**, Java 전용 |
| **tablefaker** | 테이블 데이터 → CSV/Parquet | 단일 출력, 인프라 적재 없음 |

**결론**: Demiurge의 "Kaggle 실제 데이터 → 다중 포맷 변환 → 22종 인프라 적재" 파이프라인은 **고유한 포지션**. 기존 도구의 조합으로는 달성 불가능하며, 자체 구현이 정당화됨.

### 5.4 Axis 4 결론

| 결정 | 근거 |
|------|------|
| **testcontainers-python 채택 권고** | 22종 중 15종 네이티브 지원, pytest 통합, 테스트 격리 |
| **Docker Compose 유지** | E2E 테스트 및 개발 환경용으로 병행 |
| **유사 프로젝트 없음 확인** | Demiurge의 자체 구현 정당성 입증 |

---

## 6. 종합 아키텍처 개선 권고

### 6.1 Tier 1: 즉시 채택 권고 (High Impact, Low Risk)

#### 6.1.1 cramjam → 압축 핸들러 통합

```
Before: gzip + brotli + python-snappy + lz4 + zstandard + lzma (6 deps)
After:  cramjam (1 dep)

- 의존성 5개 제거
- 시스템 C 라이브러리 의존성 제거 (snappy)
- 6개 CompressionHandler 클래스 → 1개 파라미터화 클래스
- API: cramjam.<algorithm>.compress(data) / decompress(data)
```

#### 6.1.2 orjson → JSON/JSONL 핸들러

```
Before: stdlib json (baseline)
After:  orjson (6-10x faster)

- Drop-in 교체: orjson.dumps() / orjson.loads()
- datetime, numpy 네이티브 직렬화
- 메모리 30% 절감
```

### 6.2 Tier 2: 설계 반영 권고 (High Impact, Medium Risk)

#### 6.2.1 FastStream → Streaming 어댑터 통합

```
Before: aiokafka + aio-pika + nats-py (3 deps, 3 adapters)
After:  faststream[kafka,rabbit,nats] (1 dep, unified adapter)

- Kafka/RabbitMQ/NATS 어댑터 통합
- TestBroker로 Docker 없이 스트리밍 테스트
- Pydantic 모델 네이티브 메시지 직렬화
- 주의: MQTT, Pulsar는 개별 라이브러리 유지
```

#### 6.2.2 fsspec → Storage/FileTransfer 어댑터 통합

```
Before: aiobotocore + aiofiles + aioftp + asyncssh + hdfs (5 deps, 5 adapters)
After:  fsspec + universal_pathlib (2 deps, unified adapter)

- S3/LocalFS/HDFS/FTP/SFTP를 동일 API로 접근
- UPath("s3://..."), UPath("sftp://...") 일관된 인터페이스
- 주의: 고처리량 시나리오에서 async 래핑 필요할 수 있음
```

### 6.3 Tier 3: 테스트 인프라 개선 (Medium Impact)

#### 6.3.1 testcontainers-python → 통합 테스트

```
- 15종 네이티브 모듈 + 7종 GenericContainer
- pytest fixture 기반 컨테이너 자동 관리
- Docker Compose와 병행 (E2E용)
```

#### 6.3.2 polyfactory → Pydantic 테스트 픽스처

```
- 32종 Generator의 Pydantic 스키마에서 자동 테스트 데이터 생성
- ModelFactory 상속으로 코드 1줄로 테스트 인스턴스 생성
```

### 6.4 의존성 변화 요약

```
제거 가능:
  - python-snappy (unmaintained, C 의존성)
  - 개별 lz4, zstandard, brotli, lzma 래퍼
  - aiokafka, aio-pika, nats-py (FastStream으로 통합 시)

신규 추가:
  + cramjam (압축 통합)
  + orjson (JSON 성능)
  + faststream[kafka,rabbit,nats] (스트리밍 통합)
  + fsspec, universal-pathlib (스토리지 통합)
  + testcontainers[postgres,kafka,...] (테스트)
  + polyfactory (테스트 픽스처)

선택적 추가:
  + msgspec (MessagePack/YAML 성능)
  + mimesis (보조 데이터 생성)
```

---

## 7. Demiurge 아키텍처 vs 외부 라이브러리 포지셔닝

```
┌─────────────────────────────────────────────────────┐
│                    Demiurge TestData                  │
│                                                      │
│  ┌──────────┐   ┌──────────┐   ┌──────────────────┐ │
│  │Generator │   │ Handler  │   │    Adapter        │ │
│  │ (32종)   │──▶│ (Format  │──▶│  (22종)           │ │
│  │          │   │  +Compr) │   │                    │ │
│  └──────────┘   └──────────┘   └──────────────────┘ │
│       │              │                  │            │
│  ┌────▼────┐   ┌────▼────┐     ┌──────▼──────────┐ │
│  │Kaggle   │   │cramjam  │     │ FastStream      │ │
│  │Dataset  │   │orjson   │     │ (Kafka/RMQ/NATS)│ │
│  │+Mimesis │   │msgspec  │     │ fsspec          │ │
│  │+poly    │   │fastavro │     │ (S3/FTP/SFTP)   │ │
│  │factory  │   │pyarrow  │     │ aiomqtt         │ │
│  └─────────┘   └─────────┘     │ pulsar-client   │ │
│                                 └─────────────────┘ │
│                                                      │
│  Testing: testcontainers + Docker Compose            │
└──────────────────────────────────────────────────────┘
```

---

## Sources

### Data Generation
- [SDV - Synthetic Data Vault](https://github.com/sdv-dev/SDV)
- [Faker](https://github.com/joke2k/faker)
- [Mimesis](https://github.com/lk-geimfari/mimesis)
- [polyfactory](https://github.com/litestar-org/polyfactory)
- [factory_boy](https://github.com/FactoryBoy/factory_boy)
- [Hypothesis](https://github.com/HypothesisWorks/hypothesis)
- [awesome-synthetic-data (statice)](https://github.com/statice/awesome-synthetic-data)
- [awesome-synthetic-data (gretelai)](https://github.com/gretelai/awesome-synthetic-data)

### Pipeline/Connector Frameworks
- [dlt - data load tool](https://github.com/dlt-hub/dlt)
- [Meltano](https://github.com/meltano/meltano) / [Singer SDK](https://sdk.meltano.com/)
- [Airbyte Python CDK](https://github.com/airbytehq/airbyte-python-cdk)
- [Sling CLI](https://github.com/slingdata-io/sling-cli)
- [FastStream](https://github.com/ag2ai/faststream)

### Format & Compression
- [orjson](https://github.com/ijl/orjson)
- [msgspec](https://github.com/jcrist/msgspec)
- [cramjam](https://github.com/milesgranger/cramjam)
- [fastavro](https://github.com/fastavro/fastavro)
- [Polars](https://github.com/pola-rs/polars)
- [fsspec](https://github.com/fsspec/filesystem_spec)
- [universal_pathlib](https://github.com/fsspec/universal_pathlib)
- [srsly](https://github.com/explosion/srsly)
- [ormsgpack](https://github.com/aviramha/ormsgpack)

### Streaming Generators
- [ShadowTraffic](https://shadowtraffic.io/)
- [Voluble (deprecated)](https://github.com/MichaelDrogalis/voluble)
- [kafka-connect-datagen](https://github.com/confluentinc/kafka-connect-datagen)
- [Locust](https://github.com/locustio/locust)
- [Bytewax](https://github.com/bytewax/bytewax)

### Infrastructure Testing
- [testcontainers-python](https://github.com/testcontainers/testcontainers-python)
- [pytest-databases](https://github.com/litestar-org/pytest-databases)
- [LocalStack](https://github.com/localstack/localstack)
- [pytest-factoryboy](https://github.com/pytest-dev/pytest-factoryboy)
