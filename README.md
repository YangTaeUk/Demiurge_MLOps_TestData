# Demiurge MLOps TestData

인프라 한계를 시험하는 범용 테스트 데이터 프레임워크.

32종 데이터셋 제너레이터 + 22종 어댑터 + 10종 포맷 + 6종 압축을 조합하여
MLOps 파이프라인 테스트 데이터를 생성·변환·적재한다.

## 설치

```bash
# 기본 설치
pip install demiurge-testdata

# 전체 의존성
pip install "demiurge-testdata[all]"

# 개별 선택
pip install "demiurge-testdata[rdbms,streaming,handlers]"
```

### 개발 환경

```bash
git clone https://github.com/your-org/demiurge-testdata.git
cd demiurge-testdata
uv sync --all-extras
```

## 빠른 시작

### CLI

```bash
# 등록된 컴포넌트 확인
python -m demiurge_testdata list

# 제너레이터만 보기
python -m demiurge_testdata list generators

# YAML 설정으로 파이프라인 실행
python -m demiurge_testdata run --config configs/pipelines/home_credit_postgresql.yaml

# REST API 서버 시작
python -m demiurge_testdata serve --port 8000
```

### Python API

```python
import asyncio
from demiurge_testdata.core.registry import generator_registry, format_registry
from demiurge_testdata.generators.relational import home_credit  # 레지스트리 등록
from demiurge_testdata.handlers.formats import json_handler       # 레지스트리 등록
from demiurge_testdata.handlers.chain import HandlerChain

async def main():
    # 제너레이터로 데이터 생성
    gen = generator_registry.create("home_credit", config=None)
    records = await gen.batch(1000)

    # 포맷 핸들러로 직렬화
    fmt = format_registry.create("json")
    chain = HandlerChain(format_handler=fmt)
    encoded = await chain.encode(records)

    print(f"Generated {len(records)} records, {len(encoded)} bytes")

asyncio.run(main())
```

### REST API

```bash
# 헬스 체크
curl http://localhost:8000/health

# 제너레이터 목록
curl http://localhost:8000/generators

# 특정 제너레이터 조회
curl http://localhost:8000/generators/home_credit

# 파이프라인 실행
curl -X POST http://localhost:8000/pipeline/run \
  -H "Content-Type: application/json" \
  -d '{"generator": "home_credit", "adapter": "postgresql", "batch_size": 1000}'
```

## 아키텍처

```
Generator → HandlerChain(Format + Compression) → Adapter
```

- **Generator**: 32종 데이터셋 스키마 기반 테스트 데이터 생성 (Faker)
- **HandlerChain**: 포맷 변환 (JSON, CSV, Parquet 등) + 압축 (gzip, lz4 등)
- **Adapter**: 22종 대상 시스템 적재 (PostgreSQL, MongoDB, Kafka, S3 등)

### 제너레이터 (32종)

| 카테고리 | 데이터셋 |
|---------|---------|
| **Relational** | home_credit, northwind, chinook, olist, euro_soccer, hm, fraud_trans, ga_store |
| **Event** | ieee_fraud, cc_fraud, twitter_sentiment, clickstream, store_sales, bitcoin, network_traffic |
| **Document** | instacart, amazon_reviews, yelp, airbnb, tmdb, foodcom |
| **IoT** | weather, electric_power, appliances_energy, bosch, smart_mfg |
| **Geospatial** | nyc_taxi, geolife, dataco |
| **Text** | stackoverflow, github_metadata, enron_email |

### 어댑터 (22종)

| 카테고리 | 어댑터 |
|---------|--------|
| **RDBMS** | postgresql, mysql, mariadb, mssql, oracle, sqlite, cockroachdb, bigquery |
| **NoSQL** | mongodb, elasticsearch, redis, cassandra |
| **Streaming** | kafka, rabbitmq, mqtt, pulsar, nats |
| **Storage** | s3, local_fs, hdfs |
| **FileTransfer** | ftp, sftp |

### 포맷 핸들러 (10종)

json, jsonl, csv, yaml, xml, parquet, avro, arrow, orc, msgpack

### 압축 핸들러 (6종)

gzip, lz4, snappy, zstd, brotli, lzma

## 설정

YAML 파이프라인 설정 파일:

```yaml
name: home_credit_to_postgresql
mode: batch
generator:
  type: home_credit
  batch_size: 10000
handler:
  format: json
  compression: gzip
adapter:
  type: postgresql
  host: localhost
  port: 5434
  extra:
    user: testdata
    password: testdata_dev
    database: testdata
```

32종 사전 정의 설정은 `configs/pipelines/` 디렉토리에서 확인.

## 개발

### 테스트

```bash
# 단위 테스트 (Docker 불필요)
uv run pytest tests/ -q

# 통합 테스트 (Docker 필요)
docker compose up -d
uv run pytest tests/ -m integration -q

# 벤치마크
uv run pytest tests/ -m benchmark -s

# 전체 테스트
uv run pytest tests/ -m "" -q
```

### 린트

```bash
uv run ruff check src/ tests/
uv run ruff format src/ tests/
```

### 프로젝트 구조

```
src/demiurge_testdata/
├── __init__.py
├── __main__.py              # CLI 진입점
├── api/
│   ├── grpc/server.py       # gRPC 서버
│   └── rest/app.py          # FastAPI REST 서버
├── adapters/                # 22종 어댑터
│   ├── base.py              # ABC 계층
│   ├── rdbms/               # PostgreSQL, MySQL, ...
│   ├── nosql/               # MongoDB, Elasticsearch, ...
│   ├── streaming/           # Kafka, RabbitMQ, ...
│   ├── storage/             # S3, LocalFS, HDFS
│   └── filetransfer/        # FTP, SFTP
├── core/
│   ├── config.py            # YAML → Pydantic 설정
│   ├── enums.py             # FormatType, CompressionType, ...
│   ├── exceptions.py        # 공통 예외
│   ├── pipeline.py          # DataPipeline 오케스트레이터
│   └── registry.py          # 4개 글로벌 레지스트리
├── generators/              # 32종 제너레이터
│   ├── base.py              # BaseGenerator ABC
│   ├── relational/          # Home Credit, Northwind, ...
│   ├── event/               # IEEE Fraud, Clickstream, ...
│   ├── document/            # Instacart, Yelp, ...
│   ├── iot/                 # Weather, Bosch, ...
│   ├── geospatial/          # NYC Taxi, GeoLife, ...
│   └── text/                # StackOverflow, Enron, ...
├── handlers/
│   ├── base.py              # Format/Compression ABC
│   ├── chain.py             # HandlerChain 합성기
│   ├── formats/             # 10종 포맷 핸들러
│   └── compression/         # CramjamCompressionHandler
├── schemas/config.py        # 어댑터별 Pydantic 설정
├── storage/backend.py       # StorageBackend ABC
└── protos/                  # gRPC protobuf 정의
```

## 의존성

| 카테고리 | 패키지 |
|---------|--------|
| **Core** | pydantic, pyyaml |
| **Handlers** | orjson, cramjam, msgspec |
| **Formats** | pyarrow, fastavro, lxml |
| **RDBMS** | asyncpg, aiomysql, aioodbc, oracledb, aiosqlite |
| **NoSQL** | motor, elasticsearch, redis, cassandra-driver |
| **Streaming** | faststream, aiomqtt, pulsar-client |
| **Storage** | fsspec, s3fs, universal-pathlib |
| **API** | fastapi, uvicorn, grpcio, protobuf |

## 라이선스

MIT
