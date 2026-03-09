# Demiurge TestData — DL 어댑터 통합 테스트 데이터 연동 가이드

> **목적**: Demiurge MLOps(DL) 프로젝트의 어댑터 통합 테스트에 필요한 테스트 데이터를
> TestData 프로젝트로부터 제공받기 위한 설정·적재·연동 절차를 안내합니다.

---

## 1. 아키텍처 개요

```
┌─────────────────────────────┐       ┌───────────────────────────────┐
│  Demiurge TestData          │       │  Demiurge MLOps (DL)          │
│                             │       │                               │
│  Generator (공통 스키마)     │       │  Adapter Integration Tests    │
│       ↓                     │       │                               │
│  SeedTestPipeline           │       │  PostgresAdapter.test()       │
│       ↓                     │       │  KafkaAdapter.test()          │
│  14개 인프라에 적재          │─net──→│  S3Adapter.test()             │
│       ↓                     │       │  RestAdapter.test()           │
│  REST API 서빙 (/api/test)  │─HTTP─→│  ...                         │
└─────────────────────────────┘       └───────────────────────────────┘

※ 인프라 서비스는 TestData의 Docker Compose에서 기동
※ 두 프로젝트는 Docker 네트워크(demiurge-testdata-net)로 연결
```

**역할 분담**:
- **TestData**: 테스트 데이터 생성 + 인프라 적재 + REST 서빙
- **DL**: 적재된 데이터를 각 어댑터로 조회·검증

---

## 2. 공통 스키마

모든 어댑터에 동일한 6컬럼 스키마를 사용합니다.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `id` | INTEGER | 순차 정수 (1부터 시작) |
| `name` | VARCHAR(100) | 한글 이름 (UTF-8 검증용) |
| `value` | DOUBLE PRECISION | 0.01 ~ 99999.99 범위 실수 |
| `category` | VARCHAR(50) | A / B / C / D / E 중 택1 |
| `created_at` | TIMESTAMP | 2026-01-01 ~ 2027-01-01 범위 |
| `is_active` | BOOLEAN | true / false |

**DDL 타입 매핑** (RDBMS별 자동 변환):

| 공통 타입 | PostgreSQL | MySQL / MariaDB |
|-----------|-----------|-----------------|
| INTEGER | INTEGER | INT |
| DOUBLE PRECISION | DOUBLE PRECISION | DOUBLE |
| TIMESTAMP | TIMESTAMP | DATETIME |
| BOOLEAN | BOOLEAN | TINYINT(1) |

> `seed=42`로 고정되어 동일 count에 대해 항상 동일한 데이터가 생성됩니다.

---

## 3. 어댑터별 적재 스펙

### 3.1 RDBMS

| 어댑터 | 테이블명 | 건수 | 비고 |
|--------|---------|------|------|
| PostgreSQL | `test_adapter_sample` | 500 | |
| PostgreSQL | `test_large_table` | 10,000 | 대용량 테스트용 |
| MySQL | `test_adapter_sample` | 500 | 마지막 행: `created_at = '0000-00-00 00:00:00'` |
| MariaDB | `test_adapter_sample` | 500 | |

### 3.2 NoSQL

| 어댑터 | 컬렉션/인덱스명 | 건수 |
|--------|----------------|------|
| MongoDB | `test_adapter_sample` | 500 |
| Elasticsearch | `test_adapter_sample` | 500 |
| Redis | `test_adapter_sample` | 100 |

### 3.3 Streaming

| 어댑터 | 토픽/서브젝트명 | 건수 | 비고 |
|--------|----------------|------|------|
| Kafka | `test_adapter_sample` | 100 | |
| RabbitMQ | `test_adapter_sample` | 100 | |
| NATS | `test.adapter.sample` | 100 | dot-delimited |
| MQTT | `test/adapter/sample` | 50 | Windows에서 미지원 (※ 참고) |

> **※ MQTT 참고**: `aiomqtt`는 `loop.add_reader()`/`add_writer()`를 사용하며,
> Windows의 `ProactorEventLoop`에서는 이 API가 지원되지 않습니다.
> MQTT 적재 및 테스트는 **Linux / macOS** 환경에서 실행해야 합니다.

### 3.4 Storage

| 어댑터 | 파일 경로 | 포맷 | 건수 |
|--------|----------|------|------|
| S3 (MinIO) | `csv/sample.csv` | CSV | 500 |
| S3 (MinIO) | `parquet/sample.parquet` | Parquet | 500 |
| S3 (MinIO) | `json/sample.jsonl` | JSONL | 500 |
| Local FS | `sample.csv` | CSV | 500 |
| Local FS | `sample.parquet` | Parquet | 500 |
| Local FS | `sample.json` | JSON | 100 |
| Local FS | `sample.xlsx` | Excel | 100 |

> S3 버킷명: `test-adapter-data`

### 3.5 File Transfer

| 어댑터 | 원격 경로 | 포맷 | 건수 |
|--------|----------|------|------|
| FTP | `/test-data/sample.csv` | CSV | 500 |
| SFTP | `/data/test-data/sample.csv` | CSV | 500 |

### 3.6 REST API

| 항목 | 값 |
|------|------|
| URL | `GET /api/test/data` |
| 인증 | `Authorization: Bearer test-bearer-token` |
| 총 레코드 수 | 200건 (고정) |
| 페이지네이션 | `offset` (기본 0), `limit` (기본 50, 최대 500) |

**응답 예시:**
```json
{
  "total": 200,
  "offset": 0,
  "limit": 50,
  "data": [
    {
      "id": 1,
      "name": "홍길동",
      "value": 12345.67,
      "category": "A",
      "created_at": "2026-03-15T10:22:00",
      "is_active": true
    }
  ]
}
```

---

## 4. 인프라 기동

### 4.1 사전 준비

```bash
cd Demiurge_MLOps_TestData

# .env 파일 확인 (기본 제공됨)
# DL 프로젝트와 포트 충돌 방지를 위해 아래 포트가 변경되어 있음
cat .env
```

**현재 `.env` 설정값:**

| 환경변수 | 변경값 | 변경 사유 |
|---------|--------|----------|
| `NATS_PORT` | 4223 | DL 프로젝트 NATS(4222)와 충돌 방지 |
| `NATS_MON_PORT` | 8223 | NATS 모니터링 포트 |
| `MINIO_API_PORT` | 9002 | DL 프로젝트 MinIO(9000)와 충돌 방지 |
| `MINIO_CONSOLE_PORT` | 9003 | MinIO 콘솔 포트 |
| `REDIS_PORT` | 6380 | DL 프로젝트 Redis(6379)와 충돌 방지 |
| `SFTP_PORT` | 2223 | Docker Desktop 포트 바인딩 충돌 방지 |

### 4.2 서비스 기동

```bash
# 전체 기동 (14개 서비스)
docker compose up -d

# 헬스체크 확인
docker compose ps
```

### 4.3 포트 설정 참조 (실제 운영 포트)

> **중요**: `.env`에서 변경된 포트가 반영된 **실제 포트**입니다.
> 호스트에서 직접 접근 시 아래 포트를 사용하세요.

| 서비스 | 환경변수 | 기본 포트 | 실제 포트 (.env 적용) |
|--------|---------|----------|---------------------|
| PostgreSQL | `PG_PORT` | 5434 | **5434** |
| MySQL | `MYSQL_PORT` | 3306 | **3306** |
| MariaDB | `MARIADB_PORT` | 3307 | **3307** |
| MongoDB | `MONGO_PORT` | 27017 | **27017** |
| Elasticsearch | `ES_PORT` | 9200 | **9200** |
| Redis | `REDIS_PORT` | 6379 | **6380** |
| Kafka | `KAFKA_PORT` | 9092 | **9092** |
| RabbitMQ (AMQP) | `RABBITMQ_PORT` | 5672 | **5672** |
| RabbitMQ (관리 UI) | `RABBITMQ_MGMT_PORT` | 15672 | **15672** |
| NATS | `NATS_PORT` | 4222 | **4223** |
| MQTT | `MQTT_PORT` | 1883 | **1883** |
| MinIO (API) | `MINIO_API_PORT` | 9000 | **9002** |
| MinIO (Console) | `MINIO_CONSOLE_PORT` | 9001 | **9003** |
| FTP | `FTP_PORT` | 21 | **21** |
| SFTP | `SFTP_PORT` | 2222 | **2223** |
| Zookeeper | `ZK_PORT` | 2181 | **2181** |

### 4.4 기본 인증 정보

| 서비스 | 사용자 | 비밀번호 |
|--------|--------|---------|
| PostgreSQL | `testdata` | `testdata_dev` |
| MySQL | `testdata` | `testdata_dev` |
| MariaDB | `testdata` | `testdata_dev` |
| MongoDB | `testdata` | `testdata_dev` |
| RabbitMQ | `testdata` | `testdata_dev` |
| MinIO | `testdata` | `testdata_dev_password` |
| FTP | `testdata` | `testdata_dev` |
| SFTP | `testdata` | `testdata_dev` |
| REST API | — | Bearer `test-bearer-token` |

### 4.5 호스트 주소 주의사항

> **Windows Docker Desktop 환경에서는 `localhost` 대신 `127.0.0.1`을 사용하세요.**
>
> Windows에서 `localhost`가 IPv6(`::1`)로 해석되어 Docker 포트 바인딩과
> 매칭이 안 되는 경우가 있습니다. seed-test CLI는 내부적으로 `127.0.0.1`을
> 사용하므로 별도 설정은 필요 없지만, DL 프로젝트에서 호스트 직접 접근 시
> `127.0.0.1`을 권장합니다.

---

## 5. 테스트 데이터 적재

### 5.1 패키지 설치

```bash
pip install -e ".[all,dev]"
```

### 5.2 전체 적재

```bash
# 14개 어댑터 전체 적재 (기존 데이터 삭제 후 재적재)
make seed-test

# 또는 직접 실행
python -m demiurge_testdata seed-test --drop
```

**예상 결과** (Windows 환경):
```
  Seeding test data → postgresql...
    [postgresql] test_adapter_sample: 500 records
    [postgresql] test_large_table: 10000 records
  Seeding test data → mysql...
    [mysql] test_adapter_sample: 500 records
  Seeding test data → mariadb...
    [mariadb] test_adapter_sample: 500 records
  Seeding test data → mongodb...
    [mongodb] test_adapter_sample: 500 records
  Seeding test data → elasticsearch...
    [elasticsearch] test_adapter_sample: 500 records
  Seeding test data → redis...
    [redis] test_adapter_sample: 100 records
  Seeding test data → kafka...
    [kafka] test_adapter_sample: 100 records
  Seeding test data → rabbitmq...
    [rabbitmq] test_adapter_sample: 100 records
  Seeding test data → nats...
    [nats] test.adapter.sample: 100 records
  Seeding test data → mqtt...
    [mqtt] FAILED: Operation timed out          ← Windows 전용 제한
  Seeding test data → s3...
    [s3] csv/sample.csv: 500 records
    [s3] parquet/sample.parquet: 500 records
    [s3] json/sample.jsonl: 500 records
  Seeding test data → ftp...
    [ftp] /test-data/sample.csv: 500 records
  Seeding test data → sftp...
    [sftp] /data/test-data/sample.csv: 500 records
  Seeding test data → local_fs...
    [local_fs] sample.csv: 500 records
    [local_fs] sample.parquet: 500 records
    [local_fs] sample.json: 100 records
    [local_fs] sample.xlsx: 100 records

  seed-test 완료: 13 targets, 16600 total records
```

> **참고**: 어댑터별 60초 타임아웃이 적용되어 있어, 특정 어댑터가 응답하지 않더라도
> 나머지 어댑터의 적재는 정상 진행됩니다.

### 5.3 선택적 적재

```bash
# RDBMS만
make seed-test-rdbms

# Streaming만
make seed-test-streaming

# 특정 어댑터만
python -m demiurge_testdata seed-test postgresql kafka s3 --drop
```

### 5.4 REST API 서버 기동

REST 어댑터 테스트 시 별도로 서버를 기동해야 합니다.

```bash
python -m demiurge_testdata serve --port 8000

# 동작 확인
curl http://127.0.0.1:8000/health
# {"status":"ok"}

# 테스트 데이터 조회
curl -H "Authorization: Bearer test-bearer-token" \
     "http://127.0.0.1:8000/api/test/data?offset=0&limit=10"
```

> Bearer 토큰은 환경변수 `TEST_DATA_TOKEN`으로 변경 가능합니다.

---

## 6. Docker 네트워크 연동

DL 프로젝트에서 TestData의 인프라에 서비스명으로 직접 접근하려면
Docker 네트워크를 공유해야 합니다.

### 6.1 TestData 네트워크 정보

```yaml
# TestData docker-compose.yml 에서 생성되는 네트워크
networks:
  testdata:
    name: demiurge-testdata-net
    driver: bridge
```

### 6.2 DL 프로젝트 설정

DL 프로젝트의 `docker-compose.yml`에 외부 네트워크를 선언합니다:

```yaml
# DL 프로젝트 docker-compose.yml

services:
  dl-app:
    image: demiurge-mlops:latest
    networks:
      - default
      - testdata-ext
    environment:
      # 서비스명으로 직접 접근 (localhost 대신)
      PG_HOST: postgres
      KAFKA_HOST: kafka
      MONGO_HOST: mongodb
      ES_HOST: elasticsearch
      REDIS_HOST: redis
      MINIO_ENDPOINT: http://minio:9000
      NATS_HOST: nats
      RABBITMQ_HOST: rabbitmq
      FTP_HOST: ftp
      SFTP_HOST: sftp
      # ...

networks:
  testdata-ext:
    external: true
    name: demiurge-testdata-net
```

### 6.3 네트워크 연결 후 접근 방식

| 접근 방식 | 호스트명 | 포트 | 사용 시나리오 |
|----------|---------|------|-------------|
| 호스트에서 직접 | `127.0.0.1` | 호스트 매핑 포트 (.env 적용값) | 로컬 개발, CLI 도구 |
| Docker 네트워크 | 서비스명 (`postgres` 등) | 컨테이너 내부 포트 (5432 등) | 컨테이너 간 통신 |

> **주의**: Docker 네트워크 경유 시 컨테이너 **내부 포트**를 사용합니다.
> 예: PostgreSQL → `postgres:5432` (호스트 매핑 포트 5434가 아님)

### 6.4 내부 포트 참조

| 서비스 | 서비스명 (호스트) | 내부 포트 |
|--------|-----------------|----------|
| PostgreSQL | `postgres` | 5432 |
| MySQL | `mysql` | 3306 |
| MariaDB | `mariadb` | 3306 |
| MongoDB | `mongodb` | 27017 |
| Elasticsearch | `elasticsearch` | 9200 |
| Redis | `redis` | 6379 |
| Kafka | `kafka` | 29092 |
| RabbitMQ | `rabbitmq` | 5672 |
| NATS | `nats` | 4222 |
| MQTT (Mosquitto) | `mosquitto` | 1883 |
| MinIO | `minio` | 9000 |
| FTP | `ftp` | 21 |
| SFTP | `sftp` | 22 |

> **Kafka 내부 포트 주의**: Docker 네트워크 내에서 Kafka에 접근할 때는
> `kafka:29092` (PLAINTEXT_INTERNAL listener)를 사용하세요.
> 호스트에서의 `127.0.0.1:9092`는 외부 listener입니다.

---

## 7. 기동 순서 체크리스트

```
1. □ TestData 인프라 기동        docker compose up -d
2. □ 헬스체크 통과 확인           docker compose ps (모든 서비스 healthy)
3. □ 테스트 데이터 적재           make seed-test
4. □ REST 서버 기동 (필요 시)     python -m demiurge_testdata serve
5. □ DL 프로젝트 네트워크 연결    docker compose up -d (testdata-ext 네트워크 포함)
6. □ DL 어댑터 테스트 실행        pytest tests/integration/
```

---

## 8. 검증 방법

### 8.1 RDBMS 검증

```sql
-- PostgreSQL
psql -h 127.0.0.1 -p 5434 -U testdata -d testdata \
  -c "SELECT count(*) FROM test_adapter_sample;"
-- 기대값: 500

-- MySQL
mysql -h 127.0.0.1 -P 3306 -u testdata -ptestdata_dev testdata \
  -e "SELECT count(*) FROM test_adapter_sample;"
-- 기대값: 500

-- MySQL zero-date 검증
mysql -h 127.0.0.1 -P 3306 -u testdata -ptestdata_dev testdata \
  -e "SELECT * FROM test_adapter_sample WHERE created_at = '0000-00-00 00:00:00';"
-- 기대값: 1건
```

### 8.2 NoSQL 검증

```bash
# MongoDB
mongosh --host 127.0.0.1 --port 27017 -u testdata -p testdata_dev \
  --eval "db.getSiblingDB('testdata').test_adapter_sample.countDocuments()"
# 기대값: 500

# Elasticsearch
curl "http://127.0.0.1:9200/test_adapter_sample/_count"
# 기대값: {"count": 500, ...}

# Redis (포트 변경됨: 6380)
redis-cli -h 127.0.0.1 -p 6380 KEYS "test_adapter_sample:*" | wc -l
# 기대값: 100
```

### 8.3 Storage 검증

```bash
# S3 (MinIO) — 포트 변경됨: 9002
# MinIO Client 사용
mc alias set testdata http://127.0.0.1:9002 testdata testdata_dev_password
mc ls testdata/test-adapter-data/csv/
# 기대값: sample.csv 존재

mc ls testdata/test-adapter-data/parquet/
# 기대값: sample.parquet 존재

mc ls testdata/test-adapter-data/json/
# 기대값: sample.jsonl 존재
```

### 8.4 REST 검증

```bash
# 전체 건수
curl -s -H "Authorization: Bearer test-bearer-token" \
  "http://127.0.0.1:8000/api/test/data?limit=1" | python -m json.tool
# total: 200

# 인증 실패 테스트
curl -s -H "Authorization: Bearer wrong-token" \
  "http://127.0.0.1:8000/api/test/data"
# 401 Unauthorized
```

---

## 9. 트러블슈팅

### 호스트 주소 — `localhost` vs `127.0.0.1`

Windows Docker Desktop 환경에서 `localhost`가 IPv6(`::1`)로 해석되어
Docker 포트 바인딩에 연결이 안 되는 경우가 있습니다.

```
# ❌ 실패할 수 있음
psql -h localhost -p 5434 -U testdata

# ✅ 권장
psql -h 127.0.0.1 -p 5434 -U testdata
```

### 포트 충돌

```bash
# 사용 중인 포트 확인
netstat -ano | findstr :5434

# .env에서 충돌 포트 변경 후 재기동
# 예: PG_PORT=5435
docker compose down && docker compose up -d
```

### MQTT Windows 제한

`aiomqtt`는 `loop.add_reader()`/`add_writer()`를 사용하여 Windows의
`ProactorEventLoop`에서 동작하지 않습니다. MQTT 테스트는 Linux/macOS에서
실행하거나, WSL2 내부에서 실행하세요.

### Kafka 외부 접속 불가

Kafka는 `ADVERTISED_LISTENERS`로 클라이언트에 접속 주소를 반환합니다.
현재 설정은 `PLAINTEXT://127.0.0.1:9092`입니다.

```bash
# 다른 호스트에서 접속해야 하는 경우 .env에서 변경
KAFKA_HOST=192.168.1.100
docker compose up -d kafka
```

### Elasticsearch 메모리 부족

docker-compose.yml에서 `ES_JAVA_OPTS` 기본값은 `-Xms256m -Xmx256m`입니다.
메모리 부족 시 호스트의 `vm.max_map_count`를 확인하세요:

```bash
# Linux
sysctl -w vm.max_map_count=262144

# Windows (WSL2)
wsl -d docker-desktop sysctl -w vm.max_map_count=262144
```

### MySQL zero-date 오류

MySQL 8.0 strict mode에서 `'0000-00-00 00:00:00'` 값을 거부합니다.
TestData의 MySQL 어댑터는 `aiomysql.create_pool()`에 `init_command="SET SESSION sql_mode = ''"`를
설정하여 자동 처리합니다. DL 프로젝트에서 같은 테이블을 조회할 때도
strict mode 비활성화가 필요할 수 있습니다.

```sql
SET SESSION sql_mode = '';
SELECT * FROM test_adapter_sample WHERE created_at = '0000-00-00 00:00:00';
```

### SFTP 디렉토리 권한

SFTP 컨테이너(atmoz/sftp)는 chroot 환경으로, 사용자 홈 아래 `data/`
디렉토리에만 쓰기가 가능합니다. entrypoint에서 `chown 1001:1001`을
실행하여 `data/` 디렉토리의 소유권을 testdata 사용자로 설정합니다.

### 네트워크 연결 실패

```bash
# TestData 네트워크 존재 확인
docker network ls | grep demiurge-testdata-net

# TestData 서비스가 먼저 기동되어 있어야 네트워크가 생성됨
docker compose -f /path/to/testdata/docker-compose.yml up -d
```

---

## 10. 변경 이력

| 일자 | 변경 내용 |
|------|----------|
| 2026-03-09 | 초기 작성 |
| 2026-03-09 | `localhost` → `127.0.0.1` 변경 (Windows IPv6 해석 문제) |
| 2026-03-09 | `.env` 실제 포트 반영 (NATS 4223, MinIO 9002/9003, Redis 6380, SFTP 2223) |
| 2026-03-09 | Kafka ADVERTISED_LISTENERS `127.0.0.1` 변경, 내부 포트 29092 명시 |
| 2026-03-09 | Kafka/NATS 어댑터: FastStream → aiokafka/nats-py 직접 클라이언트 전환 |
| 2026-03-09 | SFTP 디렉토리 권한 자동 설정 (entrypoint chown) |
| 2026-03-09 | MQTT Windows 제한사항 명시 |
| 2026-03-09 | MySQL zero-date `init_command` 처리 안내 추가 |
| 2026-03-09 | seed-test 어댑터별 60초 타임아웃 적용 |
| 2026-03-09 | 실제 실행 결과 (13/14 성공) 예시 추가 |
