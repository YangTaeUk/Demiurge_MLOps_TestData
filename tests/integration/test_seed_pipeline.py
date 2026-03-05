"""Phase 8 Integration Test — 전체 시딩 파이프라인 테스트

PostgreSQL(5434) + MongoDB(27017) + Kafka(9092) 대상으로
SeedPipeline의 seed_rdbms, seed_nosql, seed_streaming을 실제 인프라에서 검증한다.

Usage:
    python -m pytest tests/integration/test_seed_pipeline.py -v -m integration
    python tests/integration/test_seed_pipeline.py  # 직접 실행
"""

from __future__ import annotations

import asyncio
import contextlib
import csv
import importlib
import sys
import traceback
from pathlib import Path
from typing import Any

# ── Adapter Registration ──

adapter_modules = [
    "demiurge_testdata.adapters.rdbms.postgresql",
    "demiurge_testdata.adapters.nosql.mongodb",
    "demiurge_testdata.adapters.streaming.kafka",
    "demiurge_testdata.adapters.storage.s3",
]
for mod in adapter_modules:
    with contextlib.suppress(ImportError):
        importlib.import_module(mod)

# Format handler registration
format_modules = [
    "demiurge_testdata.handlers.formats.json_handler",
    "demiurge_testdata.handlers.formats.csv_handler",
]
for mod in format_modules:
    with contextlib.suppress(ImportError):
        importlib.import_module(mod)

from demiurge_testdata.core.registry import adapter_registry, format_registry
from demiurge_testdata.core.seed import SeedPipeline, infer_columns, load_csv_records
from demiurge_testdata.handlers.chain import HandlerChain

DATA_DIR = Path("data/raw")

# ── Infrastructure Config ──

PG_CONFIG = {
    "host": "localhost",
    "port": 5434,
    "user": "testdata",
    "password": "testdata_dev",
    "database": "testdata",
}

MONGO_CONFIG = {
    "host": "localhost",
    "port": 27017,
    "username": "testdata",
    "password": "testdata_dev",
    "database": "testdata",
}

KAFKA_CONFIG = {
    "host": "localhost",
    "port": 9092,
}


results: dict[str, dict[str, Any]] = {}


async def test_csv_loading() -> None:
    """TEST 1: CSV 로딩 + 스키마 추론"""
    print("=" * 60)
    print("[TEST 1] CSV Loading + Schema Inference")
    print("=" * 60)

    datasets = [
        ("chinook", "orders.csv"),
        ("tmdb", "tmdb_5000_movies.csv"),
        ("cc_fraud", "creditcard.csv"),
        ("weather", "temperature.csv"),
    ]

    for name, primary in datasets:
        path = DATA_DIR / name / primary
        records = load_csv_records(path)
        cols = infer_columns(records)
        print(f"  [{name}] {len(records)} rows, {len(cols)} columns")
        col_summary = ", ".join(f"{k}:{v}" for k, v in list(cols.items())[:4])
        print(f"    schema: {col_summary}...")
        results[f"csv_{name}"] = {"rows": len(records), "cols": len(cols), "ok": True}

    print()


async def test_seed_postgresql() -> None:
    """TEST 2: RDBMS 시딩 → PostgreSQL"""
    print("=" * 60)
    print("[TEST 2] Seed RDBMS → PostgreSQL (port 5434)")
    print("=" * 60)

    records = load_csv_records(DATA_DIR / "chinook" / "orders.csv")
    print(f"  Loaded {len(records)} chinook orders")

    pipeline = SeedPipeline(
        adapter_type="postgresql",
        adapter_config=PG_CONFIG,
    )

    try:
        count = await pipeline.seed_rdbms(
            table="test_chinook_orders",
            records=records,
            drop_existing=True,
        )
        print(f"  PASS: Seeded {count} records to test_chinook_orders")
        results["pg_seed_chinook"] = {"count": count, "ok": count == len(records)}

        # Verify by querying back
        adapter = adapter_registry.create("postgresql", **PG_CONFIG)
        async with adapter:
            rows = await adapter.execute_sql(
                "SELECT COUNT(*) as cnt FROM test_chinook_orders"
            )
            db_count = rows[0]["cnt"]
            print(f"  PASS: Verified {db_count} rows in DB")
            results["pg_verify_chinook"] = {
                "db_count": db_count,
                "ok": db_count == len(records),
            }

            # Sample data
            sample = await adapter.execute_sql(
                "SELECT * FROM test_chinook_orders LIMIT 2"
            )
            for r in sample:
                print(f"    sample: {dict(r)}")

    except Exception as e:
        print(f"  FAIL: PostgreSQL seed failed: {e}")
        traceback.print_exc()
        results["pg_seed_chinook"] = {"ok": False, "error": str(e)}

    print()


async def test_seed_postgresql_weather() -> None:
    """TEST 3: RDBMS 시딩 → PostgreSQL (weather)"""
    print("=" * 60)
    print("[TEST 3] Seed RDBMS → PostgreSQL (weather)")
    print("=" * 60)

    records = load_csv_records(DATA_DIR / "weather" / "temperature.csv")
    print(f"  Loaded {len(records)} weather records")

    pipeline = SeedPipeline(
        adapter_type="postgresql",
        adapter_config=PG_CONFIG,
    )

    try:
        count = await pipeline.seed_rdbms(
            table="test_weather_temp",
            records=records,
            drop_existing=True,
        )
        print(f"  PASS: Seeded {count} records to test_weather_temp")

        # Verify
        adapter = adapter_registry.create("postgresql", **PG_CONFIG)
        async with adapter:
            rows = await adapter.execute_sql(
                "SELECT COUNT(*) as cnt FROM test_weather_temp"
            )
            db_count = rows[0]["cnt"]
            print(f"  PASS: Verified {db_count} rows")
            results["pg_seed_weather"] = {"count": count, "ok": db_count == len(records)}

    except Exception as e:
        print(f"  FAIL: Weather seed failed: {e}")
        traceback.print_exc()
        results["pg_seed_weather"] = {"ok": False, "error": str(e)}

    print()


async def test_seed_mongodb() -> None:
    """TEST 4: NoSQL 시딩 → MongoDB"""
    print("=" * 60)
    print("[TEST 4] Seed NoSQL → MongoDB (port 27017)")
    print("=" * 60)

    records = load_csv_records(DATA_DIR / "tmdb" / "tmdb_5000_movies.csv")
    print(f"  Loaded {len(records)} TMDB movies")

    pipeline = SeedPipeline(
        adapter_type="mongodb",
        adapter_config=MONGO_CONFIG,
    )

    try:
        # Drop existing collection
        from motor.motor_asyncio import AsyncIOMotorClient

        client = AsyncIOMotorClient("mongodb://testdata:testdata_dev@localhost:27017")
        await client.testdata.drop_collection("test_tmdb_movies")
        client.close()

        count = await pipeline.seed_nosql(
            collection="test_tmdb_movies",
            documents=records,
        )
        print(f"  PASS: Seeded {count} documents to test_tmdb_movies")
        results["mongo_seed_tmdb"] = {"count": count, "ok": count == len(records)}

        # Verify by querying back
        adapter = adapter_registry.create("mongodb", **MONGO_CONFIG)
        async with adapter:
            total = await adapter.query_documents("test_tmdb_movies")
            sample = await adapter.query_documents("test_tmdb_movies", limit=2)
            print(f"  PASS: Verified {len(total)} documents in MongoDB")
            for doc in sample:
                print(f"    sample: title={doc.get('title')}, budget={doc.get('budget')}")
            results["mongo_verify_tmdb"] = {
                "doc_count": len(total),
                "ok": len(total) == len(records),
            }

    except Exception as e:
        print(f"  FAIL: MongoDB seed failed: {e}")
        traceback.print_exc()
        results["mongo_seed_tmdb"] = {"ok": False, "error": str(e)}

    print()


async def test_seed_kafka() -> None:
    """TEST 5: Streaming 시딩 → Kafka"""
    print("=" * 60)
    print("[TEST 5] Seed Streaming → Kafka (port 9092)")
    print("=" * 60)

    records = load_csv_records(DATA_DIR / "cc_fraud" / "creditcard.csv", limit=50)
    print(f"  Loaded {len(records)} cc_fraud records")

    try:
        fmt_handler = format_registry.create("json")
        handler_chain = HandlerChain(format_handler=fmt_handler, compression_handler=None)

        pipeline = SeedPipeline(
            adapter_type="kafka",
            adapter_config=KAFKA_CONFIG,
            handler_chain=handler_chain,
        )

        count = await pipeline.seed_streaming(
            topic="testdata.cc_fraud_test",
            records=records,
            batch_size=25,
        )
        print(f"  PASS: Published {count} messages to testdata.cc_fraud_test")
        results["kafka_seed_ccfraud"] = {"count": count, "ok": count == len(records)}

    except Exception as e:
        print(f"  FAIL: Kafka seed failed: {e}")
        traceback.print_exc()
        results["kafka_seed_ccfraud"] = {"ok": False, "error": str(e)}

    print()


async def test_schema_inference_accuracy() -> None:
    """TEST 6: 스키마 추론 정확도 검증"""
    print("=" * 60)
    print("[TEST 6] Schema Inference Accuracy")
    print("=" * 60)

    records = load_csv_records(DATA_DIR / "chinook" / "orders.csv")
    cols = infer_columns(records)

    expected = {
        "InvoiceId": "INTEGER",
        "CustomerId": "INTEGER",
        "Total": "DOUBLE PRECISION",
    }

    all_ok = True
    for col, exp_type in expected.items():
        actual = cols.get(col, "MISSING")
        ok = actual == exp_type
        status = "PASS" if ok else "FAIL"
        print(f"  {status}: {col} expected={exp_type}, actual={actual}")
        if not ok:
            all_ok = False

    results["schema_inference"] = {"ok": all_ok}
    print()


async def main() -> None:
    await test_csv_loading()
    await test_schema_inference_accuracy()
    await test_seed_postgresql()
    await test_seed_postgresql_weather()
    await test_seed_mongodb()
    await test_seed_kafka()

    # ── Summary ──
    print("=" * 60)
    print("[SUMMARY]")
    print("=" * 60)
    all_ok = True
    for name, res in results.items():
        status = "PASS" if res.get("ok") else "FAIL"
        detail = {k: v for k, v in res.items() if k != "ok"}
        print(f"  [{status}] {name}: {detail}")
        if not res.get("ok"):
            all_ok = False

    print()
    if all_ok:
        print("  ALL TESTS PASSED")
    else:
        print("  SOME TESTS FAILED")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
