.PHONY: install dev lint format test test-integration test-all clean \
       download download-essential seed seed-quick setup-data \
       seed-test seed-test-rdbms seed-test-streaming \
       stream stream-loop filedrop filedrop-loop

install:
	uv pip install -e .

dev:
	uv pip install -e ".[dev,handlers,formats]"

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

format:
	ruff check --fix src/ tests/
	ruff format src/ tests/

test:
	pytest tests/unit/

test-integration:
	pytest -m integration

test-all:
	pytest -m ""

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache .ruff_cache htmlcov
	find . -type d -name __pycache__ -exec rm -rf {} +

# ── 데이터 수집·시딩 ──

download:
	python -m demiurge_testdata download --skip-errors

download-essential:
	python -m demiurge_testdata download --essential --skip-errors

seed:
	python -m demiurge_testdata seed --skip-errors

seed-quick:
	python -m demiurge_testdata seed --essential --limit 10000 --skip-errors

setup-data:
	python -m demiurge_testdata setup --essential --limit 10000

# ── DL 어댑터 통합 테스트 데이터 ──

seed-test:
	python -m demiurge_testdata seed-test --drop

seed-test-rdbms:
	python -m demiurge_testdata seed-test postgresql mysql mariadb --drop

seed-test-streaming:
	python -m demiurge_testdata seed-test kafka rabbitmq nats mqtt --drop

# ── 스트리밍·파일드롭 시뮬레이션 ──

stream:
	python -m demiurge_testdata stream --essential --limit 10000 --skip-errors

stream-loop:
	python -m demiurge_testdata stream --essential --loop --interval 0.5 --skip-errors

filedrop:
	python -m demiurge_testdata filedrop --essential --limit 10000 --format csv --skip-errors

filedrop-loop:
	python -m demiurge_testdata filedrop --essential --loop --interval 30 --format csv --skip-errors
