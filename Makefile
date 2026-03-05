.PHONY: install dev lint format test test-all clean \
       download download-essential seed seed-quick setup-data

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
