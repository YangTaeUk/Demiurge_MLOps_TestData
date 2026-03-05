.PHONY: install dev lint format test test-all clean

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
