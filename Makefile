.PHONY: install lint format test run-api train eda clean

install:
	pip install -e ".[dev]"

lint:
	ruff check src tests

format:
	ruff format src tests
	ruff check --fix src tests

test:
	pytest

run-api:
	uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

train:
	python -m src.train

eda:
	jupyter lab notebooks/

clean:
	rm -rf __pycache__ .pytest_cache .ruff_cache mlruns
	find . -name "*.pyc" -delete
