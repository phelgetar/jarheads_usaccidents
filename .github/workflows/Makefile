.PHONY: lint type sec test itest cov all

lint:
	ruff check .
	black --check .

type:
	mypy .

sec:
	bandit -r .
	pip-audit -r requirements.txt || true

test:
	pytest -q --maxfail=1

itest:
	# Example: CI spins MySQL; locally you can run docker compose or Testcontainers.
	PYTEST_ADDOPTS="-m integration" pytest -q || true

cov:
	pytest --cov=usaccidents_app --cov-report=term-missing

all: lint type sec test
