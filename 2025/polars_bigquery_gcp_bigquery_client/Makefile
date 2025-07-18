SHELL=/bin/bash

venv:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

install:
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop

install-release:
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop --release

pre-commit:
	cargo +nightly fmt --all && cargo clippy --all-features
	.venv/bin/python -m ruff check . --fix --exit-non-zero-on-fix
	.venv/bin/python -m ruff format polars_bigquery_gcp_bigquery_client tests
	.venv/bin/mypy polars_bigquery_gcp_bigquery_client tests

test:
	.venv/bin/python -m pytest tests

run: install
	source .venv/bin/activate && python run.py

run-release: install-release
	source .venv/bin/activate && python run.py

