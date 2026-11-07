SHELL := /bin/bash

# Load .env (Postgres credentials, AIRFLOW_UID, SLACK_WEBHOOK_URL) and export it.
-include .env
export

VENV := $(CURDIR)/.venv
PY   := $(VENV)/bin/python
DBT  := $(VENV)/bin/dbt

export LAKE_ROOT        ?= $(CURDIR)/data/lake
export WAREHOUSE_PATH   ?= $(CURDIR)/data/warehouse/olist.duckdb
export DBT_PROFILES_DIR ?= $(CURDIR)/dbt/olist

.PHONY: help venv up down reset download seed simulate ingest dbt-deps dbt-debug dbt-build dbt-docs \
        test lint ci-local airflow-up airflow-down sim-up sim-down

help:  ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

venv:  ## Create .venv and install the project with dev tools
	python3 -m venv $(VENV)
	$(PY) -m pip install -U pip
	$(PY) -m pip install -e ".[dev]"

up:  ## Start Postgres
	docker compose up -d postgres

down:  ## Stop every container (data is kept)
	docker compose --profile sim down

reset:  ## DELETE the Postgres volume and the local lake/warehouse
	docker compose --profile sim down -v
	rm -rf data/lake data/warehouse data/ci

download:  ## Download the Olist CSV files from Kaggle into data/raw
	PATH=$(VENV)/bin:$$PATH bash scripts/download_olist.sh

seed:  ## Load the CSV files into Postgres
	$(PY) -m olist.load_seed

simulate:  ## Run the traffic simulator in the foreground (Ctrl+C to stop)
	$(PY) -m olist.simulator --events-per-minute 60

ingest:  ## Batch ingestion Postgres -> Bronze
	$(PY) -m olist.ingest_batch

dbt-deps:  ## Install dbt packages
	cd dbt/olist && $(DBT) deps

dbt-debug:  ## Check the dbt profile and connections
	mkdir -p data/warehouse
	cd dbt/olist && $(DBT) debug

dbt-build:  ## Build and test everything; extra flags via ARGS="--select silver"
	mkdir -p data/warehouse
	cd dbt/olist && $(DBT) build $(ARGS)

dbt-docs:  ## Generate and serve dbt docs on http://localhost:8081
	cd dbt/olist && $(DBT) docs generate && $(DBT) docs serve --port 8081

test:  ## Python unit tests
	$(PY) -m pytest

lint:  ## Ruff lint
	$(VENV)/bin/ruff check .

ci-local:  ## Run what CI runs, on fixture data (does not touch data/lake)
	$(VENV)/bin/ruff check .
	$(PY) -m pytest
	mkdir -p data/ci
	cd dbt/olist && $(DBT) deps --quiet
	for via in batch cdc; do \
		lake=$(CURDIR)/data/ci/lake$$([ $$via = cdc ] && echo -cdc); \
		$(PY) scripts/make_fixture_bronze.py --via $$via && \
		(cd dbt/olist && LAKE_ROOT=$$lake WAREHOUSE_PATH=$(CURDIR)/data/ci/$$via.duckdb $(DBT) build --target ci) \
		|| exit 1; \
	done

airflow-up:  ## Build and start Airflow on http://localhost:8080
	docker compose up -d --build airflow

airflow-down:  ## Stop Airflow
	docker compose stop airflow

sim-up:  ## Start the simulator container (SIM_BAD_RATE=0.2 make sim-up for bad data)
	docker compose --profile sim up -d simulator

sim-down:  ## Stop the simulator container
	docker compose --profile sim stop simulator
