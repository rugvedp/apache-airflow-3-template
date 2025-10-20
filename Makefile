# Makefile for quick Airflow setup via Docker Compose

AIRFLOW_IMAGE ?= apache/airflow:3.1.0
AIRFLOW_UID ?= $(shell id -u)
ENV_FILE := .env

.PHONY: help env init up upd down restart clean shell cli

help:
	@echo "Makefile commands:"
	@echo "  make env        - Create required dirs & .env file"
	@echo "  make up         - Start services in foreground"
	@echo "  make upd        - Start services in detached mode"
	@echo "  make down       - Stop & remove containers"
	@echo "  make restart    - Restart all Airflow containers"
	@echo "  make clean      - Remove containers, images & volumes"
	@echo "  make shell      - Open a bash shell in worker container"
	@echo "  make cli ARGS=\"...\" - Run arbitrary airflow CLI command"

env:
	@echo "Preparing folders and .env file..."
	mkdir -p dags logs plugins config
	@echo "AIRFLOW_UID=$(AIRFLOW_UID)" > $(ENV_FILE)
	@echo "Done: created folders (dags, logs, plugins, config) and $(ENV_FILE)"

up:
	@echo "Starting Airflow services (foreground)..."
	docker compose --env-file $(ENV_FILE) up

upd:
	@echo "Starting Airflow services (detached mode)..."
	docker compose --env-file $(ENV_FILE) up -d

down:
	@echo "Stopping Airflow services..."
	docker compose --env-file $(ENV_FILE) down

restart:
	@echo "Restarting all Airflow containers..."
	docker compose --env-file $(ENV_FILE) down
	docker compose --env-file $(ENV_FILE) up -d
	@echo "✅ Airflow restarted successfully."

clean: down
	@echo "Cleaning up volumes and images..."
	docker compose --env-file $(ENV_FILE) down --volumes --rmi all
	@echo "🧹 Clean complete."

shell:
	@echo "Opening shell in airflow-worker..."
	docker compose --env-file $(ENV_FILE) run --rm airflow-worker bash

cli:
	@echo "Running airflow CLI: $(ARGS)"
	docker compose --env-file $(ENV_FILE) run --rm airflow-worker airflow $(ARGS)
