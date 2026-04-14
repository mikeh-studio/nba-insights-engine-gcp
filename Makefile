SHELL := /bin/zsh
.EXPORT_ALL_VARIABLES:

ifneq ("$(wildcard .env)","")
include .env
endif

export AIRFLOW_HOME ?= $(CURDIR)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER ?= $(CURDIR)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES ?= False

AIRFLOW_PYTHON := $(firstword $(wildcard $(CURDIR)/.venv-airflow/bin/python3*))
ifeq ($(AIRFLOW_PYTHON),)
AIRFLOW_CMD := airflow
else
AIRFLOW_CMD := $(AIRFLOW_PYTHON) -m airflow
endif

.PHONY: airflow-init airflow-create-user airflow-webserver airflow-scheduler airflow-trigger airflow-list airflow-parse

airflow-init:
	$(AIRFLOW_CMD) db init

airflow-create-user:
	$(AIRFLOW_CMD) users create \
		--username admin \
		--firstname Local \
		--lastname Admin \
		--role Admin \
		--email admin@example.com \
		--password admin

airflow-webserver:
	$(AIRFLOW_CMD) webserver --port 8080

airflow-scheduler:
	$(AIRFLOW_CMD) scheduler

airflow-trigger:
	$(AIRFLOW_CMD) dags trigger nba_analytics_pipeline

airflow-list:
	$(AIRFLOW_CMD) dags list

airflow-parse:
	$(AIRFLOW_CMD) dags list-import-errors
