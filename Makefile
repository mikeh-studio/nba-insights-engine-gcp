SHELL := /bin/zsh
.EXPORT_ALL_VARIABLES:

ifneq ("$(wildcard .env)","")
include .env
endif

export AIRFLOW_HOME ?= $(CURDIR)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER ?= $(CURDIR)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES ?= False

.PHONY: airflow-init airflow-create-user airflow-webserver airflow-scheduler airflow-trigger airflow-list airflow-parse

airflow-init:
	airflow db init

airflow-create-user:
	airflow users create \
		--username admin \
		--firstname Local \
		--lastname Admin \
		--role Admin \
		--email admin@example.com \
		--password admin

airflow-webserver:
	airflow webserver --port 8080

airflow-scheduler:
	airflow scheduler

airflow-trigger:
	airflow dags trigger nba_analytics_pipeline

airflow-list:
	airflow dags list

airflow-parse:
	airflow dags list-import-errors
