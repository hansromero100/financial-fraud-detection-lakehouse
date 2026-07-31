.PHONY: help install test lint format clean generate-sample deploy deploy-dev deploy-staging deploy-prod

VENV := .venv
PYTHON := $(VENV)/Scripts/python.exe
PIP := $(VENV)/Scripts/pip.exe
PYTEST := $(PYTHON) -m pytest
BLACK := $(PYTHON) -m black
FLAKE8 := $(PYTHON) -m flake8

help:
	@echo "Available targets:"
	@echo "  install           - Create venv and install dependencies"
	@echo "  test              - Run PyTest suite"
	@echo "  lint              - Run Flake8 linting"
	@echo "  format            - Format code with Black"
	@echo "  check             - Run lint + format check + tests"
	@echo "  generate-sample   - Generate sample PaySim data"
	@echo "  clean             - Remove generated files and venv"
	@echo "  deploy-dev        - Deploy to Databricks (dev)"
	@echo "  deploy-staging   - Deploy to Databricks (staging)"
	@echo "  deploy-prod       - Deploy to Databricks (prod)"

install:
	python -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements-dev.txt
	$(PIP) install -e .

test:
	$(PYTEST) tests/ -v --tb=short

lint:
	$(FLAKE8) src/ tests/ --max-line-length=88 --extend-ignore=E203,W503

format:
	$(BLACK) src/ tests/ --line-length=88

check: lint format test
	@echo "All checks passed."

generate-sample:
	$(PYTHON) data/generate_sample_data.py

clean:
	rm -rf $(VENV)
	rm -rf build/ dist/ *.egg-info
	rm -rf data/sample_paysim.csv
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

deploy-dev:
	databricks bundle deploy --target dev

deploy-staging:
	databricks bundle deploy --target staging

deploy-prod:
	databricks bundle deploy --target prod
