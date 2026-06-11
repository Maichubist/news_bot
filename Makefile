# CI-ready targets. On Windows without make use: scripts\test.ps1
PY ?= python

.PHONY: test install dev validate-sources

install:
	$(PY) -m pip install -r requirements.txt

dev:
	$(PY) -m pip install -r requirements-dev.txt

test:
	$(PY) -m pytest

validate-sources:
	$(PY) scripts/validate_sources.py
