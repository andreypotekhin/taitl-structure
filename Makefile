# Root build entrypoint for the Poetry workspace.
#
# The project keeps one IDE-visible virtualenv at the repository root. If app/lib
# subprojects grow their own Makefiles later, they inherit this environment.

PROJECT_DIR := $(CURDIR)
POETRY ?= poetry
PYTHON ?= python
BACKEND ?= all

SOURCE_ROOTS := src
TEST_ROOTS := tests
PYTHON_ROOTS := $(SOURCE_ROOTS) $(TEST_ROOTS)

.PHONY: all help install update format lint type test check build compose-env integration clean

all: check build

help:
	@echo "Targets:"
	@echo "  make install    Install root Poetry environment in .venv"
	@echo "  make format     Format Python sources and tests"
	@echo "  make lint       Run import, formatting, and flake8 checks"
	@echo "  make type       Run mypy"
	@echo "  make test       Run pytest"
	@echo "  make check      Run lint, type, tests"
	@echo "  make build      Run checks and build the package"
	@echo "  make integration Run live Docker Compose integration tests"
	@echo "  make build INTEGRATION=1 Run checks, package build, then integration tests"
	@echo "  make clean      Remove local build and tool caches"

install:
	$(POETRY) install

update:
	$(POETRY) update

format: install
	$(POETRY) run isort $(PYTHON_ROOTS)
	$(POETRY) run black $(SOURCE_ROOTS)
	$(POETRY) run black $(TEST_ROOTS)

lint: install
	$(POETRY) run isort $(PYTHON_ROOTS)
	$(POETRY) run black $(SOURCE_ROOTS)
	$(POETRY) run black $(TEST_ROOTS)
	$(POETRY) run flake8 $(PYTHON_ROOTS)

type: install
	$(POETRY) run mypy $(PYTHON_ROOTS)

test: install
	$(POETRY) run pytest

check: lint type test

build: check
	$(POETRY) build
ifeq ($(INTEGRATION),1)
	$(PYTHON) scripts/run_integration.py --backend $(BACKEND)
endif

compose-env:
	$(PYTHON) scripts/ensure_compose_env.py

integration: compose-env
	$(PYTHON) scripts/run_integration.py --backend $(BACKEND)

clean:
	$(PYTHON) -c "import pathlib, shutil; [shutil.rmtree(pathlib.Path(p), ignore_errors=True) for p in '.venv .mypy_cache .pytest_cache dist build'.split()]; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').glob('*.egg-info')]"
