# Root build entrypoint for the Poetry workspace.
#
# The project keeps one IDE-visible virtualenv at the repository root. If app/lib
# subprojects grow their own Makefiles later, they inherit this environment.

PROJECT_DIR := $(CURDIR)
POETRY ?= poetry
PYTHON ?= python
BACKEND ?= all

SOURCE_ROOTS := src
EXAMPLE_ROOTS := examples
TEST_ROOTS := tests
PYTHON_ROOTS := $(SOURCE_ROOTS) $(EXAMPLE_ROOTS) $(TEST_ROOTS)
TYPE_ROOTS := src examples tests

.PHONY: all help install update format lint type test golden differential metamorphic concepts rigidity check build compose-env integration clean

all: check build

help:
	@echo "Targets:"
	@echo "  make install    Install root Poetry environment in .venv"
	@echo "  make format     Format Python sources and tests"
	@echo "  make lint       Run import, formatting, and flake8 checks"
	@echo "  make type       Run mypy"
	@echo "  make test       Run pytest"
	@echo "  make golden     Run generated-output golden tests"
	@echo "  make differential Run differential behavior tests"
	@echo "  make metamorphic Run metamorphic behavior tests"
	@echo "  make concepts   Run public concept coverage tests"
	@echo "  make rigidity   Run behavior-rigidity tests"
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
	$(POETRY) run black $(SOURCE_ROOTS) $(EXAMPLE_ROOTS)
	$(POETRY) run black $(TEST_ROOTS)

lint: install
	$(POETRY) run isort $(PYTHON_ROOTS)
	$(POETRY) run flake8 $(PYTHON_ROOTS)

type: install
	$(POETRY) run mypy $(TYPE_ROOTS)

test: install
	$(POETRY) run pytest

golden: install
	$(POETRY) run pytest tests/golden

differential: install
	$(POETRY) run pytest tests/differential

metamorphic: install
	$(POETRY) run pytest tests/metamorphic

concepts: install
	$(POETRY) run pytest tests/concepts

rigidity: install
	$(POETRY) run pytest tests/golden tests/differential tests/metamorphic tests/concepts tests/app/test_public_api_snapshot.py tests/specifications/compatibility

check: lint type test rigidity

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
