# Root build entrypoint for the Poetry workspace.
#
# The project keeps one IDE-visible virtualenv at the repository root. If app/lib
# subprojects grow their own Makefiles later, they inherit this environment.

PROJECT_DIR := $(CURDIR)
POETRY ?= poetry
PYTHON ?= python

SOURCE_ROOTS := src
TEST_ROOTS := tests
PYTHON_ROOTS := $(SOURCE_ROOTS) $(TEST_ROOTS)

CODE_STRUCTURE_DIRS := src/app src/lib
DELEGATE_DIRS := $(patsubst %/Makefile,%,$(foreach dir,$(CODE_STRUCTURE_DIRS),$(wildcard $(dir)/Makefile)))

ifeq ($(OS),Windows_NT)
VENV_BIN_DIR := $(PROJECT_DIR)/.venv/Scripts
PATHSEP := ;
else
VENV_BIN_DIR := $(PROJECT_DIR)/.venv/bin
PATHSEP := :
endif

export POETRY_VIRTUALENVS_IN_PROJECT := true

.PHONY: all help install update format lint type test check build clean delegates $(DELEGATE_DIRS)

all: check build

help:
	@echo "Targets:"
	@echo "  make install    Install root Poetry environment in .venv"
	@echo "  make format     Format Python sources and tests"
	@echo "  make lint       Run import, formatting, and flake8 checks"
	@echo "  make type       Run mypy"
	@echo "  make test       Run pytest"
	@echo "  make check      Run lint, type, tests, and app/lib delegates"
	@echo "  make build      Run checks and build the package"
	@echo "  make clean      Remove local build and tool caches"

install:
	$(POETRY) install

update:
	$(POETRY) update

format: install
	$(POETRY) run isort $(PYTHON_ROOTS)
	$(POETRY) run black $(PYTHON_ROOTS)

lint: install
	$(POETRY) run isort --check-only $(PYTHON_ROOTS)
	$(POETRY) run black --check $(PYTHON_ROOTS)
	$(POETRY) run flake8 $(PYTHON_ROOTS)

type: install
	$(POETRY) run mypy $(PYTHON_ROOTS)

test: install
	$(POETRY) run pytest

check: lint type test delegates

build: check
	$(POETRY) build

delegates: $(DELEGATE_DIRS)

$(DELEGATE_DIRS): export VIRTUAL_ENV := $(PROJECT_DIR)/.venv
$(DELEGATE_DIRS): export PATH := $(VENV_BIN_DIR)$(PATHSEP)$(PATH)
$(DELEGATE_DIRS): export POETRY_VIRTUALENVS_CREATE := false
$(DELEGATE_DIRS): install
	$(MAKE) -C $@

clean:
	$(PYTHON) -c "import pathlib, shutil; [shutil.rmtree(pathlib.Path(p), ignore_errors=True) for p in '.venv .mypy_cache .pytest_cache dist build'.split()]; [shutil.rmtree(p, ignore_errors=True) for p in pathlib.Path('.').glob('*.egg-info')]"
