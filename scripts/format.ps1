$ErrorActionPreference = 'Stop'

poetry run isort src tests
poetry run black src tests
