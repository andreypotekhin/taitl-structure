from importlib.metadata import PackageNotFoundError, version

try:
    VERSION = version("structure")
except PackageNotFoundError:
    VERSION = "unknown"
