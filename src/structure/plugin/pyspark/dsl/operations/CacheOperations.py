from collections.abc import Callable
from typing import TypeVar

from structure.plugin.pyspark.dsl.operations.CachePlan import CachePlan
from structure.plugin.pyspark.dsl.operations.MaterializationPlan import PersistPlan
from structure.plugin.pyspark.dsl.operations.OperationPlan import OperationPlan

F = TypeVar("F", bound=Callable)


def cache(storage_level: object) -> Callable[[F], F]:
    def decorate(function: F) -> F:
        operations = tuple(getattr(function, "_structure_reserved_operations", ()))
        setattr(function, "_structure_reserved_operations", (*operations, cache_operation(storage_level)))
        return function

    return decorate


def cache_operation(storage_level: object) -> OperationPlan:
    return OperationPlan.cache_operation(CachePlan(storage_level=_storage_level(storage_level)))


def persist_operation(storage_level: object | None = None) -> OperationPlan:
    return OperationPlan.persist_operation(PersistPlan(storage_level=_storage_level(storage_level)))


def reserved_operations(function: Callable) -> tuple[OperationPlan, ...]:
    return tuple(getattr(function, "_structure_reserved_operations", ()))


def _storage_level(value: object | None) -> tuple[bool, bool, bool, bool, int] | None:
    if value is None or value is True:
        return None
    names = ("useDisk", "useMemory", "useOffHeap", "deserialized", "replication")
    try:
        use_disk, use_memory, use_off_heap, deserialized, replication = (getattr(value, name) for name in names)
    except AttributeError as error:
        raise TypeError("persist(...) requires None or a PySpark StorageLevel") from error
    if not all(isinstance(option, bool) for option in (use_disk, use_memory, use_off_heap, deserialized)) or (
        isinstance(replication, bool) or not isinstance(replication, int) or replication < 1
    ):
        raise TypeError("persist(...) requires a valid PySpark StorageLevel")
    return use_disk, use_memory, use_off_heap, deserialized, replication
