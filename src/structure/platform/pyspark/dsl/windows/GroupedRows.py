from dataclasses import dataclass


@dataclass(frozen=True)
class GroupedRows:
    def having(self, predicate: object) -> None:
        from structure.platform.pyspark.dsl.operations_api import having

        having(predicate)
