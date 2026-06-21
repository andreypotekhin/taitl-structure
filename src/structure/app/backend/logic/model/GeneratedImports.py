from dataclasses import dataclass


@dataclass(frozen=True)
class GeneratedImports:
    functions_alias: str = "F"
    types_alias: str = "T"
    data_frame_name: str = "DataFrame"
    spark_session_name: str = "SparkSession"
    column_name: str = "Column"
    schema_helpers: tuple[str, ...] = ("assert_schema", "project_schema")

    def as_dict(self) -> dict[str, str | tuple[str, ...]]:
        return {
            "column_name": self.column_name,
            "data_frame_name": self.data_frame_name,
            "functions_alias": self.functions_alias,
            "schema_helpers": self.schema_helpers,
            "spark_session_name": self.spark_session_name,
            "types_alias": self.types_alias,
        }
