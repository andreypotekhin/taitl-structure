"""Schema contracts used by SQL operations."""

from structure.dsl import Schema
from structure.plugin.pyspark.dsl.field import long, string


class SqlResult(Schema):
    """Base class for SQL-specific result schemas."""


class SqlCommandResult(SqlResult):
    """Normalized result row returned by a successful SQL command."""

    __structure_sql_command_result__ = True
    label = string(nullable=True)
    num_affected_rows = long(nullable=True)
    num_updated_rows = long(nullable=True)
    num_inserted_rows = long(nullable=True)
    num_deleted_rows = long(nullable=True)
