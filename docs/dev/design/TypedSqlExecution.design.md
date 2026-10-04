# Typed SQL Execution

## Purpose

Structure supports compiler-visible expressions and relation operations, while `@raw` remains its general escape hatch. Some projects already own reviewed Spark SQL that expresses a complete relation more clearly than a new DSL operation. This design admits that narrow case: callers declare result schemas before compilation, while data-changing SQL produces an explicitly typed command-result relation.

The design targets PySpark `>=3.5,<4.1` in ordinary PySpark and Spark Connect. It follows the shape of PySpark's `SparkSession.sql` workflow, but exposes SQL only as a helper used inside Transform methods. Callers configure Spark, catalogs, Delta Lake, Iceberg, and other providers. Structure does not create sessions, configure catalogs, manage transactions, or interpret provider-specific SQL.

## Public API

The PySpark DSL adds one relation-level `sql(...)` operation for compiled Transform methods. `to` is always required. A Structure `Schema` declares a query relation. `SqlResult` is the empty abstract `Schema` base for SQL-specific result contracts; `SqlCommandResult` is its standard normalized-command subclass, and provider result schemas may also derive from `SqlResult`.

```python
class EnrichOrders(Transform):
    CUSTOMER_TABLE = "catalog.reference.customers"

    orders = input(Order)
    enriched = output(EnrichedOrder)

    def enrich(self, order: Order) -> EnrichedOrder:
        statement = (
            "SELECT o.id, c.name AS customer_name "
            "FROM {orders} AS o JOIN " + self.CUSTOMER_TABLE + " AS c "
            "ON o.customer_id = c.id"
        )
        return sql(statement, relations={"orders": order}, to=EnrichedOrder)
```

Class constants, module constants, and Python string concatenation are valid when they produce deterministic SQL text while the Transform is compiled. Symbolic expressions, DataFrames, and Spark-derived values cannot contribute to SQL text. Structure treats supplied SQL as caller-owned dialect text and delegates interpretation to Spark and the configured provider.

`relations` binds each `{name}` placeholder to either a typed Structure relation/input or a string table name. Structure relation values become backend DataFrame bindings. String values are inserted as caller-supplied SQL relation text, such as a qualified table name; they are not literal values and Structure does not quote or validate them. This combines the former `tables=` convenience with `relations=`. A key cannot be supplied twice, and keys must match placeholders. `args` remains separate for Spark-native named or positional literal parameters such as `:minimum` or `?`; the two argument styles cannot be mixed.

```python
return sql(
    "SELECT o.id FROM {orders} AS o "
    "JOIN {customer_table} AS c ON o.customer_id = c.id",
    relations={
        "orders": order,
        "customer_table": self.CUSTOMER_TABLE,
    },
    to=Order,
)
```

There is no `TableId`, `identifiers=`, `tables=`, or direct `StructureSession.sql(...)` / `StructureSession.command(...)` API. The Transform author declares the result kind through `to`. Query results are typed Structure relations and need no `SqlQueryResult` wrapper.

`SqlResult` inherits from `Schema` and declares no fields; it is an abstract marker and cannot itself be used as a concrete result schema. `SqlCommandResult` is a concrete subclass with an optional nullable `label` and four nullable Long metrics: `num_affected_rows`, `num_updated_rows`, `num_inserted_rows`, and `num_deleted_rows`. A supplied label is copied to the normalized result; when omitted, it is null. Structure uses a metric value when the command DataFrame supplies its matching column and uses null when it does not. Null means unavailable or unreported, not zero. PySpark's [`SparkSession.sql`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html) returns a DataFrame; the columns a mutation returns remain backend-dependent. Provider-specific subclasses may add further fields. A provider with a different result shape may define a separate concrete `SqlResult` subclass and adapter instead of claiming the standard command fields. A backend failure raises its native exception and does not produce a success row.

```python
class MergeMetrics(SqlCommandResult):
    operation_id = pyspark.string(nullable=True)


class UpdateOrders(Transform):
    TARGET_TABLE = "catalog.sales.orders"

    changes = input(OrderChange)
    command_results = output(SqlCommandResult)

    def merge(self, change: OrderChange) -> SqlCommandResult:
        return sql(
            "MERGE INTO {target} AS t USING {changes} AS s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET total = s.total",
            relations={"target": self.TARGET_TABLE, "changes": change},
            to=SqlCommandResult,
        )
```

The `label` argument is optional. Supply it when several commands share an output and callers need an explicit
attribution value; otherwise the normalized `label` field is null.

Step methods may return command-result schemas alongside ordinary row schemas, including in a tuple. Returning a command result retains it; omitting it from the method return discards it after the command runs. A command-result lane or output accepts repeated command-result writes implicitly and unions their rows in execution-plan order. Each result schema must be assignable to the destination schema. Other output and lane types keep the existing single-assignment behavior. Spark DataFrame row order is not guaranteed; callers can use a supplied label to attribute rows and sort when presentation order matters.

The returned `TransformResult` exposes each declared output as a named DataFrame, using the existing `result.output_name` or `result["output_name"]` access. There is no additional `.results` namespace.

The standard v1 contracts are ordinary query `Schema` classes and `SqlCommandResult` (including its subclasses). `SqlResult` itself remains abstract. Provider-specific query or command contracts can derive directly from `SqlResult` and declare their own fields without claiming the standard command fields; they require an adapter that knows how to materialize that contract. Structure does not infer the contract from SQL text or parse provider dialects; `to` is authoritative, and Spark/provider semantics determine whether the statement is valid for that result contract.

## Compilation and Execution

The Transform compiler records statement text, relation-binding names and kinds, literal-argument form, result schema, command label where applicable, source location, and streaming classification in an immutable PySpark recipe. A schema contract yields a typed row scope so later Structure expressions can use its declared fields. SQL internals remain opaque: Structure does not infer query schemas, insert implicit casts, or claim expression-level traceability.

The PySpark runtime delegates SQL execution to native `SparkSession.sql`, using DataFrame bindings for relation values and substituting string table-name values into their `{name}` slots. It does not expose a separate direct session SQL API. SQL text can be composed from Transform or module constants and Python string concatenation before the recipe is captured.

Native DataFrame bindings are preferred. Structure may create uniquely named private temporary views when a backend or an existing plan-boundary mechanism requires them. Such views must not shadow caller names and must remain available for as long as a lazy plan may resolve them; existing relation-boundary lifecycle management is the model for cleanup. This is a fallback, not a requirement for every SQL relation binding.

Runtime comparison against a declared query `Schema` or command-result schema uses the existing validation phases and modes. Intermediate query validation is controlled by `validate_intermediate`; final Transform output validation uses the existing output validation; `schema_only` checks schema shape without collecting rows. These inspections can add latency, so the existing validation configuration can disable the applicable phase. Disabling runtime validation does not remove the compile-time `to` type contract.

SQL queries are not categorically batch-only. Structure allows queries over streaming relations and delegates legality to Spark and the selected provider. Commands follow the same backend capability boundary; Structure does not promise every mutation is legal in every streaming context. Commands are ordered runtime operations, are never retried by Structure, and remain subject to Spark/provider execution and failure semantics. A transport failure can leave a mutation's commit outcome unknown.

## Errors and Limits

Structure diagnostics cover missing `to`, unsupported result contracts, invalid supplied command labels, malformed or unbound relation placeholders, duplicate binding keys, non-string table-name values where a string is required, unsupported dynamic SQL text, and an enabled schema mismatch. An exception raised directly by `SparkSession.sql` gains a Python exception note naming the Transform step and any supplied command label, then is bare-reraised so callers retain its native type and can continue catching `AnalysisException`, `PySparkException`, or provider exceptions. Spark Connect may defer analysis or execution until a later DataFrame action; those errors surface at that action and also retain their native type.

A symbolic Transform method runs while Structure captures the recipe, so Python `try/except` inside that method cannot catch a later Spark analysis or execution error. Callers catch those errors around `session.run(...)` or a later action on a lazy query relation.

`when()` builds a row-wise conditional expression; it does not conditionally execute a SQL command. It can operate on a query relation or typed command-result relation, but it cannot choose whether the command runs.

## Existing SQL Boundaries

This admits full-relation SQL with an explicit result contract inside Transform methods. It does not add raw scalar `expr(...)`, `call_function(...)`, arbitrary SQL fragments in expression positions, generic DataFrame APIs, readers, writers, catalog management, SQL scripting, or standalone session-level SQL execution.
