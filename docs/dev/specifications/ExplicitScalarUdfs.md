# Explicit Scalar Python UDFs

## Purpose

This specification documents Structure's supported opt-in scalar Python UDF boundary. It gives users a deliberate
choice when a row-local calculation cannot be represented by a symbolic expression, without allowing unsupported
Python to become a silent compiler fallback.

## Source Contract

A transform author declares a scalar UDF with `@special(type="udf")` and provides an explicit Structure return type
and nullability contract:

```python
class Normalize(Transform):
    rows = input(SourceRow)
    result = output(ResultRow)

    @special(type="udf", return_type=string(), nullable=False)
    def normalize_code(value):
        return value.strip().upper() if value is not None else "UNKNOWN"

    def publish(self, row: SourceRow) -> ResultRow:
        return ResultRow(code=self.normalize_code(row.code))
```

The example must live in a shipped example package and its documentation must contrast this case with an equivalent
symbolic helper where one exists. The UDF body is ordinary user-authored Python and is opaque to Structure's type
inference beyond its declared result contract.

## Rules

- `return_type` is a concrete PySpark-plugin Structure type.
- `nullable` is a Boolean and is authoritative because Structure cannot inspect arbitrary UDF behavior.
- The function is scalar and row-local. It must not receive a DataFrame, Spark session, context, or iterator.
- The selected PySpark runtime registers/renders the UDF for online and generated execution.
- `warn_on_udfs=true` emits the documented optimizer-opacity warning. Setting `@transform(warn_on_udfs=False)`
  suppresses the warning for that transform but does not turn the UDF into symbolic logic.
- UDFs are unsupported for Spark Connect and must fail capability validation before execution/generation on that
  variant.
- Python UDTFs, Pandas UDFs, RDD APIs, actions, and implicit conversion of unsupported Python to a UDF are outside
  this contract.

## Generated-Code and Ownership Rules

Generated code either delegates to the source transform instance or uses the configured approved embedding path for
the UDF body. It must retain the declared return type/nullability and source owner. The generator must not reconstruct
or guess UDF source from arbitrary runtime values.

## Diagnostics and Traceability

The compiler records a UDF opacity boundary and attaches the existing UDF warning to the declaring transform/step
location. Diagnostics for a missing return type, invalid nullability, unsupported target, or unavailable source-backed
UDF owner must name this specification and suggest a symbolic helper or `@raw` only when appropriate.

## Acceptance

- The shipped example compiles, renders, and executes with identical ordinary-PySpark online/generated rows.
- The warning is present with the default policy and absent only when the explicit configuration disables it.
- Spark Connect rejects the same transform through a target capability diagnostic.
- A normal symbolic helper remains preferred for an equivalent built-in operation; no unsupported Python expression is
  silently lowered to a UDF.
