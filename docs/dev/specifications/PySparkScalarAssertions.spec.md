# PySpark Scalar Assertions

## Purpose

Structure provides compiler-visible scalar guards for PySpark's `assert_true` and `raise_error` functions. They are for
validating rows reached by an ordinary Spark plan; callers use relation assertions such as `require_all(...)` when the
intent is to guard a whole relation through an aggregate check.

## Public API

`assert_true(condition, *, message=None)` requires a Boolean Structure expression and accepts an optional Python string
literal message. `raise_error(message)` requires a Python string literal. Both return non-null Boolean expressions so
they can appear in `where(...)` and other Boolean-expression positions. There are no `assert_false`, `assert_equal`, or
other composed aliases: write `assert_true(~predicate, ...)` or `assert_true(left == right, ...)`.

## Execution

Generated and online execution lower `assert_true(condition, message)` to
`F.assert_true(condition, message).isNull()`. Without a message, they omit the second native argument. They lower
`raise_error(message)` to `F.raise_error(message).isNull()`. Native PySpark returns null when an assertion succeeds;
Structure's `.isNull()` adaptation yields a Boolean true without changing the native failure. A false or null
`assert_true` condition raises when Spark evaluates the expression. `raise_error` always raises when evaluated.

The helpers are lazy, introduce no Python action or UDF, and use the PySpark 3.5--4.0 ordinary and Spark Connect
baseline. They are row-local streaming expressions; a failure is owned by the caller's current micro-batch or query.

## Validation

Tests must cover type validation, literal-message validation, generated rendering, online evaluator dispatch, public
exports, and streaming compatibility. Runtime integration evidence must verify successful, false, null, and
unconditional-failure behavior for both baseline PySpark versions when the integration matrix is run.
