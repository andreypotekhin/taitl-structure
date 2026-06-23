# Symbolic Execution App

## Purpose
The symbolic execution app captures side effects produced while Structure executes transform methods symbolically.
It lets normal-looking DSL calls such as `where(...)` and `join_one(...)` describe a Spark-visible plan without running
Spark work.

## Dependency Exchanges
The app receives `Expression` filters and `JoinPlan` records from DSL helper calls during compilation, stores them in
the active `CompileContext`, and gives the compiler frontend a step-local list of captured predicates and joins. DSL
expression scopes depend on this app, while target and runtime code only see the resulting IR.

## Inner Workings
`CompileContext` is a context manager backed by a `ContextVar`, so nested or concurrent compilation can find the
current step without global mutable state. `current_context()` returns that active context to DSL helpers, and the
context accumulates filters and joins until the compiler frontend exits the symbolic step.
