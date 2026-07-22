# DSL App

## Purpose
The DSL app owns the public authoring surface for Structure users. It provides schema declarations, transform
declarations, expression helpers, joins, hooks, and type objects that let developers write IDE-friendly Python that can
later compile into Spark-visible work.

## Dependency Exchanges
The app exposes `Structure`, `field`, scalar and collection types, `Transform`, `transform`, `input`, `output`,
`where`, `before`, `after`, `special`, `Join`, `JoinHint`, `SchemaMode`, and expression helpers through
`structure.core.dsl.api` and the top-level package. It depends on compiler symbolic execution only while recording DSL
effects. Compiler command access lives under `structure.core.compiler.api.Compiler`: use
`Compiler.frontend.analyze()` for structural inspection and `Compiler.frontend.compile()` for selected-platform
compilation.

## Inner Workings
The logic model is split by domain: `schemas` builds declared row types and field metadata, `types` defines Structure
type values, `expr` creates expression trees and row scopes, and `transforms` records inputs, outputs, hooks, expression
functions, and transform invocation state. The app stores authoring intent; compiler and target apps decide what that
intent means operationally.
