# DSL App

## Purpose
The DSL app owns the public authoring surface for Structure users. It provides schema declarations, transform
declarations, expression helpers, joins, hooks, and type objects that let developers write IDE-friendly Python that can
later compile into Spark-visible work.

## Dependency Exchanges
The app exposes `Structure`, `field`, scalar and collection types, `Transform`, `transform`, `input`, `output`,
`where`, `before`, `after`, `expr_fn`, `Join`, `JoinHint`, `SchemaMode`, and expression helpers through
`structure.app.dsl.api` and the top-level package. It depends on compiler symbolic execution only while recording DSL
effects, and it re-exports `compile_transform` for compatibility with existing public usage.

## Inner Workings
The logic model is split by domain: `schemas` builds declared row types and field metadata, `types` defines Structure
type values, `expr` creates expression trees and row scopes, and `transforms` records inputs, outputs, hooks, expression
functions, and transform invocation state. The app stores authoring intent; compiler and target apps decide what that
intent means operationally.
