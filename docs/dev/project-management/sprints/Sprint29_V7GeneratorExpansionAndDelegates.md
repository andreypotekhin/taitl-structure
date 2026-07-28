# Sprint 29: V7 Generator Expansion and Focused Delegates

## Sprint Goal

Extract the generator seams from oversized PySpark components and add the typed array-of-struct generator variants.

## In Scope

- Characterize `posexplode_struct(...)` and extract focused operation, scope/result, evaluator, renderer, and traceability delegates where the existing implementation requires them.
- Add non-outer and outer struct generator variants with explicit ordinal, null/empty, schema, and cardinality contracts.
- Keep generators batch-only and preserve public imports and generated output readability.

## Out of Scope

- Scalar-array/map generator naming, streaming generators, raw UDTFs, and dynamic output schemas.

## Acceptance

- Every admitted generator has source, recipe, online/generated, traceability, diagnostic, and live PySpark 3.5/4.0 evidence.
- Generator delegates have direct characterization coverage and no behavior drift for `posexplode_struct(...)`.

## Governing Documents

`docs/dev/design/V7PySparkGeneratorExpansion.md`, `docs/dev/specifications/V7GeneratorExpansion.md`, and
`docs/dev/planning/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
