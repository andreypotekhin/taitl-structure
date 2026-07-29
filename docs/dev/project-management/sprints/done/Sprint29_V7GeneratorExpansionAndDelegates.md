# Sprint 29: V7 Generator Expansion and Focused Delegates

Status: complete. The focused generator delegates and the typed struct-generator family shipped with specification,
recipe, rendering, online/generated parity, traceability, diagnostics, catalog, and live PySpark 3.5/4.0 evidence.

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

## Evidence

- Focused generator specifications and runtime-contract checks passed with 29 tests:
  `PYTHONPATH=.:src:tests poetry run pytest -q tests/specifications/v7-api-ledger/test_v7_explode_struct.py tests/specifications/v6-api-ledger/test_v6_posexplode_struct.py tests/user_stories/10_generated_code/shared_pyspark_semantic_contract/test_online_recipe_runtime.py -k 'explode_struct or posexplode_struct or inline_struct' --maxfail=3`.
- Live integration evidence in the consolidated v7 plan records PySpark 3.5 passing with 5 tests and 3 skips, and
  PySpark 4.0 passing with 8 tests for `tests/integration/pyspark/v7/test_struct_generators.py`.

## Governing Documents

`docs/dev/design/V7PySparkGeneratorExpansion.md`, `docs/dev/specifications/V7GeneratorExpansion.md`, and
`docs/dev/planning/P07282601.V7-pyspark-transform-coverage-and-streaming-adoption.plan.md`
