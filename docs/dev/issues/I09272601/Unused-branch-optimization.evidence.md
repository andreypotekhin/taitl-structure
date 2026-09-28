# Unused-branch optimization evidence

Issue: [I09272601](../I09272601.Search-integration-performance.issue.md), still open.
Implementation: [P09272603](../../planning/P09272603.Unused-branch-optimization.plan.md).

## Scope and baseline

Measured on 2026-09-27 after the concurrent checkpoint-work task restored lexical gap selection to union plus one
existence join. That source has **93 authored steps**, rather than the historical 92. No Search source rewrite is
part of unused-step pruning. Keep the checkpoint-work and compiled-artifact-reuse changes fixed when comparing it.

The following are compiler counts, **not runtime measurements**. Each row was compiled with pruning off and on,
without materializing runtime schemas. Backend profiles are `>=3.5,<4.0` and `>=4.0,<4.1`; all other configuration
uses project defaults. The ordinary backends retain enabled intermediate schema validation. Connect uses its existing
boundary-validation default.

| Backend | Stage outputs | Authored | Executable, off | Executable, on |
| --- | --- | ---: | ---: | ---: |
| Spark 3.5 ordinary | enabled | 93 | 93 | 93 |
| Spark 3.5 ordinary | disabled | 93 | 93 | 93 |
| Spark 4.0 ordinary | enabled | 93 | 93 | 93 |
| Spark 4.0 ordinary | disabled | 93 | 93 | 93 |
| Spark Connect 4.0 | enabled | 93 | 93 | 93 |
| Spark Connect 4.0 | disabled | 93 | 93 | 93 |

On ordinary Spark, 92 steps are retained for intermediate validation and the final step supplies the returned output.
On Connect with stage access disabled, 36 steps contain singleton-policy checks, six contain assertions, three own
lifecycle operations, 23 have operations or expressions whose removal safety is not certified, and one supplies the
returned output. Their dependencies retain the other 24 steps. Counts describe each step's first reported retention
reason, not the total number of checks in the graph.

Turning stage access off alone does not remove Search work under these safety facts. Do not disable validation or
classify unknown behavior as safe to manufacture a reduction. The reusable benefit is demonstrated separately by a
two-step pure-branch fixture: its private unused step disappears from both runtime construction and generated code;
the returned step remains. Declared child outputs restore their branches when stage access is enabled.

Reproduce the compiler inventory from the repository root:

```python
from structure.core.compiler.api import Compiler
from examples.search.transforms.searching.search_docs.SearchDocuments import SearchDocuments

for variant in ("ordinary", "spark-connect"):
    for stages in (True, False):
        for pruning in (False, True):
            compiled = Compiler.frontend.compile()(
                SearchDocuments,
                materialize_schemas=False,
                allow_stage_outputs=stages,
                prune_unused_steps=pruning,
                plugin={"pyspark": {"profile": ">=4.0,<4.1", "variant": variant}},
            )
            print(variant, stages, pruning, len(compiled.analysis.steps), len(compiled.lowered.steps))
            if compiled.optimization is not None:
                print(compiled.optimization.explain())
```

For Spark 3.5, select ordinary and `>=3.5,<4.0`. Connect 3.5 does not support this Search checkpoint contract.

## Verification

- Focused neutral/PySpark/configuration suite: 57 passed, including cache separation, nested and inferred outputs,
  overwritten producers, singleton checks, hooks, assertions, unknown operations, and lifecycle retention.
- The broader compiler/plugin endpoint suite passed before the final added regression cases (97 tests).
- Broader configuration/composition/plugin/materialization suite: 264 passed before the final two regression cases.
- Final `make build`: passed (2,011 tests passed, 202 skipped; rigidity suite 78 passed, seven skipped), including
  the cache/contract/growth regressions. Live tests are not included in that local build.
- `git diff --check`: passed.
- Focused live parity module (`-k test_pruning`): Spark 3.5 ordinary, 7 passed in 60.06s; Spark 4.0 ordinary,
  7 passed in 139.11s; Spark Connect 4.0, 7 passed in 25.36s. All runs used the production integration default
  (`STRUCTURE_PRUNE_UNUSED_STEPS` unset, therefore true), and covered both online and generated modes, empty/single/
  duplicate rows, a lazy unused uniqueness assertion, and a streaming DataFrame without an action.

The compiler retains authored analysis, checks source and streaming contracts before pruning, and recalculates
executable query plan growth diagnostics from retained steps. Missing operation producer mappings in composed
payloads conservatively retain all preceding possible producers; unknown declared inputs fail as plugin-contract
errors. This is a safety fallback, not proof that those predecessors are all needed.

## Runtime comparison status

The requested three alternating off/on repetitions, reranking/text/full Search cases, and backend medians are still
pending. The concurrent artifact-reuse task completed Spark 3.5 reranking/text runs but hit its usage limit before the
full backend matrix; those measurements do not compare pruning. No elapsed-time saving, checkpoint saving, or new
heap-safety claim is inferred from the static counts above.
Record preparation, construction (including nested checkpoint work), collection, cleanup, and total separately;
never add checkpoint duration twice.
