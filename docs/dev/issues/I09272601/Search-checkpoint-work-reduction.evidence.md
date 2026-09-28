# Search checkpoint work reduction evidence

This record captures the first controlled validation of P09272602. The code identity was `9b6712cc0ff8f176bfd4237c0a94612cea111619`
plus the uncommitted changes in the shared worktree. The Spark 3.5 Compose service used the repository's existing
driver and fixture settings, `STRUCTURE_COMPILED_ARTIFACT_REUSE=module`, `STRUCTURE_PLAN_BOUNDARIES=auto`, and
`STRUCTURE_SEARCH_STAGE_OUTPUTS=0`. The focused case was
`test_document_search_reranks_bm25_candidates_for_multiple_queries`; both online and generated execution were run.

The timing-only command was:

    docker compose --env-file infra/compose/.env -f infra/compose/docker-compose.yaml -p structure-integration run --rm \
      -e STRUCTURE_PROFILE_QUERY_PLANS=timing -e STRUCTURE_PROFILE_COMPILATION=1 \
      -e STRUCTURE_COMPILED_ARTIFACT_REUSE=module -e STRUCTURE_PLAN_BOUNDARIES=auto \
      -e STRUCTURE_SEARCH_STAGE_OUTPUTS=0 -e STRUCTURE_INTEGRATION_TIMEOUT=600 \
      -e 'INTEGRATION_PYTEST_ARGS=-k test_document_search_reranks_bm25_candidates_for_multiple_queries -vv -s' \
      structure-integration-pyspark35 bash /workspace/infra/compose/images/pyspark/run-integration.sh pyspark35

The first timing-only attempt lost the Spark JVM during the first lexical validation checkpoint. The profiler reported
checkpoint 1 as `execution_mode=online`, `step=fused.validate_lexical_candidates`, `outcome=failure`, and 18.211
seconds. The retry on the healthy service passed. It reported the following nested timings:

    online construction: 27.80s
    online checkpoints: 16.502s, 5.495s, 0.222s
    generated construction: 25.60s
    generated checkpoints: 14.734s, 5.044s, 0.219s
    online/generated collection: 0.41s / 0.27s
    result: 1 passed in 110.77s

The generated checkpoint labels were mapped from the compiled artifact after the retry instrumentation change; the
online map was `#1=fused.validate_lexical_candidates`, `#2=fused.validate_vector_candidates`, and
`#3=fused.materialize_candidates`. Checkpoint elapsed times are nested inside construction and are not added again.
Timing mode made no `explain`, `collect`, `count`, schema inspection, or query-plan fetch.

The unprofiled control used the same fixed settings with `STRUCTURE_PROFILE_QUERY_PLANS=off`. It passed with online
construction 29.57 seconds, generated construction 33.22 seconds, online/generated collection 0.39/0.33 seconds,
and total 97.22 seconds. Rankings and online/generated parity assertions passed. These are single observations, not
medians or a closure claim.

The gap-selection rewrite is therefore semantically viable on this focused batch case and has a compiled-plan guard:
the final selection has one existence join and no left joins. The shared batch retrieval rewrite was not promoted:
the current DSL cannot preserve the existing stored and streamed stage views while eliminating their two enrichment
steps, and mixed batch/stream execution has not passed its compatibility gate. Earlier materialization remains a
measurement follow-up. Broader backend repetitions, streaming fixtures, text-fixture coverage, and full-module
comparison remain open under I09272601.
