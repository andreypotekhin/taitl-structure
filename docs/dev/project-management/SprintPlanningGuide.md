# Sprint Planning Guide

## Sprint Length

Use one- or two-week iterations. The early compiler work benefits from short feedback cycles and demonstrable vertical slices.

## Sprint Planning Inputs

Each sprint should include:

- Sprint goal.
- User-facing outcome.
- Implementation tasks.
- Relevant specification items.
- Acceptance criteria.
- Demo script.
- Risks.
- Explicit non-goals.

## Estimation Guidance

Prefer small tasks that produce visible compiler progress:

- source fixture added
- IR node implemented
- generated code snapshot added
- Spark execution test added
- structured error implemented

Avoid broad tasks such as “implement compiler.” Split them by source construct and generated PySpark output.

## Spike Guidance

Sprint 00 must include short, written spike outcomes before implementation work proceeds to Sprint 01. Each spike should produce:

- the smallest executable proof;
- the design decision or remaining risk;
- links to follow-up backlog items when the proof changes scope;
- any fixture or test worth keeping.

Do not let a spike become implementation by stealth. It is complete when it proves or disproves the design assumption clearly enough to plan the vertical slice.

## Compiler Performance Tracking

Each sprint that changes discovery, symbolic execution, checks, or codegen should track compile time.

Recommended metrics:

- cold compile time for a small fixture
- warm compile time for a small fixture
- compile time for a synthetic project with 10 transforms
- compile time for a synthetic project with 100 transforms
- generated files per second
- peak memory, if easy to capture

Do not over-optimize early, but do prevent obviously quadratic algorithms from entering core paths.

## Demo Expectations

Every sprint after groundwork should be demoable using commands such as:

```bash
structure check
structure compile
pytest
```

And preferably a short PySpark run:

```python
GeneratedTransform(spark=spark).run(...)
```
