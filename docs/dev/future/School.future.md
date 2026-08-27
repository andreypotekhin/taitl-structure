# School app future

This document records mathematical and educational-computation capabilities that could sensibly be admitted to the
School example later. It is a design backlog, not a promise that every item will be implemented. A future capability
must have a typed schema contract, a focused transform boundary, symbolic and generated-code evidence where relevant,
tests for valid and invalid inputs, documentation, and an explicit decision about numerical precision and caller-owned
execution.

The current School application covers scalar algebra, vector operations, matrix multiplication, matrix-vector products,
small driver-side matrix inversion, finite recurrences, numerical series, and an external Iterable-plugin example.
The items below are not currently admitted.

## Numerical linear algebra

### Distributed matrix decomposition

`InvertMatrices` is intentionally limited to small complete matrices collected on the driver. A future linear-algebra
family could provide distributed determinant, rank, triangular solves, LU or QR decomposition, and condition estimates
over a declared matrix-cell relation. Each operation would need explicit completeness, shape, singularity, numerical
precision, and failure-output semantics. Arbitrary-size matrix materialization must not be implied by a convenient
example transform.

### Sparse matrices and structured tensors

The current matrix model represents cells directly. A future sparse representation could admit nonzero-cell storage,
block matrices, diagonal matrices, and sparse multiplication without expanding absent zeroes. A later tensor slice could
support rank-3 or higher data with explicit dimension ordering. Both need a schema contract that prevents ambiguous
shape inference and makes empty dimensions observable.

### Solvers and eigen problems

Systems of linear equations, least-squares solutions, eigenvalues, and singular-value decompositions would make School a
stronger numerical-computation example. These operations require convergence, tolerance, iteration-limit, and
non-convergence contracts. They should remain separate from the simple join-and-aggregate examples until those contracts
can be expressed clearly in generated code.

## Numerical methods and analysis

### Root finding and optimization

A future `SolveRoots` or `OptimizeFunctions` family could demonstrate bisection, Newton iteration, gradient descent, or
coordinate search over finite iteration rows. The input must carry the function parameters, initial state, tolerance,
and iteration budget. Outputs should distinguish converged, exhausted, invalid-domain, and divergent runs rather than
returning a plausible number with no status.

### Numerical integration and differential equations

School already demonstrates finite series approximations. A future numerical-analysis branch could add trapezoidal and
Simpson integration, ordinary differential-equation steps, and error estimates. It should model one finite timeline per
problem and keep adaptive step selection explicit; arbitrary recursion or hidden driver loops would weaken the example's
Structure story.

### Automatic differentiation

An expression-oriented derivative example could show gradients for a typed algebra expression and use those gradients in
optimization. This is only sensible once the expression representation, supported function set, null and domain
semantics, and generated representation have been designed. It should not be implemented as an opaque Python callback.

## Statistics, probability, and simulation

### Descriptive statistics and distributions

A future statistics module could calculate quantiles, covariance, correlation, z-scores, confidence intervals, and
grouped distribution summaries over scalar or vector observations. The contract should state sample versus population
definitions, null handling, approximate versus exact algorithms, and minimum observation counts.

### Probability and Monte Carlo

School could demonstrate seeded random variables, sampling, Monte Carlo estimates, and convergence diagnostics. Randomness
must be caller-seeded and represented in the input contract so generated and online execution can be compared. The
example should publish uncertainty and sample counts, not only a single estimate.

### Hypothesis tests and experiments

A later educational experiment workflow could compare groups with effect sizes, confidence intervals, and declared test
assumptions. It must distinguish descriptive summaries from statistical conclusions and should not silently choose a
multiple-testing correction or significance threshold.

## Geometry and discrete computation

### Geometry

Point, line, polygon, distance, intersection, and coordinate-transform examples would broaden School beyond scalar and
matrix algebra. A future geometry contract needs coordinate-system assumptions, tolerance, invalid-shape behavior, and
whether boundary contact counts as intersection. Provider-specific spatial functions should remain outside the example
unless a provider-neutral Structure contract exists.

### Graph algorithms

Typed vertices and edges could support connected components, shortest paths over bounded weighted graphs, reachability,
and simple centrality measures. Graph inputs need duplicate-edge, direction, cycle, and disconnected-component semantics.
Recursive algorithms should be expressed over a finite admitted iteration model or remain caller-owned.

### Units and dimensional analysis

A useful practical extension would attach units to values and reject invalid additions, multiplications, or conversions.
This requires a compile-time or schema-level unit contract; string labels beside numeric columns are not enough. The
initial slice should be small, such as length, time, mass, and temperature.

## Educational workflows

### Grading and feedback

The example could model assignments, submissions, rubrics, attempts, and feedback aggregation. This would demonstrate
joins, versioned criteria, late submissions, and cohort summaries, but it would introduce personally sensitive data and
needs explicit identity and retention boundaries.

### Curriculum and prerequisite paths

A curriculum workflow could resolve prerequisite graphs, recommend next exercises, and calculate progress. It should
separate observed completion from recommended sequence and handle cycles or missing prerequisites as explicit diagnostics.

### Reproducible lesson datasets

Future School additions may benefit from small named datasets that make numerical behavior visible. Such fixtures should
remain example-owned and deterministic. They should not change the shared `res/` testing model without a separate project
decision.

## Permanent boundaries

Unless a separate product decision changes the project architecture, School does not own:

- arbitrary Python numerical callbacks or opaque scientific-library execution;
- cluster provisioning, accelerator selection, or numerical-library installation;
- educational identity, gradebook persistence, notification, or access-control systems;
- claims that numerical outputs are exact when the algorithm is approximate; or
- interactive notebook, plotting, or user-interface lifecycle.

## Admission guidance

Admit School work in narrow mathematical slices. Each slice should include a small valid fixture, edge cases such as
empty or invalid input, a declared precision and convergence policy, online/generated parity where supported, and a README
example showing the observable result. Prefer transformations that keep the mathematical state visible in schemas and
generated PySpark. Use explicit hooks only when the computation genuinely cannot be represented safely, and document the
boundary and its limits.

## References

- Current School application: `examples/school/Readme.md`
- Current API deferred work: `docs/dev/deferred/ApiCatalog.deferred.md`
- Current streaming deferred work: `docs/dev/deferred/Streaming.deferred.md`
