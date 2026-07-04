# Invariants

## Purpose

Structure invariants are internal truth checks that keep compiler and runtime phases honest. They complement
diagnostics, specifications, user stories, golden files, parity tests, differential tests, and integration tests.

Diagnostics explain user-correctable problems. Invariants catch impossible internal states after Structure has accepted
source, configuration, or runtime inputs. A failed invariant means Structure has a bug or a phase boundary passed an
invalid model to the next phase.

## Principles

Invariants must be:

- deterministic for the same source, configuration, Structure version, and target backend;
- close to the phase boundary that can first prove the condition;
- cheap enough to run in ordinary tests and compiler commands;
- written in terms of stable model fields, not object representations or memory addresses;
- separate from user diagnostics unless the failure is caused by user input.

Do not use invariants to replace diagnostics. If a developer can fix the problem by changing Structure source,
configuration, invocation inputs, or generated output freshness, emit a structured diagnostic instead.

## Schema Invariants

After schema inspection, Structure should be able to prove:

- effective field order is deterministic;
- field names are unique within a schema;
- Spark column names, including aliases, are unique within a schema;
- field definitions preserve type, nullability, primary-key, metadata, and description values;
- inherited fields precede local fields unless a specification explicitly says otherwise;
- recursive struct references are rejected by diagnostics before schema materialization.

## Transform-Plan Invariants

After symbolic compilation, Structure should be able to prove:

- every declared input and output lane has a unique name;
- step order is deterministic and follows the accepted source-order contract;
- every step input schema matches the preceding step output schema or declared relation input;
- every projected output field belongs to the declared output schema;
- every filter expression is boolean;
- every join references one current or joined side and one declared input side;
- every hook target resolves to a known step and lane.

## PySpark-Plan Invariants

After lowering to the PySpark execution plan, Structure should be able to prove:

- every input, step, output, and validation recipe names a known lane;
- every schema recipe maps to a generated schema constant;
- every generated expression recipe lowers to optimizer-visible PySpark DataFrame or Column operations;
- performance guardrails remain absent from generated paths unless a hook owns the arbitrary PySpark boundary;
- online and generated execution consume the same target-level recipe model.

## Generated-File Invariants

Before writing or comparing generated files, Structure should be able to prove:

- every generated path is relative;
- no generated path escapes the configured generated root;
- every generated path uses normalized forward-slash separators in comparison results;
- generated files carry the ownership header;
- unowned files are not removed by compare, clean, or write operations.

## Runtime-Result Invariants

After online or generated execution result assembly, Structure should be able to prove:

- single-output transforms expose the documented single result and named result access;
- multi-output transforms return every declared output lane exactly once;
- result schemas are keyed by declared output lane;
- final validation runs at the specified output boundary;
- generated-mode import failures are reported through structured runtime diagnostics, not raw import errors.

## Acceptance

The invariant contract is implemented when tests prove representative checks at each boundary and when
`docs/dev/Testing.md` describes where invariant tests belong. Invariant failures should be narrow and actionable for
Structure maintainers, while user-facing failures continue to use the diagnostic registry and public documentation
links.
