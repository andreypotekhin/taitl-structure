# {{Topic}}

<!--
Draft output template.
Use the current topic background and intended chapter structure as the content sources.
Do not copy narrative prose from sibling, archived, or numbered-variant documents.
Use the phrase search engine at most once, and omit it when it adds no value.

Shape variants:
- Standalone transform / no composed parent: one transform notation and one continuous Implementation narrative.
- Composed parent with internal stages: parent composition and child-stage coverage belong in Notation; Implementation
  remains continuous prose.
- Composed parent with internal and external stages: parent composition and all child-stage boundaries belong in
  Notation; Implementation remains continuous prose.
- Topic without a main/workflow transform but with multiple collected transforms, as in Offline: list every transform
  as an internal stage in Stages and Notation, preserve each class's inputs/outputs and method coverage for Extend/Form,
  and do not add a synthetic parent or Result.
-->

## Problem

{{Prefer one vivid, active sentence or one focused paragraph stating the general use-case or industry requirement and
desired outcome. Stop before the answer, mechanisms, requirements, policies, stage duties, implementation challenges,
or failure modes.}}

## Solution

{{Three to five substantive paragraphs following the Solution contract: (1) establish the general domain practice, user
goal, and desired outcome without project-specific names or mechanics; (2) bridge that practice to the topic's conceptual
model and explain why it is useful; (3) introduce the concrete abstraction and a formula, model, or example when it
materially clarifies the answer; and (4) close with enabled behavior, semantic tradeoffs, and practical value. Define
concepts before use. For a query-structure topic, include representative metadata-only, body-only, mixed, or aggregate
query examples before explaining the internal representation. Adapt paragraph count while preserving this order.}}

## Builds on

- {{Canonical principal topic or workflow}}

## Used by

- {{Canonical principal topic or workflow}}

## Definitions

- **{{Concept}}**
  - {{Concise definition sentence.}}

## Inputs

- {{Input schema or relation}}

## Outputs

- {{Output schema or relation}}

## Stages

- {{StageName}}: {{input schemas or relations}} -> {{output schemas or relations}}

## Notation

~~~text
{{Parent composition and/or stage notation in execution order. Include every input, output, public step, and concrete
return schema. Use schema classes rather than vague relation labels.}}
~~~

## Design

{{Concise requirements and proposed design. Progress from purpose to boundaries, contracts, policies, invariants,
identity, ownership, compatibility, lifecycle, failure, fallback, and concurrency concerns as applicable. Keep these
requirements out of Problem and Solution.}}

## Implementation

{{Four to seven substantive paragraphs. Begin with implementation intent and boundary, then explain the input contract,
major data movement, important limits, stage responsibilities, schema contracts, and why the boundaries are separated.
Name relevant transforms and schemas using inline code where appropriate, but do not reproduce code or enumerate
low-level methods. Move challenges, validation, identity, availability, and failure behavior here rather than into
Problem.}}

## Code

{{Topic}}.cnd.md

<!--
Draft Code is only a collected-source reference. Do not reproduce source code here.
-->

<!--
QA:
- Keep the fixed section order and place Design immediately before Implementation.
- Keep Problem focused on need and consequences; keep the conceptual answer in Solution.
- Keep Builds on and Used by to canonical top-level names.
- Classify the source before shaping stages: no main/workflow transform means all collected topic transforms are internal
  document stages, not an excuse to omit their method-level source coverage or to invent a parent.
- Include all essential Definitions, Inputs, Outputs, Stages, and lossless Notation coverage.
- Do not add stage subsections or a Result section to Draft; those belong to Extend and Form.
- Do not add a Workflow subsection to Draft; Workflow is a Code-section subsection introduced by Extend and Form.
- Apply the Solution contract: reject an opening that starts with a project abstraction, formula, algorithm, policy, or
  implementation detail; require a general domain/user framing, a conceptual bridge, concrete topic explanation, and a
  closing account of behavior, tradeoffs, and value.
-->
