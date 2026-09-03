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
-->

## Problem

{{One or two focused paragraphs: motivating use case, topic-specific difficulty, and consequences. Stop before the
answer, mechanisms, requirements, policies, stage duties, or implementation.}}

## Solution

{{Three to five concise paragraphs. Move from general theory and practice to the central abstraction, its purpose,
semantic tradeoffs, and enabled behavior. Define concepts before use. Include one formula, model, or monochrome
diagram when it materially clarifies the answer. End with what the approach makes possible.}}

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

{{Four to seven paragraphs. Begin with implementation intent and boundary, then explain data movement in notation order,
stage responsibilities, schema contracts, and why the boundaries are separated. Name relevant transforms and schemas,
but do not reproduce code or enumerate low-level methods.}}

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
- Include all essential Definitions, Inputs, Outputs, Stages, and lossless Notation coverage.
- Do not add stage subsections or a Result section to Draft; those belong to Extend and Form.
- Do not add a Workflow subsection to Draft; Workflow is a Code-section subsection introduced by Extend and Form.
-->
