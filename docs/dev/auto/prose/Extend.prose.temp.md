# {{Topic}}

<!--
Extend output template.
Inputs: the Draft, background, plan where relevant, and collected source.
Preserve the complete document through Stages. Omit Draft Design and do not emit a top-level Notation section.
Use the phrase search engine at most once, and omit it when it adds no value.

Shape variants:
- Standalone transform / no composed workflow: one internal transform subsection and no Result.
- Composed internal-only workflow, as in Chunking: Implementation has internal child-stage subsections followed by
  Result; Code has Workflow first, followed by the collected class/method subsections.
- Composed internal-plus-external workflow, as in Similarities: Implementation has internal child-stage subsections,
  external boundary subsections, then Result; Code has Workflow first, followed by collected class/method subsections.
-->

## Problem

{{Concise general-to-specific use-case problem. Keep the answer, mechanisms, transform duties, policies,
requirements, and implementation out.}}

## Solution

{{Preserve and enrich the Draft conceptual answer with useful background context. Keep general-to-specific
progression, theory, central abstraction, behavior, and tradeoffs. Do not turn this into a design or component
inventory.}}

## Builds on

- {{Canonical principal topic or workflow}}

## Used by

- {{Canonical principal topic or workflow}}

## Definitions

- **{{Concept}}**: {{Concise definition sentence.}}

## Inputs

- {{Input schema or relation}}

## Outputs

- {{Output schema or relation}}

## Stages

- {{StageName}}: {{input schemas or relations}} -> {{output schemas or relations}}

## Implementation

{{Continuous implementation preamble. For a composed workflow, identify the parent and direct stage flow, what enters,
how the major data moves, and which boundary or policy makes the result reliable. For a standalone transform, identify
the input evidence, transformation, and observable output. Keep this preamble at component level.}}

<!--
Stage subsection rules:
- Internal stages contain the complete public method groups in collected-source order.
- Each numbered group has one short italicized intent, one explanatory paragraph, and one text signature block.
- Circled numbering is global across all internal groups.
- Private/helper methods stay in Code and are not numbered Implementation groups.
- Internal stages end with exactly one Resulting transform shape block.
- External stages have one source-backed plain description line immediately before one canonical stage-call notation
  block, numbered with the next global circled marker. Do not italicize or invent an intent. Do not add methods,
  method groups, a method inventory, or Resulting transform shape.
-->

### {{InternalStageName}}

{{Stage introduction.}}

① *{{Source-backed intent.}}* {{Explanation of the data transition and responsibility.}}

~~~text
{{public_method}}({{named typed arguments}}) -> {{ConcreteReturnSchema}}
~~~

Resulting transform shape:

~~~text
{{InternalStageName}}:
  inputs:
    {{input_name}}: {{InputSchema}}
  methods:
    {{public_method}}: {{argument schemas}} -> {{return schema}}
  outputs:
    {{output_name}}: {{OutputSchema}}
~~~

### {{ExternalStageName}}

② {{Shortest source-backed description of the external boundary.}}

~~~text
{{ExternalStageName}}:
  inputs:
    {{input_name}}: {{InputSchema}}
  outputs:
    {{output_name}}: {{OutputSchema}}
~~~

<!--
Repeat internal and external stage subsections in workflow/source order.
When a method group names parallel grain paths, enumerate every named path in its signatures and shape.
-->

### Result

{{One self-contained sentence explaining the workflow's data transition into the published result.}}

~~~text
{{Topic}}:
  inputs:
    {{input_name}}: {{InputSchema}}
  stages:
    {{stage_alias}} = {{StageName}} -> {{output_relation_name}}, ...
  outputs:
    {{output_name}}: {{OutputSchema}}
~~~

<!--
Result is present only for an exact composed parent/workflow class. Stage arrows expose unqualified relation names,
not schema types or qualified references. Final outputs remain typed.
-->

## Code

<!-- Include Workflow only when the collected source contains a composed parent/workflow class. -->

### Workflow

{{Use a source-grounded workflow description when the collected Workflow listing has no prose. Do not place a scope
or production line between Code and its first subsection.}}

~~~python
{{Exact Workflow listing copied from the collected source.}}
~~~

<!-- For a standalone transform, omit Workflow above and begin with the transform class subsection. -->

### {{InternalStageName}}

{{Collected stage prose and method groups in source order. Number public method-group clauses with one independent
non-circled sequence. Keep class listings, stage assignments, and private/helper clauses unnumbered.}}

~~~python
{{Exact collected class and method listings, unchanged.}}
~~~

<!--
Include every collected transform/method section in source order.
External stages without collected class listings are represented by the Workflow listing and Implementation boundary
subsection; do not invent Code listings for them.
Separate adjacent code listings with ordinary prose.
-->

<!--
QA:
- Preserve H1 and every section through Stages; omit Design and top-level Notation.
- Verify every actual child-stage assignment, including external calls, has the correct Implementation subsection.
- Verify internal method coverage, global circled numbering, complete signatures, and one Resulting transform shape.
- Verify external subsections contain one plain numbered description and one canonical notation block only.
- Emit Result only for an exact composed parent/workflow class, with unqualified stage-output relation names.
- Compare every Python listing byte-for-byte with the collected source and keep Code numbering independent.
- In Implementation, stage subsections precede Result; in Code, Workflow precedes the collected stage/class subsections.
-->
