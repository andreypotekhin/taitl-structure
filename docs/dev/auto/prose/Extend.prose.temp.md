# {{Topic}}

<!--
Extend output template.
Inputs: the Draft, background, plan where relevant, and collected source.
Preserve the complete document through Stages. Omit Draft Design and do not emit a top-level Notation section.
Use the phrase search engine at most once, and omit it when it adds no value.

Shape variants:
- Standalone transform / no composed workflow: one internal transform subsection and no Result.
- No main/workflow transform but multiple independent collected composed transforms, as in Offline: one top-level
  subsection per transform; each has local Implementation and Code numbering, nested internal/external stage
  subsections, and one nested Result for that transform. Do not synthesize a package-level parent, Workflow, or Result.
- Composed internal-only workflow, as in Chunking: Implementation has internal child-stage subsections followed by
  Result; Code has Workflow first, followed by the collected class/method subsections.
- Composed internal-plus-external workflow, as in Similarities: Implementation has internal child-stage subsections,
  external boundary subsections, then Result; Code has Workflow first, followed by collected class/method subsections.
-->

## Problem

{{Prefer one vivid, active sentence or one focused paragraph stating the general use-case or industry requirement and
desired outcome. Keep the answer, mechanisms, transform duties, policies, requirements, implementation challenges, and
failure modes out.}}

## Solution

{{Preserve and enrich the Draft conceptual answer with useful background context while applying the Solution contract:
(1) open with general domain practice, user goal, and desired outcome without project-specific names or mechanics; (2)
bridge that practice to the topic's conceptual model; (3) introduce concrete structures and formulas only after the
bridge; and (4) close with enabled behavior, semantic tradeoffs, and practical value. For query-structure topics such as
SearchFields, retain or add representative metadata-only, body-only, mixed, or aggregate query examples and explain their
meaning. Do not turn this into a design or component inventory.}}

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

{{Continuous, substantive implementation preamble. For a composed workflow, identify the parent and direct stage flow,
what enters, how the major data moves, what important limits apply, and which boundary or policy makes the result
reliable. For a standalone transform, identify the input evidence, transformation, and observable output. Keep this
preamble at component level and move challenge, validation, identity, availability, and failure discussion here rather
than into Problem.}}

<!--
Stage subsection rules:
- Internal stages contain the complete public method groups in collected-source order.
- Stage introductions are plain, active, and unnumbered. If public method groups exist, do not make the stage description
  or class summary into an additional circled italicized item; number only the method groups.
- Each numbered group has one short italicized intent, one explanatory paragraph, and one text signature block.
- Circled numbering is global across the stages of a common parent workflow, but restarts at ① for each independent
  composed top-level transform.
- Private/helper methods stay in Code and are not numbered Implementation groups.
- Internal step-transform stages end with exactly one Resulting transform shape block. A composed top-level transform ends
  with one nested Result containing its whole-transform workflow notation instead.
- For a composed internal child stage, include every assigned child stage in nested subsections and make its Resulting
  transform shape a composed workflow notation with named typed inputs, stage assignments, unqualified relation outputs,
  and typed final outputs; use a method vector only for step-transform children. For an independent composed top-level
  transform, put the whole-transform notation in its nested Result instead.
- External stages have one source-backed plain description line immediately before one canonical stage-call notation
  block, numbered with the next circled marker in the owning top-level transform. Do not italicize or invent an intent. Do not add methods,
  method groups, a method inventory, or Resulting transform shape.
- When no main/workflow transform exists, every collected transform class is internal to the document even if it is not
  called by another class. Standalone classes get complete method-group coverage and a Resulting transform shape;
  independent composed classes get nested child-stage coverage and a nested Result.
- Resolve internal child assignments recursively across collected family directories before deciding that a child is
  external; include each internal child's complete public method groups in its owning stage subsection.
- If a collected no-parent class is composed and has no public methods, its source-backed class/child-stage description is
  one numbered transform group in both Implementation and Code; do not invent methods or method signatures.
- If that class calls an external transform, include only a lightweight external boundary subsection and notation; do not
  include the external transform's implementation or Code listing.
-->

### {{InternalStageName}}

{{One self-contained active sentence describing the stage's main data transition and responsibility; prefer an imperative
opening such as “Match terms” or “Publish results” when natural.}}

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

<!-- For a composed internal stage, replace the method-only shape with workflow notation containing named typed inputs,
one stage assignment per collected child in source order, unqualified child-output relation names, and typed final outputs.
Include a method vector only on internal child stage calls that are step transforms. -->

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

<!-- For a common parent/workflow, use the following root Result. For independent composed transforms, repeat the same
block as a nested `#### Result` inside each top-level transform subsection, after that transform's child stages. -->

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
For a common parent/workflow, Result is present only for an exact composed parent/workflow class. For independent composed
top-level transforms, each transform has its own nested Result; do not invent a package-level parent or Result. Stage
arrows expose unqualified relation names, not schema types or qualified references. Final outputs remain typed.
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
non-circled sequence per independent top-level transform, or one sequence across a common workflow. Keep class listings,
stage assignments, and private/helper clauses unnumbered. Keep each stage transform description plain and unnumbered
before its class listing. Place each numbered italicized intent and its method-group explanation together in one item
immediately before the method listing it owns; never leave a separate unnumbered explanation after that item.}}

<!-- The stage description precedes the unnumbered class listing. Each public method group follows that listing as one
numbered italicized item plus its explanation, immediately followed by that group's exact method listing. -->

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
- Verify internal method coverage, local/global circled numbering according to the selected shape, complete signatures,
  and the correct shape: Resulting transform shape for step stages, nested Result for independent composed transforms.
- Verify external subsections contain one plain numbered description and one canonical notation block only.
- Emit Result only for an exact composed parent/workflow class, with unqualified stage-output relation names.
- For a topic with no parent/workflow class, require all collected transforms as internal Implementation and Code
  subsections. Independent composed transforms must each contain nested child stages and one nested Result; omit only a
  synthetic package-level parent, Workflow, or Result.
- For an internal step child, reject a duplicate standalone transform notation before its method groups; its transform
  notation appears only under Resulting transform shape. Keep generated prose free of text-operator terminology; use a
  direct chapter reference when external detail is omitted.
- Audit each internal step transform for duplicate named notation; for example, AllDocumentTargets may appear once under
  Resulting transform shape and nowhere else in its internal stage subsection.
- Compare every Python listing byte-for-byte with the collected source and keep Code numbering independent.
- For every public method listing, require one immediately preceding numbered item with a short italicized intent and the
  method-group explanation, with no separate unnumbered explanation after that item. Keep stage descriptions and
  class/interface listings unnumbered; they do not consume a method-group number.
- In Implementation, stage subsections precede Result; in Code, Workflow precedes the collected stage/class subsections.
-->
