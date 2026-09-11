# {{Topic}}

<!-- Follow Extend.prose.md. "for", "if", and named fragments below describe expansion; omit these comments in output. -->

<!-- Specializations use Definitions' named-base-plus-all-replacements branch. Retain complete effective inputs/outputs,
local method items, and actual replacement-stage subsections. End a specialized step with its shape and a specialized
composition with Result. Unchanged inherited behavior is supplied by the exact named base, not an invented stage. -->

## Intent

{{Draft's one- or two-sentence use-case need and desired outcome.}}

## Problem

{{Draft's developed situation, difficulty, and consequences, improved only where useful.}}

## Solution

{{Preserved conceptual coverage, enriched under the Solution contract.}}

## Builds on

{{Canonical principal topics, or empty.}}

## Used by

{{Canonical principal topics, or empty.}}

## Definitions

- **{{Concept}}**: {{Definition sentence.}}

## Inputs

- {{Schema or relation}}

## Outputs

- {{Schema or relation}}

## Stages

- {{StageName}}: {{inputs}} -> {{outputs}}

## Implementation

{{Required connected preamble paragraphs: purpose and inputs, major data movement and representations, separation of
responsibilities, and important source-backed limits. Develop the Draft account before beginning the stage tree.}}

<!-- Expand Definitions.prose.md's rendering algebra:
sole main composed: child subsections + ### Result;
step main: ### MainName + StepBody;
multiple roots (even with a main), or no main: ### RootName + StepBody or ComposedBody, restarting counters per root.
ComposedBody = plain intro + nested child subsections + nested Result.
Choose subsection depths from ownership, not a fixed heading level. -->

### {{InternalStepName}}

{{Plain active account of this stage's input evidence, substantive operation, and useful output. No placeholder
"Implement/Run {{InternalStepName}}" sentence; this introduction does not replace any public method group.}}

① *{{Short intent.}}* {{Useful explanation in the same paragraph.}}

~~~text
{{method}}({{named typed arguments}}) -> {{ConcreteReturn}}
{{Every additional public method in this group, with its complete signature.}}
~~~

<!-- Repeat public groups, advancing the root's circled counter. -->

Resulting transform shape:

~~~text
{{InternalStepName}}:
  inputs:
    {{name}}: {{Schema}}
  methods:
    {{method}}: {{all argument schemas}} -> {{concrete return schemas}}
  outputs:
    {{name}}: {{Schema}}
~~~

### {{ExternalStageName}}

② {{Source-backed plain description; no italicized intent.}}

~~~text
{{ExternalStageName}}:
  inputs:
    {{name}}: {{Schema}}
  outputs:
    {{name}}: {{Schema}}
~~~

### Result

{{What this composed transform publishes from its inputs.}}

~~~text
{{ComposedTransformName}}:
  inputs:
    {{name}}: {{Schema}}
  stages:
    {{alias}} = {{StageName}}({{complete bindings}}) -> {{unqualified output relations}}
  outputs:
    {{name}}: {{Schema}}
~~~

<!-- Result belongs to every internal composed transform, not to step transforms or a synthetic package parent. -->

## Code

<!-- Insert the collected tree, stripping its H1 and adjusting heading depths.
Use ### Workflow only for a sole composed main; otherwise start with the root class heading.
Reset Code's independent decimal counter per root. -->

### {{Workflow or InternalTransformName}}

{{Exact plain collected class description.}}

~~~python
{{Exact collected class/interface listing.}}
~~~

1. *{{Exact collected intent.}}* {{Exact collected explanation in the same paragraph.}}

~~~python
{{Exact collected method-group listing.}}
~~~

<!-- Repeat collected method/helper groups as numbered paragraphs, never method headings.
Only actual child transforms introduce further container headings, in collected order. -->

### {{ExternalStageName}}

2. *{{Exact collected external intent.}}* {{Exact collected external explanation.}}

~~~python
{{Exact collected parameterized stage assignment.}}
~~~
