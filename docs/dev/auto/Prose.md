# Prose automation

## Reference
Documenting Automation: [Documenting.auto.md](Documenting.auto.md)
Documenting: [Documenting.md](../Documenting.md)
Source Annotation: [Annotation.prose.md](prose/Annotation.prose.md).

## Text processes
Text process is the process of creating documentation content from existing code and texts.
It can be automated (e.g. triggered by code changes on task completion), or manual (triggered with a task).

Text process may be thought of as automated or semi-automated content pipeline.
Example: source annotation process is defined in [Annotation.prose.md](prose/Annotation.prose.md). It is an automated process.

Text process definition includes the following:
- Establishes the scope, inputs and outputs (dirs).
- Defines synchronization policy: e.g. source annotation automatically synchs on underlying code changes.
- Instructions on content transformation
- Points on authoring style/character
- Tips, notes, exceptions.

The process notation resembles a callable Python class, but is intended for communicating, not execution.
Ex: Annotate(dir_name) invocation in an automation task.

### Existing text processes
- Annotation, defined in [Annotation.prose.md](prose/Annotation.prose.md).
- Documentation, defined in 'Documentation pipeline' section of [Documenting.md](../Documenting.md)
  and [Documenting.auto.md](Documenting.auto.md)

## Text operators
Text operator is a code-to-text or text-to-text transformation bearing certain authoring style,
e.g. optimized for brevity, generality etc.

Example:
- Annotation.prose.md defines annotate() text operator in subsections 'Example', 'General tips' of 'Creating annotated code' section.
- It also describes how annotate() text operator applies to narrower contexts:
  - Annotated source for example code.
  - Annotated source for non-example code.

The operator notation resembles Python function call with named parameters, but is intended for communicating, not execution.
Ex: annotate(dir_name) invocation in an automation task.

Text operator definition only includes text transformation, not where to apply it,
so additional instructions (like dir_name parameter) is needed for invocation.

### Existing text operators
Existing text operators:
- baseline(): not explicitly mentioned; serving as baseline for all other operators
  - defined by 'Documenting ...' sections of [Documenting.md](../Documenting.md) and [Style.md](../Style.md)
- top_level(): defined by
  - section 'End-User documentation' of [Documenting.md](../Documenting.md)
  - section 'End-User top-level documentation' of [Documenting.auto.md](Documenting.auto.md)
- background(), reference(), recipes(): defined in
  - section 'End-User reference documentation' of [Documenting.auto.md](Documenting.auto.md)
  - section 'End-User documentation tips' of [Documenting.auto.md](Documenting.auto.md)
- developer(): defined in
  - section 'Developer documentation' of [Documenting.md](../Documenting.md)
  - sections 'Developer documentation - Top-level', 'Developer documentation - Other' of [Documenting.auto.md](Documenting.auto.md)
- annotate(): defined in [Annotation.prose.md](prose/Annotation.prose.md).

## Definitions
Chapter operator: text operator related to creation of chapters in the prospected user manual.
The chapter operators are specified in the separate files listed below.

Chapter document: resulting document when a chapter operator is applied.
Ex: Chunking.form.md (produced by Format operator).
Chapter document usually discusses one big transform, e.g. Chunking.

Main transform: the main transform of the chapter document.
Step method: a step method of a transform. Optionally decorated with @step in transform code.
Step transform: a transform that consists of step methods (as opposed to composed transform).
Composed transform: a transform that consists of stages (other transforms) rather than step methods.
Stage: a stage of composed transform, usually defined as assignment of a Transform to a field in the composed transform.
Workflow transform: the main transform which is simultaneously a composed transform.
Stage transform: a transform that serves as/implements a stage in a bigger (parent) transform,
usually as a stage of the workflow transform.
Internal stage: a stage whose transform code is in same package as parent transform, or its subpackages.
External stage: a stage whose transform code is outside parent transform package and its subpackages.

Text notation: compact plain text notation for schemas, transforms and their parts: stages, step methods.
Ex: See Chunking.ext.md:
- 'DocumentChunking' section for examples of step and transform notation.
- 'Result' section for example of composed transform notation.
Formula notation: Structure Formula Notation defined in [Notation.md](prose/Notation.md)
Step notation (Typed step notation): notation for step method (text or formula depending on text operator)

Stages section: a top-level 'Stages' section with a concise list of workflow stages.
Stage subsection: in a document with Implementation section, a subsection of Implementation describing
a Stage - a transform that serves as a stage in a bigger transform, usually a stage of the workflow transform.
Internal stage subsection: stage subsection for an internal stage.
External stage subsection: stage subsection for an external stage.
Resulting shape block: canonic stage transform notation at the end of stage subsection, usually preceded with `Resulting transform shape:` label

Explanatory item: for a step method, the prose which explains it; usually a numbered item.
Stage call: the assignment of stage to a field of a composed transform.

## Chapter operators
The chapter operators are defined in focused specifications under `docs/dev/auto/prose/`:

- [Draft.prose.md](prose/Draft.prose.md): create structured future-manual chapter sources.
- [Collect.prose.md](prose/Collect.prose.md): turn annotated source into continuous collected narrative.
- [Extend.prose.md](prose/Extend.prose.md): extend draft chapters with background and collected source.
- [Format.prose.md](prose/Format.prose.md): convert extended chapters into formula-formatted documents.

Each operator file is subordinate to this document. Apply its rules together with the shared definitions, text-process
model, notation, and authoring guidance above; the operator file's `Shared Prose context` section links back here directly.
