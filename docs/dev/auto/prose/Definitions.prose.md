# Shared prose definitions

These definitions apply to the chapter operators described in the neighboring `.prose.md` documents.

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
Workflow transform: the main transform which is simultaneously a composed transform.
Stage: a stage of composed transform, usually defined as assignment of a Transform to a field in the composed transform.
Stage transform: a transform that serves as/implements a stage in a bigger (parent) transform,
usually as a stage of the workflow transform.
Internal stage: a stage whose transform code is in same package as parent transform, or its subpackages.
External stage: a stage whose transform code is outside parent transform package and its subpackages.

Text notation: compact plain text notation for schemas, transforms and their parts: stages, step methods.
Ex: See Chunking.ext.md:
- 'DocumentChunking' section for examples of step and transform notation.
- 'Result' section for example of composed transform notation.
Formula notation: Structure Formula Notation defined in [Notation.md](Notation.md)
Step notation (Typed step notation): notation for step method (text or formula depending on text operator)

Stages section: a top-level 'Stages' section with a concise list of workflow stages.
Stage subsection: in a document with Implementation section, a subsection of Implementation describing
a Stage - a transform that serves as a stage in a bigger transform, usually as a stage of the workflow transform.
Internal stage subsection: stage subsection for an internal stage.
Resulting shape block: canonic stage transform notation at the end of stage subsection, usually preceded with
`Resulting transform shape:` label

Explanatory item: for a step method, the prose which explains it; usually a numbered item.
Stage call: the assignment of stage to a field of a composed transform.

Overly complicated language: narrative style that heavily relies on complicated terminology  
- The narrative style that relies on overly complicated terminology, such as heavy use of 
boundary, contract, orchestration, facts, evidence, to convey the meaning.
