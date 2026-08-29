# Draft operator

## Shared Prose context
This chapter operator is governed by the common concepts and conventions in [Prose.md](../Prose.md). Read its
[Definitions](../Prose.md#definitions), text-process model, and shared authoring guidance before applying this file.

## Draft

### Draft process
- Input: a topic's background document and its intended chapter structure.
- Output: close/draft `.draft.md` documents.
- Scope: topics selected for future user-manual chapters.
- Name: Draft. Usage: Draft(dir).
- Invocation: manual.

### Draft operator
- Name: draft(), usage: draft(dir)
- Goal: create a structured future-user-manual chapter whose Solution section contains the topic's real conceptual explanation.

### Draft operator instructions
Create a `.draft.md` from the topic background and retain the standard chapter structure:

Problem, Solution, Builds on, Used by, Definitions, Inputs, Outputs, Stages, Notation, Implementation, and Code.

Keep enumeration-oriented sections concise. Write the Solution section as the substantive chapter narrative:

- Use approximately five to eight paragraphs, expanding or contracting with topic complexity rather than enforcing a fixed word count.
- Begin with general theory or industry context before introducing project-specific names.
- Explain the central abstraction, its purpose, and the important semantic tradeoffs.
- Define concepts before using them.
- Progress gradually from theory to the project's proposed design.
- Include a formula, small model, or monochrome diagram when it materially clarifies the topic.
- Explain relevant identity, ownership, compatibility, lifecycle, failure, fallback, or concurrency concerns.
- End by stating what the project intends to implement and what behavior that enables.
- Use thoughtful overview/proposal prose rather than implementation instructions, status reports, or checklists.
- Do not add internal subsection headings inside Solution.
- Do not duplicate the detailed stage mechanics, notation, or code that belong in later sections.
- Preserve the project's terminology and distinguish established behavior from proposed behavior.
- Keep the remaining sections concise and structurally useful for the Extend operator.

The Solution must be useful to a technically confident reader who understands software but may be unfamiliar with the
industry topic or this project's vocabulary.

Draft the remaining sections in the concise, structured style exemplified by `close/draft/search/transforms/indexing/Indexing.draft.md`:

- `Problem`: describe the industry and project need in one or two focused paragraphs. Ground the problem in the topic itself;
  do not refer to earlier text-pipeline steps.
- `Solution`: provide the full conceptual narrative described above. Frame it as a proposed solution: explain the design
  the chapter recommends, why its boundaries and tradeoffs are chosen, and what behavior the proposal should enable.
  Refrain from referring to the proposed solution as such: omit 'The proposed solution is' and similar wording.
- `Builds on`: list only principal top-level topics that supply the topic's inputs, using canonical topic names such as
  `Chunking` or `Scoring`. Omit schema classes, step methods, internal stage transforms, policy objects, and generic
  prose; leave the section empty when no top-level topic applies.
- `Used by`: list only principal top-level topics or workflows that consume the topic's outputs, using the same canonical
  names. Omit schema classes, step methods, internal stage transforms, policy objects, and generic consumer descriptions.
- `Definitions`: define the small set of topic concepts needed by the chapter. Prefer bold, single-word concept names followed by concise explanations.
- `Inputs`: list each input schema or relation.
- `Outputs`: list each output schema or relation.
- `Stages`: list each public or workflow stage as `StageName: inputs -> outputs`. Keep this as an inventory of boundaries, not an explanation of step mechanics.
- `Notation`: include one fenced text block for the workflow. List stages in execution order and list every meaningful step
  with its input and output relations. Every return/output position must use the concrete schema class or classes, never a
  vague relation label such as `targets`, `embeddings`, or `scores` when the schema is known. Keep the notation lossless
  and concise; do not add implementation prose or Extend reference markers.
- `Implementation`: write a second substantive narrative, more concrete than Solution and less mechanical than Code. Begin with the implementation's intent and boundary, then explain how data moves through the stages in the order established by Notation. Name the relevant transforms and schemas, explain why responsibilities are separated, and describe the contracts that make the flow reliable.
  - Use approximately four to seven paragraphs, expanding with topic complexity.
  - Explain stage responsibilities and ordering in prose; use a short numbered sequence when order is itself an important behavior.
  - Cover relevant validation, invariants, identity, ownership, failure, fallback, freshness, concurrency, or observability behavior.
  - Distinguish caller-owned responsibilities, transform-owned responsibilities, and provider or backend responsibilities.
  - Use Notation as the source of truth without repeating every notation line mechanically.
  - Explain what each important boundary guarantees to the next boundary, including the behavior of partial or failed inputs.
  - End with the implementation shape and the observable behavior it enables.
  - Do not include source code, collected-code references, implementation checklists, or low-level operator inventories.
- `Code`: retain the heading and identify the corresponding collected document by filename, such as `Indexing.cnd.md`. Do not reproduce source code in the draft.

Keep the section order fixed. The draft is a structured chapter source: its lists establish the chapter's vocabulary and interfaces, its Notation block establishes workflow coverage, and its Solution establishes the reader-facing conceptual argument.

### Draft operator - Quality assurance

- Verify that `Solution` opens with a clearly proposed design and continues as a recommendation narrative: explain chosen
  boundaries, tradeoffs, and enabled behavior rather than only describing existing facts.
- Verify that `Builds on` and `Used by` contain only canonical names of principal top-level topics or workflows. Reject
  schema classes, step methods, internal stage transforms, policy objects, and generic relationship prose; allow an empty
  section when no top-level topic applies.
