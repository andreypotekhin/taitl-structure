# Draft operator

## Shared Prose context
This chapter operator is governed by the common concepts and conventions in [Prose.md](../Prose.md). Read its
text-process model, and shared authoring guidance before applying this file.
Definitions: [Definitions](Definitions.prose.md)
Styles: 
- [Problem and Solution narrative style](Solution.style.md) 
- [Implementation narrative style](Implementation.style.md)
- [General narrative style](General.style.md)

## Draft

### Draft process
- Input: a topic's background document and its intended chapter structure.
- Output: close/draft `.draft.md` documents.
- Scope: topics selected for future user-manual chapters.
- Name: Draft. Usage: Draft(dir).
- Invocation: manual.

### Draft operator
- Name: draft(), usage: draft(dir)
- Goal: create a structured future-user-manual chapter whose Problem and Solution sections explain the topic and whose
  Design section records the requirements that guide its implementation.

### Draft operator instructions
Output-shape template: [Draft.prose.temp.md](Draft.prose.temp.md). Read it with these instructions; it captures the
fixed chapter structure and the standalone, internal-only, and internal/external shape variants without replacing the
source and narrative rules below.

Create a `.draft.md` from the topic background and retain the standard chapter structure:
Problem, Solution, Builds on, Used by, Definitions, Inputs, Outputs, Stages, Notation, Design, Implementation, and Code.

Each run must author the Problem, Solution, Design, and Implementation prose anew from the current background and prompt
rules. Do not copy, paraphrase, or use sibling, archived, or variant outputs as narrative sources. Preserve only the
required structure, vocabulary, notation coverage, and source-derived code references.

Keep enumeration-oriented sections concise. Write the Solution section as the substantive answer to the use-case problem,
not as a system-design summary:
- Use approximately three to five short paragraphs, expanding with topic complexity when a formula or important tradeoff
  needs room rather than enforcing a fixed word count.
- Begin with the theory and practice of solving the motivating use case before introducing project-specific names.
- Open with a brief general domain introduction before moving to the concrete project abstraction. If the topic has a
  user-visible query or request structure, show representative examples before explaining its internal representation.
- Explain the central abstraction, its purpose, and the important semantic tradeoffs.
- Define concepts before using them.
- Progress gradually from the problem's concepts to the solution's meaning and enabled behavior.
- Include a formula, small model, or monochrome diagram when it materially clarifies the topic.
- Explain relevant identity, ownership, compatibility, lifecycle, failure, fallback, or concurrency concerns.
- End by stating what the approach makes possible; keep architecture, implementation requirements, and mechanics in
  Design and Implementation.
- Use thoughtful overview/proposal prose rather than implementation instructions, status reports, or checklists.
- Do not add internal subsection headings inside Solution.
- Do not duplicate the detailed stage mechanics, notation, or code that belong in later sections.
- Do not call Solution a design or describe system architecture, transform responsibilities, implementation requirements,
  or stage boundaries there; move those details to Design and Implementation.
- Use the same concise style as Implementation explanation items: concrete subjects, active verbs, and one main idea or
  transition per paragraph. Avoid exhaustive component inventories, long semicolon chains, and document-structure
  commentary.
- Preserve the project's terminology and distinguish established behavior from proposed behavior.
- Keep the remaining sections concise and structurally useful for the Extend operator.

Write the Problem section according to [Solution.style.md](Solution.style.md). Keep the problem focused on the need,
central difficulty, and consequences; do not let it contain the answer, transform responsibilities, or implementation
requirements.

Write Problem and Solution according to [Solution.style.md](Solution.style.md). The Solution must be useful to a
technically confident reader who understands software but may be unfamiliar with the industry topic or this project's
vocabulary.

Draft the remaining sections in the concise, structured style exemplified by `close/draft/search/transforms/indexing/Indexing.draft.md`:
- `Problem`: describe the industry and project need in one or two focused paragraphs. For Search topics, ground it in
  building and using a modern search system. State the user-facing need and desired outcome in active, lively language;
  one focused sentence is acceptable. Do not turn Problem into a list of challenges, risks, limits, or failure modes;
  move those to Solution, Design, or Implementation.
- `Solution`: provide the full conceptual narrative described above. Explain how the solution addresses the problem,
  using relevant theory and practical search-engine wisdom. Explain its central concepts, why the approach works, semantic
  tradeoffs, and enabled behavior. Do not call it a design or describe architecture, transform responsibilities,
  requirements, stage boundaries, or implementation mechanics; do not announce it with “The solution is.”
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
- `Design`: record the proposed design and implementation requirements in concise narrative paragraphs, moving from the
  solution's purpose to the boundaries, contracts, policies, and invariants that must realize it. Keep requirements here,
  not in Problem or Solution. Place Design immediately before Implementation.
- `Implementation`: write a second substantive narrative, more concrete than Solution and less mechanical than Code.
  Apply [Implementation.style.md](Implementation.style.md) to its narrative, preamble, and explanation items. Begin with
  the implementation's intent and boundary, then explain how data moves through the stages in the order established by
  Notation. Use an occasional connective word when it clarifies the movement, but keep the narrative centered on the
  concepts and responsibilities rather than on progression itself. Name relevant transforms and schemas, and explain why
  responsibilities are separated.
  - Use approximately four to seven paragraphs, expanding with topic complexity.
  - Use Notation as the source of truth without repeating every notation line mechanically.
  - Do not include source code, collected-code references, implementation checklists, or low-level operator inventories.
- `Code`: retain the heading and identify the corresponding collected document by filename, such as `Indexing.cnd.md`. Do not reproduce source code in the draft.

Keep the section order fixed. The draft is a structured chapter source: its lists establish the chapter's vocabulary and
interfaces, its Notation block establishes workflow coverage, its Solution establishes the reader-facing conceptual
argument, and its Design records implementation requirements.

### Draft operator - Problem section
Problem section:
- Apply [Solution.style.md](Solution.style.md).
- Ground the opening in the motivating use case, then narrow to the topic's specific difficulty or constraint.
- Stop once the problem and its consequences are clear; do not preview the answer, algorithms, stages, policies, or
  implementation responsibilities.
- Enrich the problem only when the background adds essential reader context; do not turn it into a problem inventory or
  implementation walkthrough.
- Use casual language, prioritize thoughtful explanation/intent over prescription/direction, gradually build understanding.
- Prefer one vivid, active statement of the general use-case or industry requirement. Move candidate, identity, policy,
  scale, and availability challenges into Design or Implementation instead of accumulating them in Problem.

### Draft operator - Solution section
Solution section:
- Apply [Solution.style.md](Solution.style.md).
- Treat Solution as the conceptual answer to the use-case problem, rather than as a short summary of Background or a
  system-design section.
- Preserve general-to-specific progression from theory and practice to the topic's central abstraction and enabled
  behavior. Do not call Solution a design or expand it into a component inventory, requirements list, or implementation
  walkthrough.
- Use casual language, prioritize thoughtful explanation/intent over prescription/direction, gradually build understanding.
- Make Solution content available for first-time reader: conceptual, easier on technical details.
- Start with a general explanation of the domain practice before introducing the project's concrete abstraction. For
  query-structure topics such as SearchFields, include representative metadata-only, body-only, mixed, or aggregate
  query examples and explain what each example means before naming the implementation stages.
- Include textbook-grade explanations as needed.
- Describe the answer directly; do not write “The solution is,” refer to the Solution section, or leave a component-only sketch.

### Draft operator - Design section
Design section:
- Place Design immediately before Implementation.
- Move requirements and proposed architecture out of Problem and Solution into concise, continuous Design prose.
- Progress from the solution's purpose to the boundaries, contracts, policies, and invariants that guide implementation.
- Keep Design concrete enough to guide Implementation, but do not reproduce stage mechanics, notation, or code.

### Draft operator - Quality assurance
- Verify that `Problem` moves from general need to topic-specific difficulty in concise paragraphs, introducing concepts
  before use, keeping the answer, requirements, and implementation detail out, and using active, expressive language.
- Verify that `Solution` explains how the solution addresses the problem, introduces concepts before use, does not
  call itself a design, “the solution,” or describe architecture, requirements, or implementation
  responsibilities.
- Verify that `Solution` moves from general purpose to central concepts and enabled behavior in approximately three to
  five concise paragraphs, avoiding catalog-like prose.
- Verify that `Design` immediately precedes `Implementation` and contains the requirements, boundaries, contracts,
  policies, and invariants needed to guide implementation.
- Verify that `Builds on` and `Used by` contain only canonical names of principal top-level topics or workflows. Reject
  schema classes, step methods, internal stage transforms, policy objects, and generic relationship prose; allow an empty
  section when no top-level topic applies.
