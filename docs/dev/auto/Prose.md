# Prose automation

## Reference  
Documenting Automation: [Documenting.auto.md](Documenting.auto.md)
Documenting: [Documenting.md](../Documenting.md)
Source Annotation: [Annotation.auto.md](Annotation.auto.md).

## Text processes
Text process is the process of creating documentation content from existing code and texts.
It can be automated (e.g. triggered by code changes on task completion), or manual (triggered with a task). 

Text process may be thought of as automated or semi-automated content pipeline.
Example: source annotation process is defined in [Annotation.auto.md](Annotation.auto.md). It is an automated process.

Text process definition includes the following:
- Establishes the scope, inputs and outputs (dirs). 
- Defines synchronization policy: e.g. source annotation automatically synchs on underlying code changes.
- Instructions on content transformation
- Points on authoring style/character
- Tips, notes, exceptions.

The process notation resembles a callable Python class, but is intended for communicating, not execution. 
Ex: Annotate(dir_name) invocation in an automation task. 

### Existing text processes
- Annotation, defined in [Annotation.auto.md](Annotation.auto.md).
- Documentation, defined in 'Documentation pipeline' section of [Documenting.md](../Documenting.md) 
and [Documenting.auto.md](Documenting.auto.md)

## Text operators
Text operator is a code-to-text or text-to-text transformation bearing certain authoring style,
e.g. optimized for brevity, generality etc.

Example:
- Annotation.auto.md defines annotate() text operator in subsections 'Example', 'General tips' of 'Creating annotated code' section. 
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
- annotate(): defined in subsections 'Example', 'General tips' of 'Creating annotated code' section of [Annotation.auto.md](Annotation.auto.md). 

More operators and processes are defined below. 

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
- Keep the remaining sections concise and structurally useful for the Combine operator.

The Solution must be useful to a technically confident reader who understands software but may be unfamiliar with the industry topic or this project's vocabulary.

Draft the remaining sections in the concise, structured style exemplified by `close/draft/search/transforms/indexing/Indexing.draft.md`:

- `Problem`: describe the industry and project need in one or two focused paragraphs. Ground the problem in the topic itself; do not refer to earlier text-pipeline steps.
- `Solution`: provide the full conceptual narrative described above.
- `Builds on`: list the principal collections, transforms, or relations that supply the topic's inputs.
- `Used by`: list the principal transforms or workflows that consume the topic's outputs.
- `Definitions`: define the small set of topic concepts needed by the chapter. Prefer bold, single-word concept names followed by concise explanations.
- `Inputs`: list each input schema or relation.
- `Outputs`: list each output schema or relation.
- `Stages`: list each public or workflow stage as `StageName: inputs -> outputs`. Keep this as an inventory of boundaries, not an explanation of step mechanics.
- `Notation`: include one fenced text block for the workflow. List stages in execution order and list every meaningful step
  with its input and output relations. Every return/output position must use the concrete schema class or classes, never a
  vague relation label such as `targets`, `embeddings`, or `scores` when the schema is known. Keep the notation lossless
  and concise; do not add implementation prose or Combine reference markers.
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

## Collection
Present annotated source as continuous narrative.

### Collect process
- Input: close/annotated
- Output: close/collected
- Scope: close/annotated/search
- Name: Collect. Usage: Collect(dir)
- Invocation: manual

### Collect operator
- Name: collect(), usage: collect(dir)
- Input: .anno.md describing a transform. Ex: rerank.anno.md
- Output: .cnd.md describing a transform in collectd form. Ex: rerank.cond.md
- Goal: present annotated source as continuous narrative.
- Instructions
  - Top header: convert to gerund, optionally extend to fuller phrase.
    - Ex: 'Rerank Documents' becomes 'Reranking the Documents'
  - Low-level headers: drop low-level headers. If that affects clarity, repeat the header as part of intro section paragraph.
    - Ex: 'Select fallback options' dropped, 'select fallback options' embedded in section paragraph.
  - Avoid merging code listings, maintain a text sentence in between.
  - Drop import statements.
  - Workflow transform is main transform in a package - a staged transform that rules other transforms in the package.   
  Usually, workflow transform is alphabetically the last file in dir (Ex: 'SearchDocuments'). Not all dirs contain the workflow transform. 
    - Top header: as-is, do not convert to gerund
    - Replace subsections as described above
    - Move workflow class listing from intro section into a new section, 'Workflow'
    - The `Workflow` section thus contains the parent workflow class listing plus all former sections, except the intro
      section. It must retain every stage call in execution order, including calls to stages defined outside the workflow
      directory, so the complete parent orchestration is readable in one place.
    - Append workflow document with the content (.cnd.md) of subtransforms as additional sections to form continuous narrative.
      - Order subtransform sections according to their stage's order in the workflow transform.
      - Convert the headers of the appended content to one level lower, to maintain header structure.
      - Do not remove any headers: former top-level headers become section headers. 
    - Treat the workflow directory as a package tree: inspect sibling source files and relevant immediate subpackages,
      - For same-package stages, include the annotated source for the stage transform. If one source file defines 
      multiple transform classes, include each class separately in workflow order.
    - Some stages may be outside of workflow dir.
      - For stages in the immediate subdirs of workflow dir: include them into main workflow doc similar subtransforms, as described above.
      - For stages defined in different dirs outside of workflow dir:
        - Include a section heading and brief description, ", as described in 'doc_header'".
        - Keep the full stage call in the parent `Workflow` section and include the complete parameterized stage call in
          the external-stage section as well, so the external reference is self-contained.
        - Describe the external stage in prose without repeating the assignment inline; the fenced listing is the single
          external reference for that call.
        - The intentional duplication is limited to the stage assignment; never duplicate the parent workflow class
          listing or a child transform narrative in the external-stage section.
        - For example, if the workflow class contains `features = Features(...)`, the external-stage section contains
          the complete `features = Features(...)` assignment, not a second `class Training(Transform):` listing.
    - Quality assurance:
      - Verify transform sections follow the order of main transform stages, with no stages missing.
      - Verify the package stage inventory is complete: every same-package stage class from the workflow has a corresponding section in the collected output.
      - Verify every same-package stage section contains its complete class declaration and all annotated step code,
        including files that define more than one transform class.
      - Verify the complete parent orchestration, including every external stage call, appears in `Workflow` in
        execution order.
      - Verify every external stage section contains the complete parameterized stage call and that the parent Workflow
        also contains the call in execution order.
      - Verify parent workflow class listings and child transform narratives are not duplicated as external listings.
      - Verify each output has exactly one top-level header, and that its first nonblank line is that header.
      - Verify each workflow output has exactly one parent `Workflow` section before inserted child sections.
      - For transform steps, verify code listing is present in each transform step section.
    - Thus, the texts between code listings in 'Workflow' section may become redundant, since the stages describe them in detail in the included content.
      - Trim texts between code listings in 'Workflow' to avoid repetition.

## Combine
Present combined continuous narrative based on draft, background and collected documents.

### Combine process
- Inputs: close/draft, close/collected, .back.md
- Output: close/combined
- Scope: close/collected/search
- Name: Combine. Usage: Combine(dir)
- Invocation: manual

### Combine operator
- Name: combine(), usage: combine(dir)
- Input: .draft.md, .back.md and .cnd.md describing a transform. Ex: chunking.draft.md
- Output: .comb.md describing a transform in combined narrative. Ex: chunking.comb.md
- Goal: present combined continuous narrative based on draft, background, planning and collected documents.
 
### Combine operator instructions
Combine draft (.draft.md), background (.back.md), plan (plan.md) and collected (.cnd.md) docs:
- Draft (close/draft) contains structured chapter (.draft.md) of the future user manual, including a substantive Solution narrative.
- Combine draft doc with background and collected docs to create an introduction narrative focused on a search engine topic, such as 'chunking'
- Use relevant plan documents (.plan.md) for deep insight into decisions/tradeoffs/inner worsings. Use to extend the narrative without explicit importing parts of plan. Avoid citing/referring the plan docs.
- Maintain content and structure set by draft doc
 - Make improvements/corrections to draft as needed, but keep it brief/succinct where it is already 
  - Specifically, some sections are mention/enumeration only: Builds on, Used by, Inputs/Outpus/Stages, Notation
 - Only include concepts under Definitions, concept name is mostly single-word.    
 - Insert contents of background and collected docs as described below.
- Audience: technically confident reader may not be familiar with industry specifics, field terminology or what comprises the target system.
- Do not modify draft, background and collected docs (other than an update from source/annotated, if needed). We only produce the new output doc.

Combining Draft with Background docs:

'Solution' section:
- Preserve the draft's substantive Solution narrative and enrich it with the background document where useful.
- Treat Solution as the conceptual center of the combined document, rather than as a short summary of Background.
- Make Solution content available for first-time reader: more conceptual, easier on technical details (ok to mention code components).
- Include textbook-grade explanations as needed.
- Technical details go to other sections, e.g. Implementation
- Also consider less-technical parts from 'How it works'/'Implementation' to go to the main section
- Use casual language, prioritize thoughtful explanation/intent over prescription/direction, gradually build understanding.
- Merge-in Solution section from draft doc if not already covered.

'Stages' section:
- Convert stage table to Structure notation for each stage 
  - Structure notation: docs/dev/auto/prose/Notation.md
  - As described in 'Stage Notation - Default' section
  - Use compact variant, without assignments
  - Place stages 

Implementation section:
- 'How it works' section of the background doc gets combined with 'Implementation' section from the draft doc
- For 'How it works' section:
  - Drop implementation direction content such as discussion of invalid inputs, 'should'/'must' paragraphs
  - Drop content from decisions sections and on
- Preserve the substantive Implementation prose from the draft before the workflow notation. Do not replace those
  paragraphs with a list or hide them after the notation.
- Preserve the complete combined document before `## Implementation`: retain the H1 and every preceding `Problem`,
  `Solution`, `Builds on`, `Used by`, `Definitions`, `Inputs`, `Outputs`, and `Stages` section. Never rebuild the output
  starting at `## Implementation`.
- Use this exact order inside the combined `Implementation` section:
  1. the combined conceptual/technical prose from the background and draft Implementation;
  2. one subsection for each workflow transform stage, including its explanatory items, individual typed step notation,
     and its final compact text notation;
  3. a `Result` subsection containing the complete workflow notation only when the transform composes child stages.
- In each stage subsection, explain the stage with circled items such as `①`, `②`, and `③`. Drop the short italic
  intent sentence used by the earlier format; each item should contain the explanatory prose directly.
- Name each stage subsection with the exact transform class it describes, and use that same exact class name in its
  compact notation. Derive the names from the source `Stages` inventory and the collected Code classes; never rename a
  child stage to the package or parent workflow name.
- Keep the parent workflow's assigned stage call separate from the internal stage's implementation narrative. An
  assignment such as `overlap = ScoreOverlap(...)` belongs in the parent workflow/result shape; the `ScoreOverlap`
  subsection must describe `ScoreOverlap` as a step transform, using the public `@step` methods declared by that class.
  Never replace that narrative with a synthetic method such as `score_overlap(...)`, and never present the assigned
  stage call as though it were one of the stage's step methods.
- Treat each stage subsection as a step-transform narrative, not as a method inventory. Partition the stage's public
  `@step` methods into adjacent, semantically coherent groups in source order. Give every group its own circled item,
  explain the data transition and responsibility in prose, and put that group's typed step signatures immediately
  beneath the explanation. Use named typed arguments and a return type, for example
  `tokenize(sentence: MaterializedSentence) -> LexicalOccurrence`.
- A group may contain several methods when they form one responsibility, such as stored and streamed candidate
  selection or parallel grain summaries. It must still show every method signature exactly once, directly under the
  group's explanation. A stage-level signature is not a substitute for its step methods. Step notation is reserved
  for step methods; do not present a step-method signature as if it were a stage transform.
- For every internal stage whose collected code includes a transform class, copy its complete public `@step` method
  coverage into those explanatory groups in the stage's Implementation subsection. Do not stop at the stage
  inputs/outputs or its compact transform shape; internal stages such as retrieval, fusion, and reranking must expose
  their individual methods through coherent narrative groups.
- Make each circled item correspond to one marker in the source notation or to a coherent group of lines carrying the
  same marker. Do not create unreferenced items or markers that have no explanatory item.
- End every stage subsection with a fenced `text` block containing that stage's compact notation; do not add an explicit
  `Notation` heading. Remove the circled reference markers from this compact stage block; the typed individual signatures
  remain under their explanatory items. Put the body-text line `Resulting transform shape:` immediately before the
  compact block so it cannot visually merge with the preceding individual step notation.
- After all stage subsections, add a `### Result` subsection with a fenced `text` block for the whole workflow transform
  only when an actual parent workflow transform exists and has child stages. A single transform with internal steps does
  not need a `Result` section. Do not invent a parent transform for a package that has only child transforms.
  Preserve workflow inputs, child-stage composition, and concrete output schemas in this result notation. Use the typed
  workflow format of `Indexing.comb.md`: list `inputs`, child transform assignments, and typed `outputs`. Do not repeat
  a stage's compact transform notation in `Result`; the parent workflow shape must be distinct.
- Refer to transforms, stages, and steps from the circled items. Consider joining cohesive notation lines when that
  keeps the correspondence clear, but retain every meaningful step in the stage and result notation.
  
Content style: 
- Problem section: no need to ground in previous steps. Ground in industry wisdom and project needs.
- Solution section: 
 - Ground in industry wisdom and project needs.
 - Include ample industry background as needed for the topic. Use formulas.
 - Make accessible for the person who gets familiar or refreshes the concepts.
 - Structure as an overview + proposal/description, rather than direction/report/achievement statements
 - Do not assume reader knows project specifics or project-specific terminology. Define/explain concepts.
 - 'Builds on', 'Used by' sections list top stages (Chunking, Fields) and top collections (Documents).
 - Notation: Must mention all input/output schemas, transform steps.
- Implementation section:
 - Body text other than bullet/numbered lists: prioritize thoughtful description/intent/proposal style 
over prescription/direction, gradually build understanding.
 - Make accessible for the first-time reader.  

Code section:
- Combine the above results with Collected doc (.cnd.md):
 - Include collected doc as Code section
 - Avoid small-info intro like 'The code below follows the declared workflow'.
 - Step methods:
   - If several step methods form a coherent group, consider grouping. In such case, step method
   paragraphs may be joined and harmonized, e.g. to avoid repetition.
   - Add global number in front of to step method paragraph, and include a short 'intent' sentence in italics.
   - The code must be preserved.

Formatting:
 - Formulas: use GitHub/Typora-compatible LaTeX, do not render formulas as inline code. 
 - Diagrams: GitHub/Typora-compatible mermaid, 
   - Monochrome diagrams only
 - Definitions: bold defined concept name
 - Inputs, Outputs, Stages sections: use bold, instead of inline code, for the class/schema/transform names. 

Finishing touches:
- Ensure continuous narrative from top to bottom, gradual buildup of concepts and understanding,
gradual introduction of technical details, no repetition, no technical overload while still preserving the goals:
 - Solution, Implementation sections should make interesting read
 - Industry wisdom/general considerations are concentrated on top (Solution section highest)
 - Implementation details can gradually grow (Code section highest)
 - Concepts are clarified before used

Avoid:
- Referring to other steps in text pipeline (e.g. 'The collected implementation') - these names/steps are internal use.
- Exhaustive comma-separated lists - use etc. as humans would.
- Corporate/bureaucratic/too-formal talk
- Do not use 'only' where can be omitted. Ex: 'Materialize sentences only for tokenization.'
- Overly complicated sentences that build up and read like mouthful.
  - Ex: 'Materialize source-faithful sentence content only in a private lane.'
- Drop negative/clarifying-by-exclusion phrases.
  - Phrases usually feature ', not' construct, followed by exclusions. 
  - Ex: 'The public index retains normalized evidence, not another copy of the source.'
  - Consider dropping or converting to positive phrase, ex:'The public index retains normalized evidence.' 
- Cut down on starting with negative statements
  - Ex: 'Search needs more than a match/no-match signal. Ranking and field constraints need normalized terms,'
    - Consider refactoring: 'Ranking and field constraints need normalized terms, because'

Quality assurance:
- Before publishing a combined document, verify that its H1 and every section before `## Implementation` are preserved
  from the source draft/combined structure.
- Verify every circled item is followed by its complete individual notation. A notation block must not begin or end with a
  continuation line torn from a neighboring item; check multiline calls and outputs as one unit.
- Verify every stage notation block contains all and only that stage's steps, in order, and that each return/output uses
  concrete schema classes.
- For every internal stage, distinguish its transform class from every parent-workflow assignment that invokes it. The
  stage subsection must explain the stage's own inputs, step transitions, and outputs; the parent assignment may appear
  only in the enclosing workflow/result notation. Reject a subsection whose apparent method is merely the assigned
  stage name in snake case, such as `score_overlap(...)` for `ScoreOverlap`, unless that exact method exists as a public
  `@step` in the collected class.
- Cross-check every method-looking name in an internal-stage subsection against the actual source class and the
  collected Code class's decorated `@step` definitions; when they disagree, the source class is authoritative. Reject
  stage calls, lane names, output aliases, or invented summaries presented as step methods. For every displayed step,
  verify the method name, argument names, argument types, and return type against source, not merely the schema set.
  Conversely, require the stage subsection's compact transform shape to reference the same actual step-method inventory
  used by its explanatory groups.
- If a real child class has no public `@step` methods because it is itself a workflow composition, describe its child
  assignments as a stage-transform/workflow shape and label them as composition. Do not invent a wrapper method to make
  the section look executable; the step-transform method rule applies whenever the class has actual decorated steps.
- Run this check as a document-wide stage audit, not as a spot check of the first or most visible internal stage:
  enumerate every real child transform class from `Stages` and `Code`, then perform the class-to-method comparison for
  each one. The audit fails if any one internal stage remains represented by a parent assignment, a stage-level call, or
  a synthetic method-shaped summary while another stage has been corrected.
- For every collected child stage, count the public `@step` methods in its source code and compare that inventory with
  the typed step signatures in the matching narrative groups. Reject a subsection that has no step signatures, has
  fewer signatures than the source, skips a method, changes method order, or includes a method from another stage.
- Treat a stage-level input/output signature and a compact `Resulting transform shape:` block as insufficient evidence
  of stage implementation coverage; both may be present only in addition to the complete grouped step narrative.
- Reject a standalone `Step methods:` inventory block or any equivalent dump of signatures that is not attached to
  explanatory circled items. Every signature must be traceable to exactly one group and its prose explanation.
- Verify every stage subsection title and compact notation name exactly matches a real transform class in the source;
  reject package names or invented workflow names when no such class exists.
- Verify every actual stage listed in `Stages` and every collected child stage that has its own transform class has an
  `Implementation` subsection. Verify step signatures remain under their owning stage and are not promoted to stage
  notation.
- For every stage subsection, classify the named class as either a step transform or a workflow composition before
  checking its notation. A step-transform subsection must contain that class's actual public `@step` methods; a workflow
  subsection must contain only that class's child-transform assignments and typed inputs/outputs. Reject a section that
  copies methods or assignments from a parent or sibling stage, even when the resulting schemas look plausible. For a
  workflow subsection, compare every assignment's left-hand alias, called child class, keyword arguments, and output
  reference with the source class; do not accept a shape merely because its child schemas are plausible.
- Apply the preceding ownership check to every stage subsection in the document, including the first, last, and nested
  internal stages. Do not stop after finding one valid subsection or after validating only the parent `Result` shape.
- Verify a package with no parent workflow class has no fabricated parent `Result` transform.
- Verify each compact transform notation block is introduced by `Resulting transform shape:` and that `Result` contains
  only a distinct typed parent workflow shape, never a repeated stage notation block.
- Verify the Implementation section has no duplicate parent workflow notation: one parent composition shape may appear in
  `Result`, while each internal stage has exactly one stage shape and no additional parent-shaped copy.
- Add a workflow `Result` section only when the transform composes child stages.
- For every `.form.md` counterpart, audit every formula block in the document, not only `Filtering` or `Scoring`:
  - Step methods must use the canonic step-method form from `prose/Notation.md`: argument names and separating colons
    are omitted, argument schema types remain, and every returned schema retains its name plus a field-name projection.
    Reject a bare return schema, a missing `return_schema_definitions` projection, or an invented `\vdots` projection when
    the source schema fields are available.
  - Use the single-argument form `\operatorname{method}(Type)` for one argument. The compact-space `\!` is permitted only
    before a multi-argument matrix or a stage-transform call; reject `\operatorname{method}\!(Type)`.
  - Keep formulas left anchored. Every `aligned` block must anchor its rows with `&`; do not use right-aligned display
    formulas or a leading unanchored continuation row. Separate consecutive standalone step formulas with a dedicated
    full `\\` row, and use short spacing only between methods inside a dense workflow method vector.
  - Escape every identifier underscore as `\\_` inside displayed formulas. A formula audit must reject any unescaped `_`,
    because subscripts are not part of Structure formula notation.
  - Each standalone `Resulting transform shape:` must be the canonic step-transform shape: transform name and colon,
    typed input vector on the left, method `Bmatrix` in the middle, `\rightarrow`, and output vector on the right. Do not
    replace that structure with a vertically stacked prose/list rendering or duplicate the transform name elsewhere.
  - A workflow `Result` must use Stage Transform Notation - Canonic: typed `name : Type` workflow inputs, assigned stage
    calls with name-only stage arguments, each stage's method vector and output vector, and typed final outputs without
    assignments. Omit the workflow name and do not repeat the `Resulting transform shape:` label in `Result`.
  - Check formula width and vertical spacing document-wide: do not insert threshold-based line breaks inside a step-method
    formula; keep its call, arrow, and return schema in one formula flow. Matrix components may use natural rows, while
    standalone methods remain visually separated and method vectors stay denser than workflow-stage gaps.
- Code section
  - Every collected transform/method section is represented.
  - Code listings order matches the collected source.
  - Root workflows include all child and external stages.
