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

## Definitions
Chapter operator: text operator related to creation of chapters in the prospected user manual.  
The operators defined in the rest of this document are chapter operators.

Chapter document: resulting document when a chapter operator is applied. 
Ex: Chunking.form.md (produced by Format operator).
Chapter document usually discusses one big transform, e.g. Chunking.

Main transform: the main transform of the chapter document.
Step method: a step method of a transform. Optionally decorated with @step in transform code.  
Step transform: a transform that consists of step methods (as opposed to composed transform).
Composed transform: a transform that consists of stages (other transforms) rather than step methods.
Stage: a stage of composed transform, usually defined as assignment of a Transform to a field in the composed transform.
Workflow transform: the main transform which is simultaneously is a composed transform. 
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
  - Low-level headers: convert section headings to italicized intent-setting sentence at the beginning of section paragraph.
    - Ex: 'Select fallback options' heading converted to italicized 'Select fallback options. ' added in front of section paragraph.
  - Avoid merging code listings - maintain a text sentence in between.
  - Drop import statements.
  - Workflow transform is the main transform in a package - a composed transform that rules other transforms in the package.   
  Usually, the workflow transform is alphabetically the last file in dir (Ex: 'SearchDocuments'). Not all dirs contain the workflow transform. 
    - Top header: as-is, do not convert to gerund
    - Replace subsections as described above
    - Move workflow class listing from intro section into a new section, 'Workflow'
    - The `Workflow` section thus contains the parent workflow class listing plus all former sections, except the intro
      section. It must retain every stage call in execution order, including calls to stages defined outside the workflow
      directory, so the complete parent orchestration is readable in one place.
    - Append workflow document with the content (.cnd.md) of stage transforms as additional sections to form continuous narrative.
      - Order stage transform sections according to their stage's order in the workflow transform.
      - Convert the headers of the appended content to one level lower, to maintain header structure.
      - Do not remove any headers: former top-level headers become section headers. 
    - Treat the workflow directory as a package tree: inspect sibling source files and subpackages,
      - For internal stages, include the annotated source for the stage transform. If one source file defines 
      multiple transform classes, include each class separately in workflow order.
    - Some stage transforms may be defined outside of workflow dir.
      - Internal stages: include them into main workflow doc as stage transforms, as described above.
      - External stages (stages defined outside the workflow dir):
        - Include a section heading and brief description, ", as described in 'doc_header'".
        - Keep the full stage call in the parent `Workflow` section and include the complete parameterized stage call in
          the external stage section as well, so the external reference is self-contained.
        - Describe the external stage in prose without repeating the assignment inline; the fenced listing is the single
          external reference for that call.
        - The intentional duplication is limited to stage assignment; never duplicate parent workflow class
          listing or a child transform narrative in external stage section.
        - For example, if the workflow class contains `features = Features(...)`, the external stage section contains
          the complete `features = Features(...)` assignment, not a second `class Training(Transform):` listing.
    - Quality assurance:
      - Verify transform sections follow the order of main transform stages, with no stages missing.
      - Verify the package stage inventory is complete: every same-package stage class from the workflow has a 
      corresponding section in the collected output.
      - Verify every internal stage section contains its complete class declaration and all annotated step code,
        including files that define more than one transform class.
      - Verify the complete parent orchestration, including every external stage call, appears in `Workflow` in
        execution order.
      - Verify every external stage section contains the complete parameterized stage call and that the parent Workflow
        also contains the call in execution order.
      - Verify parent workflow class listings and child transform narratives are not duplicated as external listings.
      - Verify each output has exactly one top-level header, and that its first nonblank line is that header.
      - Verify each workflow output has exactly one parent `Workflow` section before inserted child sections.
      - For transform steps, verify code listing is present in each transform step section.
    - Since, as result of content inclusion, stage sections carry more detail, the texts between code listings in the
    'Workflow' section may become redundant/duplicating.
      - Trim texts between code listings in the 'Workflow' to avoid repetition/duplication.

## Extend
Present extended continuous narrative based on draft, background and collected documents.

### Extend process
- Inputs: close/draft, close/collected, .back.md
- Output: close/extended
- Scope: close/collected/search
- Name: Extend. Usage: Extend(dir)
- Invocation: manual

### Extend operator
- Name: extend(), usage: extend(dir)
- Input: .draft.md, .back.md and .cnd.md describing a transform. Ex: chunking.draft.md
- Output: .ext.md describing a transform in extended narrative. Ex: chunking.ext.md
- Goal: present extended continuous narrative based on draft, background, planning and collected documents.
 
### Extend operator instructions
Extend draft (.draft.md), background (.back.md), plan (plan.md) and collected (.cnd.md) docs:
- Draft (close/draft) contains structured chapter (.draft.md) of the future user manual, including a substantive Solution narrative.
- Extend draft doc with background and collected docs to create an introduction narrative focused on a search engine topic, such as 'chunking'
- Use relevant plan documents (.plan.md) for deep insight into decisions/tradeoffs/inner worsings. Use to extend the narrative without explicit importing parts of plan. Avoid citing/referring the plan docs.
- Maintain content and structure set by draft doc
 - Make improvements/corrections to draft as needed, but keep it brief/succinct where it is already 
  - Specifically, some sections are mention/enumeration only: Builds on, Used by, Inputs/Outpus/Stages, Notation
 - Only include concepts under Definitions, concept name is mostly single-word.    
 - Insert contents of background and collected docs as described below.
- Audience: technically confident reader may not be familiar with industry specifics, field terminology or what comprises the target system.
- Do not modify draft, background and collected docs (other than an update from source/annotated, if needed). We only produce the new output doc.

Extending Draft with Background docs:

'Solution' section:
- Preserve the draft's substantive Solution narrative and enrich it with the background document where useful.
- Treat Solution as the conceptual center of the extended document, rather than as a short summary of Background.
- Make Solution content available for first-time reader: more conceptual, easier on technical details (ok to mention code components).
- Include textbook-grade explanations as needed.
- Technical details go to other sections, e.g. Implementation
- Also consider less-technical parts from 'How it works'/'Implementation' to go to the main section
- Use casual language, prioritize thoughtful explanation/intent over prescription/direction, gradually build understanding.
- Merge-in Solution section from draft doc if not already covered.

'Stages' section:
- Transfer as is from input doc, apply formatting as described below.

Implementation section:
- 'How it works' section of the background doc gets extended with 'Implementation' section from the draft doc
- For 'How it works' section:
  - Drop implementation direction content such as discussion of invalid inputs, 'should'/'must' paragraphs
  - Drop content from decisions sections and on
- Preserve the substantive Implementation prose from the draft before the workflow notation. Do not replace those
  paragraphs with a list or hide them after the notation.
- Preserve the complete extended document before `## Implementation`: retain the H1 and every preceding `Problem`,
  `Solution`, `Builds on`, `Used by`, `Definitions`, `Inputs`, `Outputs`, and `Stages` section. Never rebuild the output
  starting at `## Implementation`.
- Use this exact order inside the extended `Implementation` section:
  1. the extended conceptual/technical prose from the background and draft Implementation;
  2. one subsection (a Stage subsection) for each workflow stage transform, including its explanatory items, 
     individual typed step notation, and its final compact text notation;
  3. (only when workflow is a composed transform) a `Result` subsection containing the complete workflow notation.
- Identify the parent/workflow transform from the collected source before creating Implementation subsections. The parent
  transform is represented by the `Result` subsection only; do not create a stage subsection or numbered step narrative
  named after the parent/workflow transform. Implementation prose may explain the parent orchestration, but its assigned
  stage calls belong in the parent `Result` notation, while numbered narratives and stage subsections belong only to child
  stage transforms.
- In each stage subsection, explain stage flow with numbered items. Each item must begin with one short italicized intent
    sentence, immediately followed by the explanatory prose for that same step. Do not place explanatory prose before the
    intent sentence, and do not duplicate the intent or explanation as separate numbered items.
- Name each stage subsection with the its exact transform class name and use that class name in text notation. 
  Derive the names from the source `Stages` inventory and the collected Code classes; never rename a
  child stage to the package or parent workflow name.
- Keep parent workflow's assigned stage call separate from stage implementation narrative.
  Ex: An assignment such as `overlap = ScoreOverlap(...)` belongs in parent workflow/result shape; the `ScoreOverlap`
  subsection must describe `ScoreOverlap` as a step transform, using the public `@step` methods declared by that class.
  Never replace that narrative with a synthetic method such as `score_overlap(...)`, and never present the assigned
  stage call as though it were one of the stage's step methods.
- Treat each stage subsection as a step-transform narrative, not as a method inventory. Partition stage transform public
  `@step` methods into adjacent, semantically coherent groups in source order. Give every group its own explanatory item,
  explain data transition and responsibility in prose, and put that group's typed step signatures immediately
  beneath the explanation. Use named typed arguments and a return type, for example
  `tokenize(sentence: MaterializedSentence) -> LexicalOccurrence`.
- A step method group may contain several methods when they form one responsibility, such as stored and streamed candidate
  selection or parallel grain summaries. Replace repeating parts of text notation with a text e.g.'Same for
  other grains', or similar (in body text, outside of text notation fenced block). A stage-level signature is not a substitute for
  its step methods. Step notation is reserved for step methods; do not present a step-method signature as if it were a composed transform.
- For every internal stage whose collected code includes a transform class, copy its complete public `@step` method
  coverage into explanatory groups in the stage subsection. Do not stop at the stage inputs/outputs or its compact
  transform shape; internal stages must expose their individual methods through coherent narrative groups.
- End every internal stage subsection with a fenced text block containing stage notation; do not add an
  explicit `Notation` heading. Remove the circled reference markers from stage notation block; the typed individual
  signatures remain under their explanatory items. Put body-text line `Resulting transform shape:` immediately before
  the notation block so it cannot visually merge with the preceding individual step notation. For an external stage,
  keep one stage subsection per actual external stage call, with one short numbered intent and one canonical stage-call
  notation block; do not include its typed step methods, step groups, method inventory, or `Resulting transform shape:`.
- After all stage subsections, add a Result subsection with a fenced text notation block for the workflow transform,
  only when an actual parent workflow transform exists and is a composed transform. A single transform with internal 
  steps does not need a Result section. Do not invent a parent transform for a package that has only child transforms.
  Confirm the exact parent class declaration and its composed stage assignments in the collected source before emitting
  `Result`; if no such class exists, omit `Result`, any parent-named Implementation subsection, and any package-level
  parent notation. This applies to aggregate topics such as Evaluation and Experiments.
  Preserve workflow inputs, stage calls and concrete output schemas in result notation. Use the typed
  workflow format of `Indexing.ext.md`: list `inputs`, child transform assignments, and typed `outputs`. Do not repeat
  stage transform notations in Result section; the parent workflow shape must be distinct.
- Refer to transforms, stages, and steps from the numbered items. Consider joining cohesive notation lines when that
  keeps the correspondence clear, but retain every meaningful step in the stage transform and result notations.
  
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
- Extend the above results with Collected doc (.cnd.md):
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

#### Extend operator instructions - Quality assurance
Quality assurance rules

General
- Before publishing an extended document, verify that its H1 and every section before Implementation section are preserved
  from the source draft/extended structure.
- Normalize prose wrapping before publishing: continuation lines in ordinary paragraphs and numbered-item prose must start at
  column zero. Preserve indentation only inside fenced code, structured text notation, lists, and display math; reject runs of
  leading spaces that would render as literal whitespace in Typora.
- In every numbered implementation item, place exactly one short italicized intent sentence first, followed immediately by
  the explanatory prose for that same step. Reject prose duplicated before the item, a separate unnumbered explanation before
  the intent, or an item containing only the intent with its explanation outside the item.

Implementation section - main body
- Verify every numbered item is followed by its complete individual notation. A notation block must not begin or end with a
  continuation line torn from a neighboring item; check multiline calls and outputs as one unit.

Implementation section - stage subsections 
  - Verify every stage subsection
    - Contains all and only that stage transform steps, in order, and that each return/output uses concrete schema classes.
    - Is a child stage subsection, never a subsection named after the parent/workflow transform. Parent orchestration and
      its stage assignments belong only in the `Result` subsection.
    - For internal stages, ends with `Resulting transform shape:` and canonical transform notation. 
      For external stages, contains one numbered intent and one canonical stage-call notation without typed step methods
      or a `Resulting transform shape:` block.
  - Verify main/workflow transform, if any, has Result section with full notation.
  - Verify `Result` is emitted only when the collected source contains the exact parent/workflow class and composed stage
    assignments. For a topic package with only standalone or child transform classes, reject `Result`, a synthetic
    parent-named Implementation subsection, and any package-level parent shape.
  - For a composed workflow, reject any numbered implementation narrative, child-stage heading, or stage notation block
    that presents the parent/workflow transform as one of its own stages. The only parent transform notation is the
    composed notation in `Result`; the parent may be mentioned in surrounding conceptual prose.
  - Verify no repeat/duplication of transform notations.

Code section
- Every collected transform/method section is represented.
- Code listings order matches the collected source.
- Root workflows include all child and external stages.

Automation:
- For every internal stage, distinguish its transform class from every parent-workflow assignment that invokes it. 
- The stage subsection must explain the stage transform inputs, step transitions, and outputs; no stage call may appear
  there. Reject a subsection whose apparent method is merely the assigned stage name in snake case, 
  such as `score_overlap(...)` for `ScoreOverlap`, unless that exact method exists as a public `@step` in the collected class.
- Cross-check every method-looking name in an internal stage subsection against the actual source class and the
  collected Code class's decorated step methods; when they disagree, the source class is authoritative. Reject
  stage calls, lane names, output aliases, or invented summaries presented as step methods. For every displayed step,
  verify the method name, argument names, argument types, and return type against source, not merely the schema set.
- For parent workflow, audit every child stage, including imported or shared transforms: each stage must have its own
  stage subsection, internal stages have explanatory narrative and one complete Resulting transform shape with concrete
  inputs, methods or child stages, and outputs. Parent Result shape must reproduce stage calls and typed outputs exactly; 
  reject placeholders such as “grain terms,” omitted per-stage shapes, or a parent shape that merely repeats a child shape.
- Derive the stage inventory from every actual transform call in the parent workflow source and Code, not from the draft
  `Stages` list alone. If a called class is outside the main/workflow package tree, require an external stage subsection
  for it even when the draft inventory omitted it; if it is inside the tree, require the corresponding internal narrative.
- For every external stage subsection, verify that the subsection names the called class exactly, contains one concise
  numbered intent and one canonical stage-call notation, and contains no typed step signature, step group, method vector,
  or `Resulting transform shape:` block. The parent workflow may repeat the actual stage assignment in its Result shape.
- Never use parent workflow name as a child stage's shape. If a subsection heading names multiple
  classes (for example, `A / B`), split it into subsection-per-class and give each class its own narrative and notation.
  Conversely, require the stage subsection transform notation to reference the same actual step-method inventory
  used by its explanatory groups.
- If a stage transform is itself a composed transform, use composed transform notation with stage calls. 
  Do not invent a wrapper method to make the section look executable: the step-transform method rule applies 
  whenever the class is a genuine step transform.
- Run this check as a document-wide stage audit: enumerate every child transform class from `Stages` and `Code`, 
  then perform the class-to-method comparison for each one. The audit fails if any one internal stage remains represented 
  by a parent assignment, a stage call, or a synthetic method-shaped summary while another stage has been corrected.
- Reject a subsection if it includes a method from another stage.
- Treat stage transform input/output signature and a `Resulting transform shape` notation as partial evidence
  of stage implementation; both may be present only in addition to the complete grouped step narrative.
- Reject a standalone `Step methods:` inventory block or any equivalent dump of signatures that is not attached to
  explanatory items. Every signature must be traceable to exactly one group and its prose explanation.
- Verify every stage subsection title and notation name exactly matches a real transform class in the source;
  reject package names or invented workflow names or when no such class exists.
- Verify every stage listed in Stages section and every collected child stage that has its own stage subsection. 
- Verify step signatures remain under their owning stage subsection and are not promoted to stage notation.
- For every stage subsection, classify the named class as either a step transform or composed transform before
  checking its notation. A step-transform subsection must contain that class's actual public `@step` methods; a composed
  transform subsection must contain only that class's child-transform assignments and typed inputs/outputs. Reject a 
  section that copies methods or assignments from a parent or sibling stage, even when the resulting schemas look plausible. 
- For a workflow (Result) section, compare every assignment's left-hand alias, called child class, keyword arguments, and output
  reference with the source class; do not accept a shape merely because its child schemas are plausible.
- Apply preceding ownership check to every stage subsection, including the first, last, and nested
  internal stages. Do not stop after finding one valid subsection or after validating only the `Result` shape.
- Verify a package with no parent workflow class has no fabricated parent `Result` transform.
- Verify each stage transform notation block is introduced by `Resulting transform shape:` and that `Result` contains
  only a distinct typed parent workflow notation and no repeated stage notation block(s).
- Verify every stage subsection has exactly one associated `Resulting transform shape:` block and that no
  child subsection borrows a shape from a sibling. The parent workflow shape may occur only in
  the `Result` section; reject a parent-shaped block after the last stage subsection.
- Verify Implementation section has no duplicate parent workflow notations: one parent composition shape may appear in
  `Result`, while each internal stage has exactly one stage shape and no additional parent-shaped copy.
- For every `Resulting transform shape:` block, verify the exact transform class name, canonical shape structure, and method
  inventory against the owning class. Reject a helper-shaped or stage-call-shaped block, such as `extract: Document ->
  Document`, in place of `ExtractDocumentFields:` with its typed vectors and complete method signatures.
- For every standalone step transform, verify that its text `Resulting transform shape` contains the exact class name
  followed by one complete typed signature per public step, one signature per line. Reject `methods:` summaries,
  abbreviated method-name lists, `inputs:`/`outputs:` summary blocks, and any signature not traceable to that transform.
- Extend preserves source-level raw/special helper discussion; raw/special omission is a Format-only rule applied while
  producing formula notation, never a reason to replace a complete extended transform narrative with a method summary.
- In Extend, include every `@raw` and `@special` method in its owning text step narrative and standalone transform
  notation, with its typed input and output signature. Format alone omits those methods from formula notation.

## Format
Create formatted documents (.form.md), based on extended documents (.ext.md).

### Format process
- Inputs: close/extended, .ext.md
- Output: close/form
- Scope: close/extended/search
- Name: Format. Usage: Format(dir)
- Invocation: manual

### Format operator
- Name: format(), usage: format(dir)
- Input: .ext.md
- Output: .form.md
- Goal: create formatted documents based on extended documents.
 
### Format operator instructions
Structure Formula Notation: see [Notation.md](prose/Notation.md)

## Format operator instructions - General rules
When applying Structure Formula Notation:

Step method:
- Use the canonic step-method form: 
  - omit method argument names and separating colons while retaining argument schema
    types; 
  - omit field type annotations in the result schema definitions, while retaining its schema name and field names.
- Use 'Schema Notation - With Projection' notation for return schemas: 
  - when source code returns Schema.project(...), Schema.base(...), or a projected call with added fields, 
  use : show \\vdots for inherited/projected fields and list only fields introduced by that return expression.
  - Omit return schema definition (colon and vector), and only show return schema name(s), if:
    - If return schema definition for the schema is already shown in preceding formulas of same document.
    - If return schema is same as one of the argument schemas.
    - If the only content of return schema definition vector is lone ellipses (\\vdots).
  - Maintain a document-wide set of emitted schema definitions while formatting, including definitions emitted in
    other stage subsections. Once a schema's complete field vector has appeared, later returns of that same schema
    use only the schema name. Emit a return definition again only when the returned field set is genuinely different,
    such as a distinct projection.
- Keep short step-method formulas in one formula flow. Use 140 characters as the soft wrapping limit for the longest
  rendered arrow-bearing row, not for the sum of vertically stacked matrix rows. Wrap once at the arrow only when that
  row reaches the limit or visibly exceeds the viewer's content width because of long identifiers. Do not split the
  method call or return schema internally.

Step methods:
- Separate consecutive standalone step method formulas with two consecutive dedicated full lines (\\) rather than
  adjusting line height of the leading formula's final row;

Special/opaque methods:
- Omit opaque helpers such as `@special`, and omit `@raw` methods only when they are implementation helpers without an
  explicit typed step notation in Extend. Preserve a typed `@raw` method that Extend documents as a step,
  in its step formula and transform method vectors.

Transform - standalone (as in 'Resulting transform shape' sections of .form.md docs):
- Use the canonic transform notation: show transform name and colon, place input vector on the left, 
  the step-method vector in the middle, and the output vector on the  right. Omit \odot.
- Put exactly one schema type on each input and output vector row; never collapse multiple comma-separated types into one
  row, even when several methods consume or produce the same relation set.
- Preserve transform name in formula, including workflow stages such as Features; 
  omit a name only in workflow Result formula when the surrounding prose already names it.
- In the canonic transform shape, the middle method vector contains operator names only. Do not place argument types,
  argument vectors, arrows, return schemas, or full step signatures inside that vector; those belong in the explanatory
  step formulas.
- Define external stages by source package: a stage is external when its transform class is outside the package tree rooted at
  the main/workflow transform; imports alone do not make a same-package or child-package stage external. For an external
  stage whose implementation and step methods are not discussed in the document, keep one canonical standalone transform
  notation: transform name followed by colon, schema types with omitted input/output names, and no `Resulting transform
  shape:` label or block. Internal stages retain their step methods and one `Resulting transform shape:` block.

Stage call:
- Use 'Stage Call Notation' for stages in a workflow. 
- In a composed-transform shape, retain one `name : Type` pair per input/output row and include the assigned stage's
  canonic method vector; do not substitute the child transform's method signatures for the stage call.
 
Main Transform - e.g. workflow transform in Result section of .form.md docs
- Omit transform name and colon.
- Show workflow inputs and final workflow outputs as name : Type pairs without value assignments.
- Use a smaller gap such as \\[2pt] between methods inside a dense workflow method vector.
- Add two dedicated full \\ rows between workflow inputs, each stage and final outputs so adjacent vectors do not
  visually merge.
- Apply the same two-row separation to every composed-transform formula, including a stage subsection's
  `Resulting transform shape:` and its multi-stage explanatory flow; do not let adjacent stage calls share a visual row.

Additional Rules
- Keep the Inputs, Outputs, and Stages sections in their source text form; formulas are applied to the individual
  step methods, standalone transforms, and the workflow transform.
- Do not repeat the 'Resulting transform shape': label in the workflow's Result section when the
  transform notation is already shown in the preceding sections.

Quality assurance:
  - For every `.form.md` output, audit every formula block in the document.
  - Outside fenced code, structured text notation, and display math, require ordinary paragraph and numbered-item
    continuation lines to start at column zero. Reject indentation introduced only by wrapping prose, because Typora
    renders those leading spaces as visible whitespace.
  - Verify every display formula has balanced, properly nested `\\begin{...}`/`\\end{...}` environments. Reject an
    unclosed nested `aligned`, `gathered`, `pmatrix`, or `Bmatrix` environment, including when one is embedded in another.
  - Step methods must use the canonic step method notation from `prose/Notation.md`: argument names and separating colons
    are omitted, argument schema types remain, and every returned schema retains its name plus a field-name projection.
    Reject a bare return schema, lone ellipses (\\vdots) return schema, a missing `return_schema_definitions` projection,
    or an invented `\vdots` projection when the source schema fields are available.
  - Use the single-argument form `\operatorname{method}(Type)` for exactly one argument. Never render a one-argument
    matrix, and never use `\!` before its parentheses. The compact-space `\!` is permitted only before a multi-argument
    matrix or a stage call; reject `\operatorname{method}\!(Type)` and any one-argument `pmatrix`.
  - Keep formulas left anchored. Every `aligned` block must anchor its rows with `&`; do not use right-aligned display
    formulas or a leading unanchored continuation row. Separate consecutive standalone step formulas with two dedicated
    full `\\` rows, and use short spacing only between methods inside a dense workflow method vector.
  - Escape every identifier underscore as `\\_` inside displayed formulas. A formula audit must reject any unescaped `_`,
    because subscripts are not part of Structure formula notation.
  - Each standalone `Resulting transform shape:` must be the canonic step-transform shape: transform name and colon,
    typed input vector on the left, method `Bmatrix` in the middle, `\rightarrow`, and output vector on the right. Do not
    replace that structure with a vertically stacked prose/list rendering or duplicate the transform name elsewhere.
  - In every canonic transform method `Bmatrix`, each entry must be an operator name only. Reject argument parentheses,
    argument matrices or types, return arrows, return-schema definitions, and any other full step-method notation inside
    the transform vector; full signatures belong only to the explanatory step formulas.
  - In every canonic standalone transform input and output vector, put exactly one schema type on each row. Reject
    comma-separated type lists collapsed into one matrix row; multiple inputs or outputs must occupy separate rows.
  - Whenever a step method, stage call, or other formula returns multiple schemas, represent the return schemas as one
    output `pmatrix` with exactly one schema per row. Reject comma-separated return lists, including lists in abstract or
    variant stage formulas.
  - Apply the raw/special omission rule to every formula context, including method vectors nested inside composed-transform
    stage calls. Reject opaque `@special` helpers and undocumented `@raw` helpers in formulas; a typed `@raw` step
    explicitly documented by Extend must remain present in its corresponding formula contexts.
  - For every typed step signature in an extended explanatory item, require exactly one corresponding step formula and
    require its method name in the owning standalone transform shape and any composed-transform stage method vector.
  - Use the GitHub/Typora-compatible `\operatorname{...}` command for every displayed operator. Reject the invalid
    `\operator{...}` form, raw text such as `extract: Document -> Document`, and any standalone shape whose name is not the
    exact source transform class.
  - A workflow Result section must use canonic 'Composed Transform Notation': typed `name : Type` workflow inputs, assigned stage
    calls with name-only stage arguments, each stage's method vector and output vector, and typed final outputs without
    assignments. Omit the workflow name and do not repeat the `Resulting transform shape:` label in `Result`.
  - For a composed root transform, use a `### Result` section for the parent composition; never emit the parent as an
    additional `Resulting transform shape:` block. Reserve that label for internal standalone stage shapes.
  - Emit `### Result` only after verifying an exact parent/workflow class with composed stage assignments in the collected
    Code. If the topic contains no such class, reject any Result section, parent-named implementation narrative, or
    invented package-level transform notation.
  - Classify a named shape as standalone or composed from the source transform before formatting. A composed transform must
    use `Composed Transform Notation - With name`: `TransformClassName :`, typed input/output name pairs, and assigned stage
    calls with their canonic stage method vectors. Never render a composed transform as a standalone step-transform shape.
  - Check formula width and vertical spacing document-wide: keep short step-method formulas in one flow. Apply 140
    character threshold to the longest rendered arrow-bearing row, not to the aggregate source length of a formula with
    vertical matrices. Also inspect rendered width for long identifiers; wrap any row that still exceeds the viewer
    content width once at the arrow. Never split the method call or return schema internally. Matrix components may use
    natural rows, while standalone methods remain visually separated and method vectors stay denser than stage call gaps
    of a composed transform.
  - For every `gathered` block containing consecutive standalone step-method formulas, require exactly two dedicated full
    `\\` rows between adjacent formulas. Count only full rows outside any `Bmatrix` method vector; reject zero or one
    separator rows and reject artificial `\\[12pt]` spacing used in their place.
  - For the workflow formula in Result section, require exactly two dedicated full `\\` rows between the typed input
    vector, each assigned stage, and the typed final-output vector. Reject a directly adjacent row or a single separator row;
    the check must cover every workflow document, not only `SearchDocuments`.
  - For every composed-transform formula outside `Result`, including internal stage shapes and explanatory stage flows,
    require exactly two dedicated full `\\` rows between adjacent stage calls and between the input/output vectors and
    neighboring stage calls. Reject directly adjacent calls or a single separator row.
  - After applying the 140-character rule, reject any arrow-centered `aligned` split whose normalized call-plus-arrow row is
    at most 140 characters. The height of a return matrix must never trigger wrapping of an otherwise viewer-safe method.
  - Preserve exact transform class name in every 'Resulting transform shape' formula, including workflow stages such as Features. 
    The main transform Result section notation omits its transform name when the prose already identifies it.
  - Inspect each source return expression for .project() and .base(). Use 'Schema Notation - With Projection' in the matching
    step formula: retain return schema name, show \vdots for inherited/projected fields, and list only fields introduced
    by the projection call.
  - Track emitted schema field vectors across the entire document, including all stage subsections. After a schema's
    complete definition has been shown once, reject later identical return-schema vectors and require the schema name
    alone. A repeated schema may show a definition only when its fields differ because of a distinct projection.
  - Verify every arrow-bearing row at or above 140 characters, and every row that visibly exceeds the viewer content
    width, has at most one arrow-centered wrap. Do not wrap a formula merely because its vertically stacked matrix rows
    make the aggregate source block longer; verify shorter, viewer-safe formulas have no artificial wrap.
  - Classify every stage by source package before formatting: only a transform outside main transform package tree is
    external. External stages may have one transform formula with a colon and unnamed schema vectors, but must
    not have a `Resulting transform shape:` block when their step methods are not discussed. Internal stages must
    retain their documented methods and exactly one `Resulting transform shape:` block.
  - Verify every standalone transform notation names its exact transform class, composed transforms have visible 
    spacing between input, stage, and output rows, and consecutive standalone step formulas contain two dedicated
    full \\ separator rows.

