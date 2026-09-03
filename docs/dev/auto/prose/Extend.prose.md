# Extend operator

## Shared Prose context
This chapter operator is governed by the common concepts and conventions in [Prose.md](../Prose.md). Read its
text-process model, and shared authoring guidance before applying this file.
Definitions: [Definitions](Definitions.prose.md)
Styles: 
- [Problem and Solution narrative style](Solution.style.md)
- [Implementation narrative style](Implementation.style.md)
- [General narrative style](General.style.md)

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
Output-shape template: [Extend.prose.temp.md](Extend.prose.temp.md). Read it with these instructions; it captures the
extended-document structure and the standalone, internal-only, and internal/external stage variants without replacing
the source and narrative rules below.

Extend draft (.draft.md), background (.back.md), plan (plan.md) and collected (.cnd.md) docs:
- Author Problem, Solution, and Implementation narrative anew from these current inputs and the shared style rules. Do
  not copy, paraphrase, or use sibling, archived, or variant outputs as narrative sources; preserve source-derived
  stage, notation, and code coverage as required by this operator.
- Draft (close/draft) contains a structured chapter (.draft.md) of the future user manual, including a substantive
  Solution narrative that answers the motivating use-case problem.
- Extend draft doc with background and collected docs to create an introduction narrative focused on a search-engine use
  case, such as building a system that retrieves useful passages from documents.
- Use relevant plan documents (.plan.md) for deep insight into decisions/tradeoffs/inner worsings. Use to extend the narrative without explicit importing parts of plan. Avoid citing/referring the plan docs.
- Maintain content and structure set by draft doc
 - Make improvements/corrections to draft as needed, but keep it brief/succinct where it is already
 - Specifically, some sections are mention/enumeration only: Builds on, Used by, Inputs, Outputs, and Stages.
 - Do not emit a top-level `## Notation` section in `.ext.md`; text notation belongs in the required Implementation
   stage and Result blocks.
 - Only include concepts under Definitions, concept name is mostly single-word.
 - Insert contents of background and collected docs as described below.
- Audience: technically confident reader may not be familiar with industry specifics, field terminology or what comprises the target system.
- Do not modify draft, background and collected docs (other than an update from source/annotated, if needed). We only produce the new output doc.
- A Draft may contain a Design section for source planning. Use its requirements to understand the draft when needed, but
  do not emit or proliferate a Design section in the extended document; keep design-specific detail in Implementation.

#### Extending Draft inputs with Background inputs

Problem section:
- Apply [Solution.style.md](Solution.style.md). Ground Problem in the search-engine use case and keep it focused on the
  user's need, topic-specific difficulty, and consequences; do not import the answer, solution mechanisms, transform
  responsibilities, or implementation requirements.
- Preserve a concise general-to-specific narrative that introduces only the concepts needed to make the use-case
  problem clear, then stop before explaining how the system solves it.
- Enrich the problem only when the background adds essential reader context; do not turn it into a problem inventory or
  implementation walkthrough.
- Also consider less-technical parts from 'How it works'/'Implementation' to go to the main section
- Use casual language, prioritize thoughtful explanation/intent over prescription/direction, gradually build understanding.
- Merge in the draft Solution only as the conceptual answer; do not let its algorithms, policies, transform duties, or
  implementation requirements leak back into Problem.

Solution section:
- Apply [Solution.style.md](Solution.style.md).
- Preserve the draft's substantive Solution narrative and enrich it with theory and practical search-engine context
  from the background where useful.
- Treat Solution as the conceptual answer to the stated use-case problem, rather than as a short summary of Background
  or a system-design section.
- Preserve the draft's concise general-to-specific progression from theory and practice to the central abstraction,
  behavior, and tradeoffs. Do not expand the section into a component inventory, transform walkthrough, or requirements
  list.
- Make Solution content available for first-time reader: more conceptual, easier on technical details (ok to mention code components).
- Include textbook-grade explanations as needed.
- Describe the answer directly; do not write “The solution is” or refer to the Solution section as a document part.
- Technical details, algorithm mechanics, policies, and transform responsibilities go to Design or Implementation.

Stages section:
- Transfer as is from input doc, apply formatting as described below.

Implementation section - narrative:
- Apply [Implementation.style.md](Implementation.style.md) to the narrative body, preamble, stage introductions,
  explanatory items, and external-stage explanations.
- 'How it works' section of the background doc gets extended with 'Implementation' section from the draft doc
- For 'How it works' section:
  - Drop implementation direction content such as discussion of invalid inputs, 'should'/'must' paragraphs
  - Drop content from decisions sections and on
- Preserve the complete extended document before `## Implementation`: retain the H1 and every preceding `Problem`,
  `Solution`, `Builds on`, `Used by`, `Definitions`, `Inputs`, `Outputs`, and `Stages` section. Never rebuild the output
  starting at `## Implementation`.

Implementation preamble:
- Apply the shared [Implementation narrative style](Implementation.style.md), especially its rules for general-to-specific
  progression, concise continuous prose, concept introduction, and boundary rationale.
- Allow occasional connective words such as “then” or “from there” when they clarify live data movement. Keep the
  preamble centered on purpose, concepts, and boundary rationale rather than turning it into a step-by-step account.
- For a composed workflow, identify the actual parent workflow and its actual stage flow in direct, active prose. State
  what enters the workflow, how the major data moves, and what boundary or policy makes the result reliable.
- For a standalone transform, identify the input evidence, the transformation it performs, and the observable output it
  enables. Use the exact transform name and avoid vague references to “the stage” or “the workflow.”

Content style:
 - Problem section: apply [Solution.style.md](Solution.style.md); ground it in industry wisdom and project needs, and
   keep proposed behavior and implementation requirements out.
 - Solution section:
  - Apply [Solution.style.md](Solution.style.md).
 - Ground in industry wisdom and project needs.
 - Include ample industry background as needed for the topic. Use formulas.
 - Make accessible for the person who gets familiar or refreshes the concepts.
 - Structure as an overview + proposal/description, rather than direction/report/achievement statements
 - Do not assume reader knows project specifics or project-specific terminology. Define/explain concepts.
 - 'Builds on', 'Used by' sections list top stages (Chunking, Fields) and top collections (Documents).
 - Notation: Must mention all input/output schemas, transform steps.
- Implementation section:
 - Apply [Implementation.style.md](Implementation.style.md) to body text, stage introductions, and explanatory items.
 - Make the implementation narrative accessible for the first-time reader.

#### Extending combined content with Collected document inputs

##### Code section - subsections

Code section:
- Extend the above results with Collected doc (.cnd.md):
 - Include collected doc as Code section
 - Do not add a scope or production line between the `Code` heading and its first subsection. If the `Workflow`
   subsection has no source-grounded description, add one concise sentence summarizing the workflow logic as its first
   prose line before the workflow listing.
  - Methods and method groups:
    - Identify coherent method groups from the collected source. A group begins with the a short italicized intent
      sentence and explanatory paragraph.
    - Implementation method groups contain public methods only. Preserve private/helper methods (including names that
      begin with `_`) in Code listings, but exclude them from numbered Implementation narratives and standalone
      transform shapes. If a collected group contains only private/helper methods, omit that Implementation group and
      do not invent replacement prose or consume a global number.
    - Add a global number (non-circled) in front of every public method-group clause. Begin each such clause with one
      short italicized intent, preserving the collected intent when present and deriving a concise source-backed intent
      from the group's plain explanation when needed. This Code-section sequence is independent of the circled
      Implementation sequence. Do not number workflow, class, stage-assignment, plain explanatory, or private/helper
      clauses.
    - The code must be preserved.
    - Render each collected intent/explanation exactly once in Code. Put it on the numbered group that owns its
      notation, immediately before the code listing or notation it explains, and remove any standalone or trailing copy of
      that same prose. Never emit a collected paragraph before the numbered item and again inside the item.
    - When a collected class or workflow listing has no source-backed intent, explanation, or class-level description,
      preserve the listing without inventing a numbered Code item. Keep class, workflow, and stage-assignment listings
      unnumbered even when they contain short docstrings. Public method groups always receive the short italicized intent
      and marker described above; private/helper listings remain unnumbered. Keep prose preceding class listings plain.
    - Do not take prose from Implementation section.

##### Implementation section - subsections
Extend implementation section with Stage subsections, numbered method groups and Result subsection. 
- Preserve content as created so far in 'Implementation section - narrative'. 
- Use this order inside the extended Implementation section:
  1. Conceptual/technical prose from the background and draft Implementation ('Implementation section - narrative');
  2. A subsection (a Stage subsection) for each workflow stage transform, including its explanatory items,
     individual typed step notation, and its final compact text notation;
  3. (only when workflow is a composed transform) a `Result` subsection containing the complete workflow notation.

Implementation section - Stage subsections:
- Identify the main/workflow transform from the collected source. 
 - The parent transform is represented by the `Result` subsection only; do not create a stage subsection or numbered step 
   narrative named after the parent/workflow transform. 
 - Implementation prose may explain the parent orchestration, but its assigned stage calls belong in the parent `Result` 
   notation, while numbered narratives and stage subsections belong only to child stage transforms.
 - Keep parent workflow's assigned stage calls separate from stage implementation narrative.
   Ex: An assignment such as `overlap = ScoreOverlap(...)` belongs in parent workflow/result shape; the `ScoreOverlap`
   subsection must describe `ScoreOverlap` as a standalone step transform, using the public methods declared by 
   that class. Never replace that narrative with a synthetic method such as `score_overlap(...)`, and never present the 
   assigned stage call as though it were one of the stage's step methods.
- Name each Stage subsection with its its transform class name and use that name in text notation.
  Derive the names from the source `Stages` inventory and the collected Code classes; never rename a child stage.
- Treat each Stage subsection as a step-transform narrative, not just as a method inventory. 
- Use [Implementation.style.md](Implementation.style.md) for the stage's introductory sentence and explanatory prose;
  keep minor field-level mechanics for the numbered method groups and Code section.

Step/helper method narrative/method groups:
- Partition transform's public methods into groups in source order, following the groupings from collected source.
  Code section's method-group boundaries and source order are binding for the Stage subsection.
- Mark each group with a global number (circled). Reuse upstream intent sentence and italic formatting when the
  upstream prose contains italicized intent.
- When a collected class listing contains a short class docstring, use that one-sentence docstring as the circled
  italicized intent for the class-level group. Keep any longer prose preceding the listing plain and use it as the
  group's explanation; never expand that prose into an italicized intent.
- Use collected source for group’s canonical group boundaries, intent sentence, and use Code section
  to verify method membership and order. Do not split, merge, reorder, or reassign a method to a different group.
- Do not invent an intent sentence when the upstream prose has none.
- Explanatory prose is the text that immediately follows the intent sentence.
- Give every group its own explanatory prose, explain data transition and responsibility in prose.
- Do not blind copy the collected source for group’s explanatory prose - instead, create
  explanation as part of implementation narrative, based on deep understanding of what the group step does,
  expressed according to [Implementation.style.md](Implementation.style.md) and accessible to a first-time reader.
- Put typed method signatures in a text notation block immediately following the explanation. 
- Use named typed arguments and a return type, for example `tokenize(sentence: MaterializedSentence) -> LexicalOccurrence`. 
- Do not place explanatory prose before intent sentence, and do not duplicate the intent or explanation as separate numbered items.
- A method group may contain several methods forming one responsibility, such as stored and streamed candidate
  selection or parallel grain summaries. Replace repeating parts of text notation with a text e.g.'Same for
  other grains', or similar (in body text, outside of the text notation fenced block). 
- Step notation is reserved for step methods. Do not present a step-method signature as if it were a composed transform.
- Refer to transforms, stages, and steps from the numbered items. Consider joining cohesive notation lines when that
  keeps the correspondence clear, but retain every meaningful step in the stage transform and result notations.
 
Internal vs external stages:
- Internal stages: 
 - For every internal stage whose collected code includes a transform class, copy its complete public method
  coverage into explanatory groups in the stage subsection. Do not stop at the stage inputs/outputs or its compact
  transform shape; internal stages must expose their individual methods through coherent narrative groups.
 - End every internal stage subsection with a fenced text block containing stage notation, preceded with
 `Resulting transform shape:` body-text line. Do not add an explicit `Notation` heading. 
 - Remove the circled reference markers, if any, from stage notation block; the typed individual 
  signatures remain under their explanatory items.
- External stages:
  - Keep one stage subsection per actual external stage call. Include at least one source-backed description line before
   its canonical stage-call notation block and number that description with the next global circled Implementation marker.
   The description need not be italicized and must not be presented as an invented intent sentence. For an external stage
   without its own collected class listing, inspect the parent workflow's stage context and the stage source description;
   keep the shortest accurate description, use active voice naturally, and do not invent implementation detail.
  - Do not include external stages' typed step methods, step groups, method inventory, or `Resulting transform shape:`.

Result subsection
- After all stage subsections, add a Result subsection with a fenced text notation block for the workflow transform,
  only when an actual parent workflow transform exists and is a composed transform. A single transform with internal
  steps does not need a Result section. Do not invent a parent transform for a package that has only child transforms.
  Confirm the exact parent class declaration and its composed stage assignments in the collected source before emitting
  `Result`; if no such class exists, omit `Result`, any parent-named Implementation subsection, and any package-level
  parent notation. This applies to aggregate topics such as Evaluation and Experiments.
  Preserve workflow inputs, stage calls and concrete output schemas in result notation. Use the typed
  workflow format of `Indexing.ext.md`: list typed `inputs`, child transform assignments whose stage arrows expose
  assigned output relation names, and typed final `outputs`. Do not repeat
  stage transform notations in Result section; the parent workflow shape must be distinct.
  Precede the notation with one self-contained narrative sentence that explains how the workflow combines its stages
  into the published result. Use the same active, concrete, data-transition style as stage explanations; avoid generic
  wording such as “The workflow composes …”.

#### Extend operator instructions - General tips

Formatting:
 - Formulas: use GitHub/Typora-compatible LaTeX, do not render formulas as inline code.
 - Diagrams: GitHub/Typora-compatible mermaid,
   - Monochrome diagrams only
 - Definitions: bold defined concept name
 - In `.ext.md`, keep Builds on, Used by, Inputs, Outputs, and Stages as plain list text without bold names.

Finishing touches:
- Ensure continuous narrative from top to bottom, gradual buildup of concepts and understanding,
gradual introduction of technical details, no repetition, no technical overload while still preserving the goals:
 - Solution, Implementation sections should make interesting read
 - Industry wisdom/general considerations are concentrated on top (Solution section highest)
 - Implementation details can gradually grow (Code section highest)
 - Concepts are clarified before used

Avoid:
- Referring to other steps in text pipeline (e.g. 'The collected implementation') - these names/steps are internal use.
- Do not expose document-production commentary in generated prose. In particular, do not say that a helper is omitted,
  remains in Code, is absent from a formula, or is handled by an operator/formatting stage.
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
- Do not mention numbered items, explanation prose, Code, Format, or other document-production structure in generated
  Problem, Solution, or Implementation narrative.

#### Extend operator instructions - Quality assurance
Quality assurance rules

General
- Before publishing an extended document, verify that its H1 and every section before Implementation section are preserved
  from the source draft/extended structure.
- Verify that Builds on, Used by, Inputs, Outputs, and Stages contain plain list text without bold markup.
- Verify that no top-level `## Notation` section appears in `.ext.md`.
- Normalize prose wrapping before publishing: continuation lines in ordinary paragraphs and numbered-item prose must start at
  column zero. Preserve indentation only inside fenced code, structured text notation, lists, and display math; reject runs of
  leading spaces that would render as literal whitespace in Typora.

Implementation section - subsections
  - Verify each Stage subsection:
    - Contains all and only that stage transform's public steps, in order, and that each return/output uses concrete schema
      classes. Cross-check the class declaration, including trailing output-publishing methods, against the collected
      method inventory so no public method is lost after the last collected group.
    - Inventory every actual child-stage assignment in the parent/workflow class, including stages whose transform class
      is outside the topic package. Require one subsection for every such external stage, with at least one source-backed
      description line, a global circled marker, and its canonical stage-call notation.
    - Is a child stage subsection, never a subsection named after the parent/workflow transform. Parent orchestration and
      its stage assignments belong in the `Result` subsection.
    - For internal stages, ends with `Resulting transform shape:` and canonical transform notation.
      For external stages, contains one canonical stage-call notation without typed step methods or a `Resulting
      transform shape:` block.
  - Verify each numbered item:
    - Cross-check Implementation item against its corresponding collected-source group.
    - Verify the upstream intent wording and formatting, followed by one concise narrative explanation that identifies
      the group's data transition and responsibility, uses active voice naturally, and does not copy Code prose verbatim.
    - Reject an Implementation item whose item has no matching collected-source group.
    - Verify every numbered item is followed by its complete individual notation. 
     - A notation block must not begin or end with a continuation line torn from a neighboring item; 
      check multiline calls and outputs as one unit.
  - Verify Result subsection:
    - Verify main/workflow transform, if any, has Result section with full notation.
    - Verify Result section is emitted only when the collected source contains the exact parent/workflow class 
    and composed stage assignments. For a topic package with only standalone or child transform classes, 
    reject Result section, a synthetic parent-named Implementation subsection, and any package-level parent shape.
    - For a composed workflow, reject any numbered implementation narrative, child-stage heading, or stage notation block
    that presents the parent/workflow transform as one of its own stages. 
    - The only workflow transform notation is the composed notation in Result section.
  - Overall
    - Verify no repeat/duplication of transform notations.

Code section
- Every collected transform/method section is represented.
- Verify that no scope or production line appears between the `Code` heading and its first subsection. If the `Workflow`
  subsection lacks source-grounded descriptive prose, verify that its first prose line is one concise sentence
  summarizing the workflow logic.
- Verify prose outside fenced blocks contains no literal escaped-backtick sequence (`\\``). When inline-code styling is
  not source-backed or needed for an identifier, emit ordinary prose instead.
- Code listings order matches the collected source.
- Every public method-group clause begins with one short italicized intent and its global numeric marker. Verify that the
  markers are visible in Code and continue one independent sequence across public method groups only; do not number
  workflow, class, stage-assignment, plain explanatory, or private/helper clauses. Do not require the numbers to match
  the circled Implementation groups, and never adjust Code numbering in response to Implementation content.
- Verify each numbered Code item appears immediately before the code listing or notation for its own group. Reject a
  numbered item placed after its corresponding listing, a numbered item with no source-backed Code group, or an invented
  numbered item for a listing whose collected source has no intent/explanation.
- Verify every pair of adjacent fenced Code listings is separated by at least one ordinary prose line. Reject a closing
  code fence immediately followed by an opening code fence, and do not use duplicated intent text or another code block as
  the separator.
- Cross-check every public step/helper in each Code class against corresponding Implementation method coverage and
  formulas, including trailing output-publishing methods; this is a method-coverage check, not a numbering rule. Reject
  any public method that appears in a class listing or transform shape without corresponding coverage and notation.
- Root workflows include all child and external stages.
- Method groups use the collected input as the sole source of method group breakdown and their intent, explanatory
  prose and and code listings.
- Emit group explanation exactly once with one leading group number; replace stale generated intent prose rather than
  prepending or wrapping it again. Reject duplicated sentences, adjacent italic spans such as `**`, and any group whose
  italicized intent does not match the collected source.
- Reject prose duplicated before the numbered item, a separate unnumbered explanation before the intent, 
  or an item containing only the intent with its explanation outside the item.

#### Extend operator instructions - Automation-generated
Additional instructions generated by automation

Stage subsections:
- For every internal stage, distinguish its transform class from every parent-workflow assignment that invokes it.
- The stage subsection must explain the stage transform inputs, step transitions, and outputs; no stage call may appear
  there. Reject a subsection whose apparent method is merely the assigned stage name in snake case,
  such as `score_overlap(...)` for `ScoreOverlap`, unless that exact method exists as a public `@step` in the collected class.
- Cross-check every method-looking name in an internal stage subsection against the collected source. Reject
  stage calls, lane names, output aliases, or invented summaries presented as step methods. For every displayed step,
  verify method name, argument names, argument types and return type against source, not merely the schema set.
- For parent workflow, audit every child stage, including imported or shared transforms: each stage must have its own
  stage subsection, internal stages have explanatory narrative and one complete Resulting transform shape with concrete
  inputs, methods or child stages, and outputs. 
- Derive the stage inventory from every actual transform call in the parent workflow source and Code, not from the draft
  `Stages` list alone. If a called class is outside the main/workflow package tree, require an external stage subsection
  for it even when the draft inventory omitted it; if it is inside the tree, require the corresponding internal narrative.
- If a subsection heading names multiple
  classes (for example, `A / B`), split it into subsection-per-class and give each class its own narrative and notation.
  Conversely, require the stage subsection transform notation to reference the same actual step-method inventory
  used by its explanatory groups.
- Verify every stage subsection title and notation name exactly matches a real transform class in the source;
  reject package names or invented workflow names or when no such class exists.
- Verify every stage listed in Stages section and every collected child stage that has its own stage subsection.
- Classify the transform class as either a step transform or composed transform before checking its notation. 
  A step-transform subsection must contain that class's actual public methods; a composed
  transform subsection must contain only that class's child-transform assignments and typed inputs/outputs. 
- Reject a section that copies methods or assignments from a parent or sibling stage, even when the resulting schemas look plausible.

External stages:
- For every external stage subsection, verify that the subsection names the called class exactly and contains one
  canonical stage-call notation. When matching source context exists, require one circled numbered item immediately
  before the notation; use the short italicized intent when present upstream, including a directly applicable parent
  workflow class docstring when the external stage has no collected class listing. Otherwise use the shortest plain
  source-backed sentence. Keep longer explanation plain and do not convert it to italics. When no matching source
  context exists, emit no number or invented prose. In either case, contain no typed step signature, step group, method
  vector, or `Resulting transform shape:` block. The parent workflow shows the stage assignment in its Result shape.

Stage transforms:
- If a stage transform is itself a composed transform, use composed transform notation with stage calls.
  Do not invent a wrapper method to make the section look executable: the step-transform method rule applies
  whenever the class is a genuine step transform.
- Run this check as a document-wide stage audit: enumerate every child transform class from `Stages` and `Code`,
  then perform the class-to-method comparison for each one. The audit fails if any internal stage remains represented
  by a parent assignment, a stage call, or a synthetic method-shaped summary while another stage has been corrected.
- Treat stage transform input/output signature and a `Resulting transform shape` notation as partial evidence;
  both may be present only in addition to the complete grouped step narrative.

Step/helper method narrative:
- Private/helper methods are Code-only evidence unless the collected source explicitly makes them public API. Do not
  include them in numbered Implementation groups, method narratives, or standalone transform shapes, and do not invent
  replacement intent or explanation for their omission.
- Reject Stage subsection if it includes a method from another stage.
- Reject a standalone `Step methods:` inventory block or any equivalent dump of signatures that is not attached to
  explanatory items. Every signature must be traceable to exactly one group and its prose explanation.
- Verify step signatures remain under their owning stage subsection and are not promoted to stage notation.
- Apply preceding ownership check to every stage subsection, including the first, last, and nested
  internal stages. Do not stop after finding one valid subsection or after validating only the `Result` shape.
- Preserve source-level raw/special helper discussion; those helpers remain represented in the extended narrative
  and in the corresponding formatted formulas, never as a reason to replace a complete transform narrative with a method summary.
- Include every `@raw` and `@special` method in its owning text step narrative and standalone transform
  notation, with its typed input and output signature.

Resulting transform shape:
- Verify every stage subsection has exactly one associated `Resulting transform shape:` block and that no
  child subsection borrows a shape from a sibling. 
- Verify each stage transform notation block is introduced by `Resulting transform shape:`.
- For every `Resulting transform shape:` block, verify the exact transform class name, canonical shape structure, and method
  inventory against the owning class. Reject a helper-shaped or stage-call-shaped block, such as `extract: Document ->
  Document`, in place of `Fields:` with its typed vectors and complete method signatures.
- For every standalone step transform, verify that its text `Resulting transform shape` contains the exact class name
  followed by one complete typed signature per public typed method, including non-step helpers, one signature per line. 
  Reject `methods:` summaries, abbreviated method-name lists, `inputs:`/`outputs:` summary blocks, and any signature not 
  traceable to that transform.
- When a source-backed method group names parallel grain paths, expand every named path into its own typed signature in
  the explanatory text notation and in the `Resulting transform shape`; do not collapse section, paragraph, or sentence
  methods into a representative document method or a prose-only “same pattern” statement.
- QA failure: when a source-backed method group names parallel grain paths or says that finer grains use the same formula,
  enumerate every named grain and verify one typed signature for each in the explanatory notation and one corresponding
  method name for each in the `Resulting transform shape`. Reject a representative document method, a prose-only “same
  pattern” statement, or a shape whose grain method inventory is shorter than the prose-described inventory.

Workflow transform:
- Parent Result shape must reproduce stage calls and typed outputs exactly;
  reject placeholders such as “grain terms,” omitted per-stage shapes, or a parent shape that merely repeats a child shape.
- Never use parent workflow name as a child stage's shape.
- For a workflow (Result) section, compare every assignment's left-hand alias, called child class, keyword arguments, and output
  reference with the source class; do not accept a shape merely because its child schemas are plausible.
- Verify a package with no parent workflow class has no fabricated parent `Result` transform.
- Verify that `Result` contains only a distinct typed parent workflow notation and no repeated stage notation block(s).
- Verify Implementation section has no duplicate parent workflow notations: one parent composition shape may appear in
  `Result`, while each internal stage has exactly one stage shape and no additional parent-shaped copy.
- The parent workflow shape may occur only in the `Result` section; reject a parent-shaped block after the last stage subsection. 
