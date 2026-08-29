# Extend operator

## Shared Prose context
This chapter operator is governed by the common concepts and conventions in [Prose.md](../Prose.md). Read its
[Definitions](../Prose.md#definitions), text-process model, and shared authoring guidance before applying this file.

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
- Name each stage subsection with its exact transform class name and use that class name in text notation.
  Derive the names from the source `Stages` inventory and the collected Code classes; never rename a
  child stage to the package or parent workflow name.
- Keep parent workflow's assigned stage call separate from stage implementation narrative.
  Ex: An assignment such as `overlap = ScoreOverlap(...)` belongs in parent workflow/result shape; the `ScoreOverlap`
  subsection must describe `ScoreOverlap` as a step transform, using the public `@step` methods declared by that class.
  Never replace that narrative with a synthetic method such as `score_overlap(...)`, and never present the assigned
  stage call as though it were one of the stage's step methods.
- Treat each Stage subsection as a step-transform narrative, not as a method inventory. 
  Partition transform's public methods into adjacent, semantically coherent groups in source order (can use groupings
  from Code section as guidance).
  Mark each group with a global number (circled), one short intent sentence followed by explanatory sentence(s),
  and put group's typed method signatures in text notation block immediately following the explanation. 
  Use named typed arguments and a return type, for example `tokenize(sentence: MaterializedSentence) -> LexicalOccurrence`.
  Do not place explanatory prose before intent sentence, and do not duplicate the intent or explanation as separate numbered items.
- A method group may contain several methods when they form one responsibility, such as stored and streamed candidate
  selection or parallel grain summaries. Replace repeating parts of text notation with a text e.g.'Same for
  other grains', or similar (in body text, outside of the text notation fenced block). A stage-level signature is not a
  substitute for its step methods. Step notation is reserved for step methods; do not present a step-method signature as
  if it were a composed transform.
- For every internal stage whose collected code includes a transform class, copy its complete public method
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
 - Methods and method groups:
   - Identify coherent method groups - a group begins with short italicized intent sentence.
   - Add global number (non-circled) in front of each group.
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
      its stage assignments belong in the `Result` subsection.
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
- For every Code method group, use the corresponding collected paragraph as the sole source of its intent and explanation.
  Emit that paragraph exactly once with one leading Arabic group number; replace stale generated intent prose rather than
  prepending or wrapping it again. Reject duplicated sentences, adjacent italic spans such as `**`, and any group whose
  italicized intent does not match the collected source.

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
  followed by one complete typed signature per public typed method, including non-step helpers, one signature per line. Reject `methods:` summaries,
  abbreviated method-name lists, `inputs:`/`outputs:` summary blocks, and any signature not traceable to that transform.
- Extend preserves source-level raw/special helper discussion; those helpers remain represented in the extended narrative
  and in the corresponding formatted formulas, never as a reason to replace a complete transform narrative with a method summary.
- In Extend, include every `@raw` and `@special` method in its owning text step narrative and standalone transform
  notation, with its typed input and output signature. Format must preserve and convert those typed helper signatures
  as well; no typed method is omitted from formula notation.
