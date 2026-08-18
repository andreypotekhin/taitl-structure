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
- `Inputs`: list each input schema or relation, grouping related inputs when that improves readability.
- `Outputs`: list each output schema or relation and introduce grouped output families with a short descriptive sentence when useful.
- `Stages`: list each public or workflow stage as `StageName: inputs -> outputs`. Keep this as an inventory of boundaries, not an explanation of step mechanics.
- `Notation`: include one fenced text block for the workflow. List stages in execution order and list every meaningful step with its input and output relations. Keep the notation lossless and concise; do not add implementation prose or Combine reference markers.
- `Implementation`: retain the heading as the location for the later combined implementation narrative. Leave it empty unless the drafting task explicitly supplies implementation prose.
- `Code`: retain the heading and identify the corresponding collected document by filename, such as `Indexing.cnd.md`. Do not reproduce source code in the draft.

Keep the section order fixed. The draft is a structured chapter source: its lists establish the chapter's vocabulary and interfaces, its Notation block establishes workflow coverage, and its Solution establishes the reader-facing conceptual argument.

## Collection

### Collect process
- Input: close/annotated
- Output: close/collected
- Scope: close/annotated/search
- Name: Collect. Usage: Collect(dir)
- Invocation: manual

### Collect operator
- Name: collect(), usage: collect(dir)
- Input: .py.md describing a transform. Ex: rerank.py.md
- Output: .cnd.md describing a transform in collectd form. Ex: rerank.cond.md
- Goal: present annotated source as continuous narrative.
- Instructions
  - Top header: convert to gerund, optionally extend to fuller phrase.
    - Ex: 'Rerank Documents' becomes 'Reranking the Documents'
  - Low-level headers: drop low-level headers. If that affects clarity, repeat the header as part of intro section paragraph.
    - Ex: 'Select fallback options' dropped, 'select fallback options' embedded in section paragraph.
  - Avoid merging code listings, maintain a text sentence in between.
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
- Goal: present combined continuous narrative based on structured draft, background document and collected code.
 
### Combine operator instructions
Combine draft (.draft.md), background (.back.md) and collected (.cnd.md) docs:
- Draft (close/draft) contains structured chapters (.draft.md) of the future user manual, including a substantive Solution narrative.
- Combine draft doc with background and collected docs to create an introduction narrative focused on a search engine topic, such as 'chunking'
- Maintain content and structure set by draft
 - Make improvements/corrections as needed, but keep it brief/succinct where it is already; 
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
- Convert stages list to a table Stage/Inputs/Outputs 

Implementation section:
- 'How it works' section of the background doc becomes combined doc's 'Implementation' section
- Should have at least one paragraph before main bullet list
- Move-in the details that are too technical from the Solution section
- Drop implementation direction content such as discussion of invalid inputs, 'should'/'must' paragraphs
- Drop content from decisions sections and on
- Move 'Notation' section from draft into 'Implementation' section
  - Insert it after intro paragraphs, before main bullet list. 
  - Remove 'Notation' heading
  - Don't change notation content compared to draft doc.
- Make 'Implementation' main bullet list a numbered list
- Apply italics to the intent intros in numbered items
- Do refer to transforms, stages and steps from the numbered items. 
- Add sentences to numbered items for comprehensive explanation/accessibility/understandability.
- Consider joining cohesive numbered items together, to reduce/balance the overall list.  
- For each numbered item in Implementation list, add it's number as a reference number to the above notation block:
  - Mark notation line with the numbered item that explains it. 
  - Use circled digit like &#9312; for reference numbers in the notation block.
  - Add reference number to the end of notation line.
  - Omit repeating reference numbers in notation block - assume point is taken by first occurence.
  - Don't add reference numbers to end of numbered list items themselves, they already have numbers in front. 
  
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
- Code section
 - Every collected transform/method section is represented.
 - Code listings order matches the collected source.
 - Root workflows include all child and external stages.
