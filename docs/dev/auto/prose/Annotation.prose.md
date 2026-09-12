# Annotated code

Maintain annotated sources for certain parts of project code.

## Shared Prose context

This chapter operator is governed by the common concepts and conventions in [Prose.md](../Prose.md). Read its
text-process model, and shared authoring guidance before applying this file.
Definitions: [Definitions](Definitions.prose.md). Language conventions: [General style](General.style.md).

## Scope

Currently, we only maintain annotated code for these code dirs and their subdirs:

- examples/search/transforms/chunking
- examples/search/transforms/clicks
- examples/search/transforms/cohorts
- examples/search/transforms/features
- examples/search/transforms/filtering
- examples/search/transforms/inference
- examples/search/transforms/indexing
- examples/search/transforms/labeling
- examples/search/transforms/offline
- examples/search/transforms/online
- examples/search/transforms/relevance
- examples/search/transforms/scoring
- examples/search/transforms/vectorization
- examples/search/transforms/searching/search_docs
- examples/search/transforms/training
- examples/search/schemas/scoring
- examples/store/transforms/catalog
- examples/store/transforms/personalization
- examples/store/transforms/recommender
- examples/security/transforms
- src/structure/core/configuration
- src/structure/core/compiler
- src/structure/core/docs
- src/structure/core/plugins
- src/structure/core/runtime
- src/structure/core/sources
- src/structure/core/target

## Output

Destination: close/annotated/

- Example sources omit the `examples/` prefix: examples/search -> close/annotated/search
- Main sources omit the `src/` prefix: src/structure/core/docs -> close/annotated/structure/core/docs

## Annotation process

Maintain one Markdown `.anno.md` document per selected source unit, mirroring its path and filename. Ignore package
and other dunder files such as `__init__.py`. For example,
`examples/search/transforms/indexing/lexical/LexIndex.py` maps to
`close/annotated/search/transforms/indexing/lexical/LexIndex.anno.md`.

1. Inventory the requested scope recursively, including nested packages and every class in each eligible module.
   A focused revision uses the explicitly selected units; a full scope run must account for every eligible module.
2. Read current source and establish class interfaces, ordered method groups, and any composed-stage assignments.
   Annotate example transforms and explicitly scoped schemas; reference other schemas without listing their definitions.
3. Apply the Annotation operator below. Preserve source behavior and code; older outputs illustrate style, not contracts.
4. Run the quality checks before considering the selected scope complete.

## Annotation operator

Write a continuous, code-adjacent account for someone new to the domain who will glance at the listing.
The prose supplies purpose and meaning; the code supplies the complete operations. Chapter Solution and Implementation
depth requirements do not apply to these paragraphs.

### Document shape

~~~text
source unit =
    for each class in source order:
        H2 class heading
        plain purpose paragraph
        exact class declaration/interface listing
        for each coherent method group in source order:
            H3 short intent heading
            plain explanatory paragraph
            exact method listing
~~~

Use readable headings such as `## Lex Index` and `### Count document frequencies`; keep exact identifiers intact and
in inline code in body prose. The class paragraph states its useful contribution in the surrounding flow, without
anticipating low-level methods. Include decorators, parameters, inputs, and outputs in the class listing, not in a
separate Inputs section. A composed or parameter-only class receives a plain description of its actual role, not a
synthetic method group.

Each method group has one purpose. Group parallel grain methods when that purpose is shared, but retain every method
in full and in source order. Keep private helpers in clearly identified sections, such as Private methods. Give each
listing its own preceding explanation; never concatenate listings without prose or repeat an explanation after its code.

Annotation headings provide Collect's short italic intents. Annotation itself does not introduce chapter item numbers,
italicized class intents, or numeric callouts inside Python. Collect preserves group prose; Extend alone adds Code
numbers, independently of Implementation.

### Method-group prose contract

**Lead with the useful outcome.** Explain what this step contributes to the surrounding task, not how to read its
operators. The sentence must add meaning beyond its intent heading. Prefer a concrete action and familiar object:
"Count how many documents contain each term" rather than "Count document-term rows by term."

**Prefer one short sentence.** Add a second only for a consequential distinction, rationale, or behavior that the
reader would otherwise miss. Do not compress a paragraph into a clause-packed sentence, and do not add a rationale
merely to make every group sound complete. Read adjacent groups as a sequence; do not restate established context.

**Choose details by explanatory value.** Keep a mechanism when it explains the result, and name a schema or field when
it resolves ambiguity. Preserve consequential limits, assigned discriminator values, precedence, and statistical
populations. Do not routinely enumerate join keys, parent identities, intermediate relations, or downstream uses
already evident from the listing. A grain difference needs prose only when it changes interpretation; complete code
coverage never depends on repeating that difference in words.

**Keep the language approachable.** Define unfamiliar roles and terms when first needed. Prefer concrete verbs to
stacked abstractions, avoid overqualified nouns and unexplained shorthand, and never assume that fewer words alone
mean greater clarity. Do not merely translate each code operation into English.

Necessary extended explanation, such as a non-obvious formula or boundary case, may follow the listing. It must add
new understanding; do not move discarded paraphrases below the code to satisfy the one-sentence preference. Keep
this detail distinct from the group paragraph that Collect carries into numbered Code items.

### Calibration examples

These examples demonstrate selection of meaning, not text to copy regardless of current source:

- Count document frequencies: "Count how many documents contain each term. This corpus-level frequency is separate
  from the per-document term count." The second sentence distinguishes two easily confused measures; explaining the
  uniqueness of the preceding relation as well is usually redundant.
- Match targets: explain the matched query weight accumulated for each target. Mention admission or finer grains when
  needed, without repeating the entire identity hierarchy or narrating every join.
- Publish overlap scores: explain normalized overlap and the active scoring timestamp. Do not append a generic claim
  that separate output relations let downstream stages combine compatible evidence.

### Quality assurance

| ID | Acceptance check |
|---|---|
| A1 Scope | Every eligible module in the requested scope has its mapped annotation; every class has its own introduction and declaration. A focused revision records its selected units rather than claiming full-scope coverage. |
| A2 Exact coverage | Interfaces and all public steps, implicit steps, special/raw methods, and private helpers occur exactly once in source order. Preserve code contents and complete parallel grain paths; do not insert callouts or invent methods. |
| A3 Group shape | Every coherent method listing has a short intent heading and one plain explanatory paragraph immediately before it. Class/stage descriptions remain plain; no synthetic group, duplicate explanation, unseparated listing, or chapter numbering. |
| A4 Purpose first | Read the paragraph without its heading: it identifies a useful contribution, not merely a sequence of operators. Read it beside the code: it adds meaning rather than paraphrasing each line. |
| A5 Subtraction test | Usually one short sentence. For every additional sentence or clause, identify the consequential distinction it adds. Remove facts already apparent from adjacent code or established prose; do not relocate them or pack them into one long sentence. |
| A6 Accessible precision | A first-time reader can identify the action and result without unexplained jargon. Preserve necessary populations, limits, discriminator values, and other observable distinctions; name concrete schemas/fields selectively. Trace each method's declared input: sequence alone does not mean it consumes the preceding step's output. Fields flattening reads the original map, not the completed map. |
| A7 Sequence and propagation | Read neighboring groups for repetition and continuity. When downstream refresh is authorized, Collect retains the revised paragraph verbatim, Extend adds only its independent Code prefix, and Form preserves Extend Code exactly. Do not derive Code prose or numbering from Implementation. |
| A8 Style regression | When comparing older examples, review purpose-first phrasing, sentence load, and useful distinctions, not word count alone. Exercise document frequency, multi-grain matching, and score publication; account for current source behavior before adopting an older explanation. |

### Non-example app annotated source 
Non-example app annotated source, e.g. annotated source for core packages, 
follows the above instructions for example code, with following adjustments:
- For top-level packages under core/, plugin/, plugin/pyspark, create Package document: package.anno.md
- In the package document, describe the purpose of the package and overall flow.  
- Package dirs, modules and methods do not typically follow 'top-to-bottom' sequence inherent to transforms.
Therefore, it may be more difficult to create a continuous narrative for top-to-bottom reader.
Instead, the responsibility of providing structure and clarity for the reader belongs to the package document.
There is still some top-to-bottom opportunity, since our api-commands-logic are alpha ordered, general-to-detail sequence. 
So the expectation is the reader is still following top-to-bottom, but because it stops at the subpackage, readers need
project doc for backtracking to the correct path.
- Can incorporate Readme.md into project doc, if it exists.

Tips:
- Drop 'package', 'flow' from package doc headings
