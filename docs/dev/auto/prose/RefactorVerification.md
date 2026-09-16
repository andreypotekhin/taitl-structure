# Chapter operator refactor verification

## Example-led Problem style applied across all families — September 15

Applied the approved Problem trial for Chunking, Fields, and Indexing, preserving the accepted wording with file line
wrapping. Revised the other eighteen families sequentially using current chapters, without numbered-variant copying
or parallel family work. Prose was manually authored; apply_patch propagated it across Draft, Extend, and Format.
No prose-generation script, full chapter regeneration, annotation refresh, or collected-code changes were used.

The Problem contract now develops general context -> concrete situation -> meaningful variation -> explained difficulty
-> broader need. It applies Solution's graspability principles while reserving the proposed answer for Solution.
The Draft template and existing narrative QA checks use the same distinction. General framing precedes examples;
terminology arrives in context; connected explanation has room to develop without a paragraph quota or required
rhetorical pattern. QA also records concurrent-edit exceptions rather than overwriting unrelated work to pass a hash check.

### Family acceptance record

Every family passed its three-phase text and outside-Problem preservation checks immediately after its edit, before
the next family began. Counts are whitespace-token comparisons, not readability scores.

| Family | Before -> after words | Draft / Extend / Form |
|---|---:|---|
| Chunking | 187 -> 234 | Pass |
| Fields | 190 -> 243 | Pass |
| Indexing | 204 -> 290 | Pass |
| Filtering | 203 -> 258 | Pass; final preservation exception below |
| Inference | 192 -> 257 | Pass |
| Vectorization | 191 -> 246 | Pass |
| Scoring | 197 -> 251 | Pass |
| Similarities | 179 -> 235 | Pass |
| SearchDocuments | 199 -> 248 | Pass |
| SearchFields | 210 -> 249 | Pass |
| SearchSimilarity | 196 -> 238 | Pass |
| Offline | 192 -> 246 | Pass |
| Online | 203 -> 241 | Pass |
| Cohorts | 218 -> 253 | Pass |
| Clicks | 210 -> 252 | Pass |
| Relevance | 200 -> 253 | Pass |
| Labeling | 192 -> 235 | Pass |
| Features | 200 -> 251 | Pass |
| Training | 213 -> 249 | Pass |
| Experiments | 200 -> 234 | Pass |
| Evaluation | 237 -> 262 | Pass |

All 21 families / 63 files contain their authored Problem revisions, with identical Problem text across each family's
three phases. Counts rise from 4,213 to 5,225 words across the 21 distinct chapters. Editorial review checked general
openings, connected examples, retained distinctions, and the transition into each unchanged Solution. All newly used
inline definitions have existing glossary entries, allowing ordinary singular/plural variation. An unnecessary
unintroduced use of "vector" in Online was replaced with "representation" and verified again.

The final fresh-read comparison confirms exact outside-Problem SHA-256 matches for 62 files. Filtering.form.md passed
that comparison when its Problem was edited, but later changed outside Problem during concurrent diagram work.
Its saved and final Solutions show removal of the diagram embed, explanatory paragraph, and PNG-preview link; its
Problem still matches both other phases. The full outside-Problem hash remains different after accounting for that
Solution block, so this file is not claimed as a final exact-preservation pass. No attempt was made to restore or
alter unrelated content. The existing Diagrams.style.md edit was also left untouched.

This pass's chapter patches modify Problem only. The retained Solutions, formulas, Implementation, and Code were
not regenerated. Direct file reads were used because close/ is ignored by Git. Scoped checks for the changed prompt,
template, QA, and verification files pass. No runtime code changed and no application build was required.

Verification is a focused editorial and preservation review, not a reader study, new visual rendering check, or full
source-contract audit. The concurrent Filtering exception remains explicit rather than being hidden in a blanket
claim that every complete file stayed unchanged outside Problem.


## Example-led teaching applied across Solutions — September 15

Recorded the accepted Rust Programming Language-inspired teaching approach in Solution style: establish a familiar
need, follow a concrete situation, explain its outcome, then extend it to motivate the next concept. The guidance is
self-contained and links to the book only as an illustrative reference. It calls for original chapter prose, not copied
wording, Rust-specific examples, or a code-tutorial format. General grounding, conceptual precision, expressive prose,
and room for difficult connections remain requirements.

The Draft template invokes this progression. QA tests whether examples explain the approach rather than decorate a
definition inventory, whether each variation advances understanding, and whether concepts and symbols arrive before
the explanation depends on them. Optional questions and conversational transitions are not mandatory sentence patterns.
Draft and Extend reference the shared style; Format checks it while preserving the extended Solution narrative.

Applied the accepted trial to Chunking, Fields, and Indexing, then revised the remaining families sequentially.
Successful worked calculations were retained and connected to the surrounding explanation. All changes were manually
authored and propagated through apply_patch; no prose-generation script, parallel family work, or numbered-variant
copying was used. Every family passed its three-phase preservation checks before the next began.

### Family acceptance record

Counts are comparative whitespace-token counts with each notation block represented by one placeholder. They flag
unexpected compression; they do not measure readability. The notation column counts corresponding blocks per phase.

| Family | Before -> after prose count | Notation blocks | Draft / Extend / Form checks |
|---|---:|---:|---|
| Chunking | 430 -> 485 | 1 | Pass |
| Fields | 614 -> 656 | 0 | Pass |
| Indexing | 841 -> 880 | 2 | Pass |
| Filtering | 552 -> 556 | 1 | Pass |
| Inference | 610 -> 637 | 1 | Pass |
| Vectorization | 646 -> 692 | 3 | Pass |
| Scoring | 931 -> 980 | 6 | Pass |
| Similarities | 632 -> 656 | 2 | Pass |
| SearchDocuments | 632 -> 674 | 1 | Pass |
| SearchFields | 681 -> 706 | 1 | Pass |
| SearchSimilarity | 561 -> 588 | 1 | Pass |
| Offline | 629 -> 675 | 0 | Pass |
| Online | 593 -> 600 | 1 | Pass |
| Cohorts | 684 -> 676 | 1 | Pass |
| Clicks | 639 -> 656 | 1 | Pass |
| Relevance | 1002 -> 1057 | 7 | Pass |
| Labeling | 579 -> 583 | 0 | Pass |
| Features | 606 -> 629 | 2 | Pass |
| Training | 695 -> 746 | 1 | Pass |
| Experiments | 657 -> 705 | 1 | Pass |
| Evaluation | 1222 -> 1350 | 6 | Pass |

All 21 families / 63 documents have revised Solution prose. Fresh file reads confirm:

- The authored Solution text is present in every file.
- Draft, Extend, and Form contain the same Solution prose after accounting for notation presentation.
- All 39 corresponding notation blocks retain their exact pre-edit contents and order in each phase.
- The SHA-256 hash of every complete document outside its Solution body matches the pre-edit baseline.
- No unresolved numbered placeholders, unbalanced inline-math delimiter counts, or text fences occur in Form Solutions.

Editorial review checked the paragraph-opening progression and the explanations around the examples. Particular
attention went to model compatibility, duplicate-term scoring, one-sided retrieval evidence, SearchFields selection
limits, independent Offline entry points, cached score-group replacement, feedback calculations, labeling precedence,
training eligibility/defaults, and evaluation denominators and missing judgments. The overall comparison count rises
from 14,436 to 15,187; the small Cohorts reduction retains its matching, identity, and fallback distinctions.

Only Solution changed in the chapter files. Intent, Problem, Definitions, stage inventories, Design, Notation,
Implementation, and Code are unchanged. Annotated source and collected code were not regenerated. The close/ tree is
ignored by Git, so direct file comparisons provide chapter evidence. Scoped documentation diff checks pass. A separate
concurrent Diagrams.style.md edit has trailing whitespace; it was left untouched and is outside this pass.

Verification is a focused editorial and preservation review, not a reader study, full source-contract audit, or new
visual rendering check. Existing formulas and Implementation/Code were preserved rather than regenerated or newly
certified. No runtime code changed, so no application build was required.


## Graspability applied to all Solution sections — September 14

Recorded graspability in Solution style as understanding on a skim, supported by explanation on a close reading.
The Draft template invokes that contract; QA checks both the readable progression and retained conceptual precision.
Paragraph openings carry useful ideas without becoming compulsory slogans. Technical terms are introduced in context;
harder connections receive more development. Generality, theory, expressiveness, and pace remain requirements.

Applied the accepted Chunking, Fields, and Indexing trial to their existing Solution sections, preserving their original
phase-specific notation. Revised the other eighteen families sequentially, retaining successful prose and developing
dense introductions, relationships, or qualifications. The work was manually authored; tool orchestration propagated
the same edits through apply_patch and checked preservation. No prose-generation script or numbered-variant copying
was used. This pass changes Solution only: Intent, Problem, Definitions, inventories, Design, Notation, Implementation,
Code, and all text outside the Solution body remain unchanged in each chapter.

### Family acceptance record

Each family passed before the next began. Counts below are comparative whitespace-token counts with each notation
block represented by one placeholder, not reading-quality scores. The notation column counts corresponding Solution
blocks in each phase; Form retains the existing display formulas.

| Family | Before -> after prose count | Notation blocks | Draft / Extend / Form checks |
|---|---:|---:|---|
| Chunking | 551 -> 430 | 1 | Pass |
| Fields | 552 -> 614 | 0 | Pass |
| Indexing | 769 -> 841 | 2 | Pass |
| Filtering | 540 -> 552 | 1 | Pass |
| Inference | 609 -> 610 | 1 | Pass |
| Vectorization | 600 -> 646 | 3 | Pass |
| Scoring | 827 -> 931 | 6 | Pass |
| Similarities | 606 -> 632 | 2 | Pass |
| SearchDocuments | 619 -> 632 | 1 | Pass |
| SearchFields | 617 -> 681 | 1 | Pass |
| SearchSimilarity | 540 -> 561 | 1 | Pass |
| Offline | 577 -> 629 | 0 | Pass |
| Online | 619 -> 593 | 1 | Pass |
| Cohorts | 638 -> 684 | 1 | Pass |
| Clicks | 616 -> 639 | 1 | Pass |
| Relevance | 888 -> 1002 | 7 | Pass |
| Labeling | 570 -> 579 | 0 | Pass |
| Features | 613 -> 606 | 2 | Pass |
| Training | 686 -> 695 | 1 | Pass |
| Experiments | 631 -> 657 | 1 | Pass |
| Evaluation | 1177 -> 1222 | 6 | Pass |

All 21 families / 63 documents have changed Solution prose. Exact comparisons confirm matching Solution prose across
Draft/Extend/Form after accounting for notation presentation. All 39 text-model/display-formula pairs are unchanged,
including block contents and order. SHA-256 comparisons of the complete document outside Solution match the saved
pre-edit baseline for every file. Checks also found no unresolved placeholders, unbalanced inline-math delimiter
counts, or text fences in Form Solutions. Documentation diff checks pass; close/ is ignored by Git, so chapter checks
used direct file reads rather than relying on git status.

Editorial review covered the paragraph-opening progression and the complete explanation, with particular attention
to duplicate-term scoring, one-sided fusion and lexical fallback, SearchFields scope limits, independent Offline
entry points, query-score group replacement, feedback thresholds, labeling precedence, model defaults, and metric
eligibility. A review caught the loss of the word "natural" before logarithm in Relevance; it was restored in all
three phases, and QA now explicitly preserves calculation qualifiers during readability edits.

Chunking's reduction from 551 to 430 follows the accepted trial: parentage/ordinal navigation is explained together
rather than repeated after the span example. Its hierarchy, grains, source spans, formula interpretation, boundary
choices, segmenter role, and later retrieval responsibilities remain. Overall comparison counts rise from 13,845 to
14,436; neither that increase nor any individual reduction establishes graspability on its own.

Verification is a focused narrative/preservation review, not a new reader study, full source-contract audit, or rendered
math check. Existing formulas and all Implementation/Code content were preserved, not regenerated or newly certified.
No runtime code changed, and no application build was required.

## Accessible explanation restored after the Solution retry — September 14

The user accepted the second chat-only Chunking, Fields, and Indexing Solution trial and requested its guidance in
the prompts and QA. This supersedes the audience assumption and explanation-trimming guidance in the entry below.
General style again assumes no prior search/IR introduction. Solution style and the Draft template ask for concept
meaning, purpose, and connections, with familiar language, concrete examples, and room for developed explanation.
Clear referents, direct sentences, coherent topic order, and successful expressive wording remain requirements.

Revised S3d/S3e/S6 and the narrative regression cases to assess first-reading understanding without Definitions or
Code. Removed pressure to keep presumed easy material brief, move on after a definition, or require a new idea in
each paragraph. Examples and consequences may develop existing concepts; contrasts alone do not indicate excessive
explanation. Existing grounding, conceptual coverage, formula teaching, and source-fidelity requirements still apply.

Verified the rules remain connected to Draft/Extend/Format and Prose.md. Searched the active guidance and template
for the superseded audience and trimming instructions; none remain. Documentation diff checks pass. Before/after
SHA-256 comparisons confirm all 63 current Draft/Extend/Form chapter files are unchanged. The accepted trial was
shown in chat; this update changes prompts, template, and QA only, with no chapter regeneration or runtime-code change.

## Selective Solution depth and forward movement — September 14

Historical entry: its audience assumption and explanation-trimming guidance are superseded by the retry above.

Updated prompting and QA after review of the chat-only Chunking, Fields, and Indexing Solution trial. The reader is
sharp but unfamiliar with search/IR; accessibility should supply missing concepts and non-obvious connections, not
repeated explanations of established ideas. Solution.style.md now allocates expansion by difficulty, keeps conceptual
threads together, favors concrete referents and direct sentences, and preserves successful expressive wording.
Contrast phrases are editorial review signals, not prohibited syntax or count-based failures.

Revised the Draft Solution slot and shared QA S3d/S6, and added S3e with four editorial regression cases: repeated
Chunking relationship walkthroughs, layered boundary/representation explanations, vague Fields subjects and storage
obligations, and Indexing's normalization/compatibility detour. Positive and negative cases preserve necessary semantic
contrasts and the dedicated explanation for each conceptual formula. The existing general opening, coverage, and
formula contracts still apply; selective depth neither imposes brevity nor requires a fixed volume increase.

Verified the owning rules are already referenced by Draft/Extend/Format and Prose.md. Compared the Solution style's
Intent/Problem sections and calculation contract with HEAD; they are unchanged. Example-selection guidance is also
unchanged apart from line wrapping, as the example exercise remains deferred. Documentation diff checks pass.
SHA-256 comparisons confirm all 63 current Draft/Extend/Form chapter files are byte-for-byte unchanged.

This is a prompt/template/QA revision with editorial regression cases, not a chapter regeneration or a measured
generation-quality result. Application code and the implementation/notation contracts were not changed; no runtime
build or visual-rendering test was needed for these instruction-only edits.

## Beginner-accessible development: remaining families — September 14

Applied the restored accessibility contract to the twelve remaining families: Filtering, SearchDocuments,
SearchFields, SearchSimilarity, Offline, Online, Cohorts, Clicks, Relevance, Labeling, Features, and Training.
This continues the nine-family pass below, not the subsequently reverted restoration of 7/ Problems.
All twelve families now have updated Draft, Extend, and Form outputs (36 files). The earlier nine families were not
edited in this pass. The combined checklist covers all twenty-one chapters named in Prose.md, including Features
as its own chapter.

Worked sequentially: authored each family's Problem/Solution revision, preserved its glossary, propagated the accepted
narrative through Extend and Form, and checked readback and protected content before starting the next family.
Final editorial review improved paragraph separation and wrapping. No numbered-reference prose was copied.
Existing General/Solution style, template slots, and QA S3a/S3d/S8d/S0 cover this continuation; no new prompt rule was
needed.

### Development and calculation coverage

| Family | Problem prose words: before / after | Solution prose words: before / after | Solution formulas |
|---|---|---|---|
| Filtering | 148 / 203 | 333 / 539 | 1 |
| SearchDocuments | 155 / 199 | 371 / 618 | 1 |
| SearchFields | 166 / 210 | 425 / 616 | 1 |
| SearchSimilarity | 151 / 196 | 341 / 539 | 1 |
| Offline | 151 / 192 | 359 / 577 | 0 |
| Online | 145 / 203 | 403 / 618 | 1 |
| Cohorts | 164 / 218 | 404 / 637 | 1 |
| Clicks | 152 / 210 | 403 / 615 | 1 |
| Relevance | 160 / 200 | 535 / 881 | 7 |
| Labeling | 152 / 192 | 355 / 570 | 0 |
| Features | 147 / 200 | 401 / 611 | 2 |
| Training | 167 / 213 | 417 / 685 | 1 |

Counts exclude display-model contents and document development rather than impose length targets. Problems retain
gentle introductions and add recognizable situations. Solutions preserve their general openings while developing
examples, explanations, and limitations. Filtering explains distinct-term counting; SearchDocuments and
SearchSimilarity interpret rank fusion with explicitly illustrative constants rather than configured defaults.
SearchFields develops supported query forms and phrase offsets, retaining its admission-bound limitation and published
evidence scopes.

Offline explains independently reusable artifacts and popular/recent query populations without inventing a common
workflow. Online separates temporal suitability, model/scope compatibility, gap selection, external processing, and
replacement of invalidated score groups. Cohorts explains overlapping dimensions, ordered context identity, and
declared fallback; it does not promise access control or automatic feedback selection. Clicks distinguishes event
counts, responding impressions, attributed display days, credited dwell, and streaming state.

Relevance now interleaves seven calculations with their explanations: combined age/propensity weight, weighted CTR,
log-transformed dwell, group maximum, relative dwell, exposure-gated CTR contribution, and the final blend.
All calculations from the earlier combined blocks remain present. Examples retain distinct clicked impressions as the
response numerator and distinguish suppression of the CTR blend contribution from the still-reported rates.
The maximum and normalized dwell examples remain group-relative, not absolute relevance judgments.

Labeling explains rule outcomes and overlay precedence, including generated zeros overriding caller ones and locale
selection without an invented English retry. Features separates total and distinct token counts into two formulas with
their own examples; it preserves original versus normalized lengths and supplied rather than inferred query flags.
Training explains judged candidate inclusion and the contributions of a supplied linear model, without claiming model
fitting, automatic reranking, or promotion. Its arithmetic example uses all five-feature semantics while isolating one
illustrative contribution.

### Verification

Final readback passed for all 36 files. Draft/Extend/Form Problem text agrees, and Solution prose agrees after
normalizing only its notation blocks. Original glossary entries remain intact; added Cache, Weighted click-through
rate, and Regular expression definitions follow alphabetical ordering and each phase's prescribed shape.
SHA-256 comparisons with CRLF/LF normalized, excluding only Problem, Solution, and Definitions bodies, match every
pre-edit file. Intent, inventories, Design, Implementation preambles/items, return-schema notation, Results,
numbering, and Code are therefore unchanged.

Each of the 17 Solution formula blocks has a dedicated following explanation; no text/LaTeX notation fence remains in
Form Solution. Pandoc converted all 17 to MathML without residual LaTeX outside source annotations. Reviewed the
illustrative arithmetic and source-backed distinctions, including relevance weighting/gating and feature counts.
Offline and Labeling remain formula-free rather than adding a calculation merely for a quota.

Documentation whitespace checks passed. This is a focused narrative, preservation, and notation-parser verification,
not a full source audit, visual layout inspection, or runtime/build test. Annotation, collected source, application
code, and numbered references were not edited.


## Beginner-accessible Problem and Solution development — September 13

Applied the approved Fields/Indexing accessibility trial sequentially to Chunking, Fields, Indexing, Scoring,
Similarities, Inference, Vectorization, Experiments, and Evaluation across Draft, Extend, and Form (27 files).
Each family passed a readback and preservation check before the next family was edited. No numbered reference
was copied and no prose-generation script or parallel family agent was used.

General.style.md now assumes no prior introduction to search or information retrieval. Solution.style.md and the
Draft template replace paragraph quotas with connected development: introduce the idea, explain its purpose, then
show its consequence or example. A small recurring search-user situation provides continuity where useful.
QA S3d checks beginner development; S8d inventories conceptual calculations, introduced symbols, dedicated explanation
paragraphs, worked arithmetic, and source-grounded boundaries. These rules do not expand Implementation items.

The revisions use a help-collection example while preserving each chapter's scope. Fields distinguishes body text,
dedicated attributes, maps, and field matching. Indexing develops book-index practice into occurrences, frequencies,
positions, and structural grains. Scoring separates the calculations behind weighted overlap, BM25, and cosine
similarity. Similarities explains directional evidence before reciprocal reduction. Inference introduces vectors,
providers, and inference adapters; Vectorization distinguishes producing vectors, reusing them, and comparing them.
Experiments explains controlled alternatives and the illustrative feedback blend. Evaluation separates each judged
and behavior calculation, including its denominator, interpretation, and evidence limitations.

### Coverage and preservation

Counts below describe prose words in the Draft bodies, excluding display text-model blocks. They document development,
not a length target. Formula counts describe Solution blocks in Form, not Implementation signatures.

| Family | Problem words: before / after | Solution words: before / after | Solution formulas |
|---|---|---|---|
| Chunking | 142 / 187 | 369 / 550 | 1 |
| Fields | 143 / 190 | 336 / 552 | 0 |
| Indexing | 157 / 204 | 408 / 767 | 2 |
| Scoring | 142 / 197 | 529 / 821 | 6 |
| Similarities | 152 / 179 | 343 / 604 | 2 |
| Inference | 156 / 192 | 359 / 608 | 1 |
| Vectorization | 152 / 191 | 365 / 597 | 3 |
| Experiments | 149 / 200 | 411 / 630 | 1 |
| Evaluation | 146 / 237 | 654 / 1171 | 6 |

Retained the existing conceptual calculations, splitting multi-calculation blocks to interleave explanations.
Made Indexing's term/target-frequency definitions and Experiments' existing feedback blend explicit as formulas.
Fields remains formula-free because its explanation does not need an artificial calculation. Each of the 22 Form
Solution formulas has a dedicated explanatory paragraph. Examples distinguish illustrative values from configured
defaults, preserve Scoring's repetition behavior, and explain Evaluation's fixed cutoffs, judgment eligibility,
propensity weighting, and non-probabilistic dwell credit.

Preserved existing glossary definitions, added essential concepts introduced by the expanded prose, and alphabetized
entries using the appropriate Draft/Extend/Form shape. SHA-256 comparisons with line endings normalized pass for all
27 files after excluding only Problem, Solution, and Definitions bodies. Intent, remaining inventories, Design,
Implementation preambles and items, signature/return schemas, stage formulas, Results, numbering, and Code therefore
remain unchanged. Readback matches each manually authored target; Form uses formulas rather than text-model fences.

Pandoc converted all 22 revised Solution display formulas to MathML with no residual LaTeX outside source annotations.
This verifies parser compatibility, not visual layout in Typora. Current-source checks support the explanatory
examples and boundaries; this pass is not a full source audit or runtime/build test. Annotation, collected source,
application code, and numbered reference documents were not edited.


## Grounded Solution openings: remaining families — September 13

Applied the accepted Solution-opening contract sequentially to the remaining fifteen families. Reviewed each current
opening with the next paragraph and its glossary, and compared the fourteen available `close/5/draft/` references for
grounding and progression rather than copying their prose. Features has no `5/` counterpart.

Revised Filtering, Similarities, SearchDocuments, SearchFields, SearchSimilarity, Offline, Online, Clicks, Relevance,
Labeling, Training, Experiments, and Evaluation across Draft, Extend, and Form. Retained Cohorts, whose shared-experience
example already connects practice, purpose, and concepts, and Features, which was authored under the new contract.
The earlier six-family trial is unchanged. No additional prompt changes were needed; Solution.style.md and QA S3c
already govern this behavior.

| Family | Opening words: before / 5 / after | Outcome |
|---|---|---|
| Filtering | 53 / 75 / 77 | Revised |
| Similarities | 52 / 66 / 71 | Revised |
| SearchDocuments | 48 / 61 / 72 | Revised |
| SearchFields | 48 / 77 / 80 | Revised |
| SearchSimilarity | 54 / 56 / 71 | Revised |
| Offline | 59 / 61 / 68 | Revised |
| Online | 52 / 75 / 82 | Revised |
| Cohorts | 55 / 82 / 55 | Retained |
| Clicks | 61 / 74 / 78 | Revised |
| Relevance | 69 / 71 / 84 | Revised |
| Labeling | 62 / 66 / 75 | Revised |
| Features | 76 / — / 76 | Retained |
| Training | 56 / 74 / 80 | Revised |
| Experiments | 51 / 71 / 79 | Revised |
| Evaluation | 46 / 65 / 74 | Revised |

Read each opening into its unchanged second paragraph. The revisions explain why the approach helps before describing
its choices: candidate filtering before detailed ranking; lexical similarity at a consistent grain; complementary
retrieval evidence; combined metadata/body requests; reuse across preparation and request-time work; exposure as the
basis for behavioral interpretation; query groups for evaluation; and judged examples for learning. Preserve the
distinctions that feedback reranks existing candidates, observations are not relevance judgments, and training-data
preparation does not fit the model later used for scoring. Existing glossary entries cover the introduced terms;
inline defining occurrences retain selective italics.

Verification covered all twenty-one families and sixty-three narrative-phase files. Thirteen families changed
(thirty-nine files); the remaining eight families' files match their pre-edit snapshots in full. In changed files,
SHA-256 comparisons after excluding only the Solution opening match the pre-edit content, with CRLF/LF normalized.
All non-opening content is therefore preserved: Intent, Problem, the rest of Solution, definitions, formulas,
Implementation, numbering, and Code. Each revised family's three opening paragraphs agree exactly, and each family
passed its readback/preservation gate before the next family was edited.

The existing ownership and empty-section rules remain intact. No application code, annotation, collected source, or
numbered reference was edited. Documentation diff checks passed. This is focused narrative review and preservation
verification, not a full source audit, visual rendering, or runtime/build test.


## Empty sections, Features, and grounded Solution openings — September 13

Format now omits genuinely empty optional sections at every depth, rather than special-casing Stages. The owning
Format rule, Form template, and QA F1/F2/F14 distinguish a nonempty child container from an empty subtree, and missing
required content from an optional omission. Removed empty Builds on sections from Labeling, Cohorts, and Clicks Form
documents; their upstream inventories remain unchanged. A fence-aware scan of all twenty-one current Form chapters
found no remaining empty section subtrees.

### Features chapter

Added Features to Prose.md's current chapter set and authored its complete family from the current features package:
three annotation documents plus Draft, Collect, Extend, and Form. Features is a composed main with internal
BuildDocumentFeatures and BuildQueryFeatures stages, not an expanded part of Training. Training's Draft/Extend/Form
preamble and its external-stage description now refer to the Features chapter; its Code and boundary formulas remain
unchanged.

Verified three class declarations and five public methods against current source: eight Python listings, unchanged
through annotation, collection, and downstream Code. Collect prose matches Extend Code after heading rebasing and
removing independent decimal prefixes; Form Code matches Extend verbatim. Implementation has five circled items, two
step shapes, and one composed Result. Its two real stage assignments retain their aliases and complete bindings.

Nine text-notation blocks in Extend map to nine Form display formulas: one conceptual model, five method signatures,
two step shapes, and one Result. Each of the five returned schemas has one full first-return definition, including
pure projections and inherited query-token fields. The field ledger is DocumentFeatures (9),
ExpandedQueryFeatureToken (3), QueryFeatureToken (2), QueryTokenSummary (3), and QueryFeatures (8), in source order.
All explicit projection fields and carried identities/attributes are present; no field ellipses are needed for these
small records. Pandoc converted all nine display formulas to MathML without residual LaTeX outside source annotations.
This is a notation-parser check, not a visual rendering claim.

Reviewed the repeated-token example (three total, two distinct), tokenless-query zero counts, original rather than
normalized text lengths, nullable URL prefix checks, copied query flags, and query-identity grouping against source.
The chapter does not claim model fitting, label generation, key validation, or snapshot/provider guarantees that the
background proposes but these methods do not implement. Ten alphabetized glossary entries cover the opening concepts.

### Solution opening trial

Compared current and `close/5/draft/` openings for the six requested families. The previous style pass often replaced
an explanatory opening with an imperative instruction. Solution.style.md now requires one connected opening that
grounds the topic in theory/practice, establishes its purpose, and introduces the approach's concepts gently; the
Draft template and QA S3c reinforce that contract without a fixed paragraph or sentence quota.

| Family | Opening words: before / 5 / after | Restored connection |
|---|---|---|
| Chunking | 58 / 58 / 79 | Passage retrieval connects precise evidence to surrounding argument and context. |
| Fields | 49 / 53 / 70 | Familiar metadata clues motivate a common field model without a fixed application vocabulary. |
| Indexing | 65 / 77 / 81 | Shared preparation supplies reusable term evidence for retrieval and later ranking. |
| Scoring | 54 / 66 / 68 | Ranking draws on distinct kinds of evidence whose raw scales need not agree. |
| Inference | 52 / 64 / 73 | Different wording motivates a shared representation while preserving exact-word matching. |
| Vectorization | 57 / 87 / 83 | Repeated comparisons motivate reusable embeddings with text and model identity. |

Applied the openings sequentially to Draft, Extend, and Form, checking each family before the next. SHA-256 comparisons
after excluding only the opening paragraph confirm that all other content in these eighteen files is unchanged,
including Intent, the remainder of Solution, formulas, definitions, Implementation, and Code. No numbered variant
was edited or copied wholesale. This is focused narrative QA, not a new full-source certification of those chapters.

Final checks: twenty-one complete Draft/Collect/Extend/Form families; no empty Form sections; source/listing/prose
parity for Features; Training references and Code parity; eighteen-file narrative preservation; git diff --check.
No application source was edited by this documentation task. Other source changes appearing during the pass were
left untouched; no runtime/build tests or visual chapter rendering were performed.

## Intent clarity rollout — September 13

Applied the approved Intent style sequentially across all twenty current families, preserving useful terminology and
expressiveness rather than requiring a rewrite. The shared Intent contract in Solution.style.md owns the rule;
QA.prose.md S3b checks first-reading clarity, scope, preservation of distinctions, and downstream consistency.

- Updated: Chunking, Filtering, Inference, Similarities, SearchDocuments, SearchFields, SearchSimilarity, Offline,
  Cohorts, Clicks, Relevance, Labeling, Training, Experiments, and Evaluation.
- Retained: Indexing, Scoring, and Vectorization, as preferred in the comparison; Fields and Online also already met
  the contract. The eight approved trial rewrites were applied without wording changes, apart from line wrapping.
- Coverage: twenty families and sixty Draft/Extend/Form files checked; fifteen families and forty-five files changed.
  Each family was read back and verified before proceeding to the next. Draft, Extend, and Form Intents agree within
  every family after whitespace normalization.
- Preservation: SHA-256 comparisons of each file with only its Intent body removed match the pre-edit snapshot in all
  sixty files, after CRLF/LF normalization. Problem, Solution, Definitions, Implementation, formulas, and Code remain
  unchanged. Annotated/collected source and numbered variants were not edited.
- Review: each Intent remains one or two sentences. Inference supports semantic retrieval rather than performing it;
  Training prepares examples and makes a supplied model usable rather than claiming model fitting. Similarities keeps
  lexical evidence and same-level comparisons distinct from SearchSimilarity's combined retrieval sources. Offline
  describes advance preparation without inventing a common parent workflow.
- Validation: focused content checks and git diff --check passed. No chapter-generation scripts, parallel agents,
  renderer, or build tests were used; this pass changes only prose and prompt guidance.

## Accessible narrative rollout, definitions, and inline mathematics — September 12

Applied the approved Problem/Solution style sequentially to the remaining fourteen families. Each family was checked
after its narrative edits and glossary review before moving on. Draft and Extend carry the same conceptual account;
Form preserves that prose and the existing display formulas. Existing Implementation and Code were not regenerated.

| Family | Problem words, before → after | Solution words, before → after | Retained conceptual distinctions |
|---|---:|---:|---|
| Experiments | 85 → 149 | 426 → 383 | Production/active identities, independent variants, BM25 parameters, 90/10 example versus final score, judged/served evidence and limits on causal claims. |
| Evaluation | 92 → 146 | 684 → 626 | Fixed-cutoff precision, judged recall example, graded gain, eligibility/nulls, request population, long clicks, exposure weighting and independent slices. |
| Offline | 84 → 151 | 328 → 350 | Separate artifact lifecycles, popular/recent union, scope and compatibility, caller-selected inference population, scheduling/storage ownership. |
| Online | 102 → 145 | 435 → 373 | Independent gaps, request-relative time formula, vector compatibility, admitted scope, full invalidated-group replacement and caller ownership. |
| Chunking | 109 → 142 | 361 → 348 | Focus/context, hierarchy and ordinals, half-open span example/formula, source positions, replaceable segmentation and relevance separation. |
| Fields | 105 → 143 | 331 → 315 | Typed/map precedence example, custom fields, profiles/analyzers, keyword/text behavior, aggregate metadata gaps and separate body content. |
| Filtering | 103 → 148 | 377 → 309 | Candidate tradeoff, normalization example, distinct-term formula, ties, lexical blind spots and timestamp versus reuse eligibility. |
| Inference | 100 → 156 | 383 → 338 | Different-word example, model/provider/adapter roles, embedding comparison formula, compatibility, query/document lifecycles, statuses and lexical fallback. |
| Vectorization | 101 → 152 | 353 → 339 | Reusable vector identity, cosine model, caller-owned work selection, binding versus inference, text/similarity examples and source identity. |
| Scoring | 102 → 142 | 589 → 515 | All four calculations, grain populations, query multiplicity and uncapped overlap, absent rows, BM25 parameters, cosine compatibility and evidence separation. |
| Similarities | 115 → 152 | 320 → 324 | Same-grain self-query, term retention, reciprocal summaries, canonical pair versus presentation direction, per-source limit and lexical limitations. |
| SearchDocuments | 101 → 155 | 357 → 347 | Multiple evidence roles, duplicate-discovery example, RRF model, lexical-only baseline, feedback context and reranking only existing candidates. |
| SearchFields | 110 → 166 | 443 → 393 | Four request forms, supported boolean rules, positional phrase formula, parent/child delegation, qualification versus rank bound, all three match scopes. |
| SearchSimilarity | 93 → 151 | 368 → 324 | Known-source discovery, missing-lane behavior, RRF model, vector provider versus adapter, document grain, per-source limit and presentation ownership. |

Counts exclude headings and model blocks. Shorter Solutions remove repetition and dense wording, not the distinctions
in the final column; all fifteen existing conceptual model blocks in these families remain unchanged. Problem adds
context before difficulty rather than becoming an implementation checklist. No numbered-variant prose was copied.

### Feedback fixes and shared rules

- Cohorts now defines Feedback in its opening narrative and glossary, alongside the supporting Context key and Priority
  concepts. The glossary review begins with Intent, Problem, and Solution rather than only the implementation inventory.
  S4a requires that review and an entry for each essential reusable concept even when it is also defined inline.
- General style and S4b distinguish a term's defining occurrence from ordinary mentions. Cohorts uses `*band*` where
  the meaning is introduced, but leaves profile and later mentions plain. Definition sentences are not italicized as
  a whole, and exact program names retain inline code. Glossary names remain bold and alphabetized.
- Relevance now uses inline mathematics throughout its symbol introductions and interpretation: `$L_d$`, `$M_P$`,
  `$D_d$`, `$\alpha$`, and the other references no longer appear as literal subscript names or Greek-name substitutes.
  Notation, Format, its template, and F13 distinguish inline symbol references from complete display calculations.
  Removed the conflicting blanket prohibition on inline-dollar mathematics.
- Rechecked the four other earlier pilot chapters for the same rules without rewriting their accepted narratives:
  Clicks, Labeling, Training, and Indexing received selective definition emphasis and glossary additions; Clicks and
  Training also received inline symbol markup. The six earlier families retain their approved narrative development.
- Reviewed the opening concepts and glossary for all twenty families. Added still-needed supporting concepts, retained
  existing coverage, and consolidated Scoring's Overlap/Weighted overlap synonyms under the more precise latter name.
  Counts were not used as glossary targets. Fields and Evaluation now omit their step-only Stages inventories, leaving
  method groups, shapes, and independent roots intact under the previously established rule.

### Verification and limits

All sixty Draft/Extend/Form files were read back after their edits and compared with exact intended changes, normalizing
line endings. Across all twenty families, narrative prose agrees between phases after the existing text-model/math
presentation difference. The twenty conceptual models and all 289 Form display-math blocks are unchanged. The entire
Implementation body and Code match their pre-edit content, preserving stage descriptions, methods, numbering, Results,
return definitions, and Python listings. Intent is unchanged.

Checked glossary order and reviewed opening-concept coverage. Checked 101 inline symbol references in Form Problem and
Solution for balanced delimiters; scans found no remaining bare underscored mathematical names or literal alpha/beta
references outside math/code in those sections. Read their meaning against the adjacent formulas rather than relying
on delimiter counts alone. Inspected Cohorts' defining occurrences and Relevance's corrected interpretation directly.

This is focused narrative/typography QA, not a new certification of untouched implementation contracts or calculations.
No Typora or visual formula rendering, application build, or runtime tests were run. The Git-ignored chapter outputs
were checked directly and Git whitespace checks passed. Prose was authored manually and work remained sequential;
mechanical patch propagation and read-only comparisons did not generate prose.

## First-reading narrative trial and structural corrections — September 12

Revised Problem and Solution sequentially for Cohorts, Clicks, Relevance, Labeling, Training, and Indexing, checking each
family before proceeding. Authored the same narrative in Draft and Extend and preserved it in Form, including the
existing text-model/display-formula distinction. The trial changes eighteen phase documents, not collected source.

Problem now starts with the activity and basic concepts, then develops a familiar situation, the difficulty, and its
consequences. Solution uses the clear, purpose-first language of the accepted explanation items without importing their
sentence limits or italic intents. It remains a developed account of the approach rather than a shortened abstract.

| Family | Problem words, before → after | Solution words, before → after | Retained conceptual coverage |
|---|---:|---:|---|
| Cohorts | 92 → 156 | 439 → 402 | Overlapping bands, priorities, matching examples, context-key formula, catalog identity, broader/global fallback and consumer choice. |
| Clicks | 96 → 152 | 421 → 386 | Exposure versus response, display-day attribution, repeat actions versus replay, dwell-credit formula, propensity, watermark and caller responsibilities. |
| Relevance | 84 → 160 | 547 → 521 | Query feedback versus popularity, context, decay/propensity formula, dwell normalization and blend formula, CTR threshold scope, valid-input assumptions. |
| Labeling | 95 → 152 | 374 → 342 | Multiple labels, caller and pattern sources, worked question example, locale behavior, generated zeros, overlay precedence and downstream selection. |
| Training | 108 → 167 | 368 → 393 | Explicit judgments, candidate coverage, five features and repetition example, standardized linear formula, defaults, artifact application versus learning/promotion. |
| Indexing | 112 → 157 | 379 → 392 | Inverted lookup, normalization, postings, repetition/length/rarity, all four grains, positional metadata, keyword/text distinction and stop-word gaps. |

Counts exclude headings and model blocks and are observations, not targets. The modest Solution reductions remove
repetition and compressed phrasing without dropping the listed ideas. All five existing conceptual models and their
interpretations remain; no new ranking or statistical behavior was introduced. Reviewed time/count semantics against
Clicks source, feature/default semantics against RankDocumentCandidates, and band matching against ResolveCohortBands.

### Specific feedback

- Similarities Form repeated each pair-schema field vector on reversal after already showing its complete definition
  on canonicalization. The projection branch ran before the full-definition check. Moved that check first in Notation,
  updated Format and its template, and added the same-group regression to F6. Removed the four redundant vectors for
  DocumentSimilarityPair, SectionSimilarityPair, ParagraphSimilarityPair, and SentenceSimilarityPair; retained their
  first complete definitions and every method signature. Verified those definitions against inherited source fields.
  Partial projections still require explicit and essential fields until the schema has a full definition.
- Compared SearchDocuments Definitions directly with `close/2/form/.../SearchDocuments.form.md`. Restored the still-useful
  Band, User band, Candidate lane, Feedback option, and Fallback concepts, retained the newer concepts, and clarified
  basic query/request and retrieval terms. Its seventeen entries are alphabetized across Draft/Extend/Form. No other
  SearchDocuments content changed.
- Removed Stages from Cohorts, Clicks, and Relevance in all three narrative phases: these trial chapters have only step
  transforms, not child-stage calls. Kept every method in notation and Implementation. Labeling, Training, and Indexing
  retain their actual composition inventories. The shared model, templates, Draft operator, and D1/S1b now make Stages
  conditional; methods and independent step roots alone do not create stages.
- Alphabetized the existing definitions in all six trial families without dropping entries. General style and S4a now
  require alphabetical order and coverage review rather than a fixed glossary size. S3/S3a test the gentler opening and
  first-reading accessibility, with a conceptual coverage check separate from word counts.

### Verification and limits

Read the resulting files back and compared each with its intended edits. Across the six families, Draft/Extend/Form
narrative prose agrees after accounting for existing mathematical presentation. All 63 Form math blocks and 59 Python
listings per Code copy are unchanged. Implementation preambles, stage descriptions, method items, numbering, Results,
and complete Code sections match their pre-edit content. Definition changes are alphabetical reordering only in these
six families. The separate SearchDocuments and Similarities edits were checked against their own exact allowed changes.

This verifies twenty-two chapter files directly, including ignored `close/` outputs. Checks normalize line endings;
they do not claim byte-level line-ending preservation. Git whitespace checks passed. Prose was authored manually;
patch propagation and read-only comparisons did not generate it. No parallel agents, generation scripts, application
build, or visual formula render were used. Untouched chapters have not been reapplied to the new rules by this trial.

## Accessible transform-description rollout — September 11

Applied the approved Online/SearchDocuments revision to the remaining eighteen families, sequentially, with a
preservation check before moving to the next family. This supersedes the initial pilot's one-sentence preference:
internal introductions now use one or two connected, accessible sentences; external introductions describe the
actual call and supplied inputs, not preparation performed by the caller. Extend owns these descriptions and Form
preserves them. Draft has no stage subsections and was not changed.

| Family | Internal descriptions | External descriptions |
|---|---:|---:|
| Chunking | 2 | 0 |
| Fields | 1 | 0 |
| Indexing | 2 | 0 |
| Filtering | 1 | 0 |
| Inference | 5 | 0 |
| Vectorization | 3 | 1 |
| Scoring | 4 | 0 |
| Similarities | 2 | 3 |
| SearchFields | 3 | 1 |
| SearchSimilarity | 4 | 0 |
| Offline | 8 | 6 |
| Cohorts | 1 | 0 |
| Clicks | 2 | 0 |
| Relevance | 1 | 0 |
| Labeling | 2 | 0 |
| Training | 3 | 1 |
| Experiments | 6 | 1 |
| Evaluation | 8 | 0 |
| This rollout | 58 | 13 |
| Online, approved retry rechecked | 13 | 4 |
| SearchDocuments, approved retry rechecked | 3 | 3 |
| All twenty families | 74 | 20 |

The 71 new descriptions were authored individually from their Code descriptions and actual call contracts, without
generation scripts, parallel agents, or copying old chapters. Mechanical patch propagation and read-only comparisons
did not generate prose. Internal introductions remain unnumbered; external descriptions retain their circled markers
without italicized intents. Independent roots retain separate counters and their existing Result subtrees.

### Corrections and prompt coverage

- Updated the thirteen external-call explanations at Collect and propagated them through both Code sections, retaining
  the original short intents and decimal numbers. Experiments' annotated BM25-call description was synchronized too.
- Corrected Inference's two annotation paragraphs and corresponding collected/Code items to say that the inference
  adapter produces embeddings for queries or documents. Its two concise Form method explanations now use that wording
  too. The detailed Extend method explanations remain intact. Streaming still controls adapter execution; no source
  parameter, signature, or result contract changed.
- Preserved actual input selection: Offline passes its full supplied population to Vectorization, while Online passes
  selected gaps. Offline Scoring receives selected lexical queries and separately supplied vector queries. SearchDocuments
  passes document content and selected target IDs to OnlineVectorization, not an already-filtered document relation.
- Removed the false Implementation references to a Scoring-chapter definition of AllScoringTargets and a nonexistent
  Features chapter. The description algebra and template now make references conditional on actual chapter coverage;
  General style and S8c already require that check.
- Checked vector self-exclusion against ScoreVectors: document scoring excludes the source document, whereas paragraph
  scoring excludes the source paragraph, not its entire document. S9 now includes that regression case.
- S7/S7a cover accessible internal orientation, root-aware matching, caller/callee responsibility, external presentation,
  and upstream prose propagation. S9d covers input/artifact/execution distinctions. The approved retry's rules remain
  the common contract rather than being duplicated in family-specific instructions.

### Verification and limits

For each revised family, compared the full files with the intended paragraph edits and checked preservation outside
those edits. The 217 Python listings per collected/Code copy and all 231 Form display formulas are unchanged. So are
section trees, numbering, preambles, Results, short intents, and method-group explanations, except the two explicitly
listed Inference Form explanations. All comparisons normalize CRLF/LF; they are content checks, not line-ending claims.

Across all twenty families, Extend/Form descriptions agree exactly, Code sections agree, and collected intent-led
explanations match downstream Code after removing decimal prefixes and normalizing prose wrapping. The accepted Online
and SearchDocuments retry outputs were rechecked without rewriting them. The Git-ignored chapter outputs were read
directly; Git whitespace checks passed.

This is focused description and semantic QA, not full chapter regeneration or recertification of untouched formulas,
return schemas, or calculations. No visual formula render, application build, or runtime tests were run.

## Implementation transform-description pilot — September 11

Applied sequentially to Online, then SearchDocuments, in Extend and Form. The earlier concise projection covered
method-group explanations, not transform descriptions; the latter still encouraged a fuller input/operation/output
account. Added a distinct Transform descriptions contract in Implementation.style, routed Extend and its template
through it, and made Form's preservation explicit. S7/S7a now test concise orientation, correct Code-description
matching, internal/external presentation, and preservation outside this focused change.

Descriptions start from the corresponding Code class or call paragraph and explain the useful contribution in one
short sentence, with a brief chapter reference for external operations. Read SearchDocuments' `close/2/` descriptions
for clarity and reading load, not as behavioral authority. Kept current source distinctions, including the 10,000-target
bound, caller constraints, streaming inference outcomes, and reranking existing candidates rather than adding documents.
Matched Online's identically named SelectGapQueries classes within their separate filtering and scoring roots.

| Family | Internal descriptions | External descriptions | Description words, before → after |
|---|---:|---:|---:|
| Online | 13 | 4 | 471 → 251 |
| SearchDocuments | 3 | 3 | 244 → 95 |

Counts exclude circled markers and include chapter references; they are observations, not length quotas. Every
description was read without Code for a concrete, understandable role. External descriptions retain their existing
circled numbers and have no italicized intents; internal descriptions remain plain and unnumbered. Extend and Form
descriptions agree exactly. All other file content matches pre-edit snapshots, including method-group explanations,
headings, preambles, Results, full notation/formulas, and Code. Explicit checks also compared Python listings, display
formulas, and numbered method items; no changes were found. No Draft, annotation, collected source, or application code
was edited. Git whitespace checks passed. This is a description-only review, not a new visual render or runtime test.

## Annotation prose rollout — September 11

Completed the remaining eighteen established chapter families sequentially, with a family-level verification gate
before starting the next. Indexing and Scoring were excluded; their pilot outputs and annotations remain unchanged.
The inventory contains twenty families and eighty Draft/Collect/Extend/Form documents, so no additional established
family was left outside this rollout.

Revised code-adjacent explanation paragraphs at their annotation owner and carried them into Collect, Extend Code,
and Form Code. External-call explanations were refreshed at Collect from the actual source call; no external
implementation was imported. All 150 items were reviewed (130 method/helper groups and 20 external calls), with
already-suitable prose retained where rewriting added no value.

| Family | Code items | Python listings | Plain explanation words, before → after |
|---|---:|---:|---:|
| Chunking | 8 | 11 | 193 → 126 |
| Fields | 2 | 3 | 60 → 37 |
| Inference | 7 | 13 | 214 → 120 |
| Vectorization | 3 | 6 | 103 → 60 |
| Filtering | 4 | 6 | 109 → 61 |
| Similarities | 13 | 16 | 258 → 210 |
| SearchDocuments | 15 | 19 | 305 → 243 |
| SearchFields | 9 | 13 | 143 → 138 |
| SearchSimilarity | 7 | 12 | 101 → 106 |
| Offline | 12 | 20 | 166 → 175 |
| Online | 26 | 39 | 458 → 407 |
| Cohorts | 8 | 9 | 139 → 112 |
| Clicks | 2 | 4 | 52 → 46 |
| Relevance | 4 | 5 | 86 → 74 |
| Labeling | 5 | 8 | 118 → 95 |
| Training | 4 | 7 | 83 → 61 |
| Experiments | 6 | 12 | 109 → 79 |
| Evaluation | 15 | 23 | 265 → 231 |
| Total | 150 | 226 | 2962 → 2381 |

The counts exclude italic intents. They are not shortening targets: SearchSimilarity and Offline grew slightly where
clearer behavior or useful chapter references warranted it. Prose was authored family by family, without generation
scripts, parallel agents, or copying older chapters. Mechanical comparisons and patch propagation did not generate prose.

### Source and reading checks

- Checked each revised method-group explanation beside its complete listing and current Python source, including
  private helpers, inherited replacements, all grain paths, and method-local imports.
- Corrected Fields annotation's stale claim that flattening reads the completed map: its declared input is the original
  document map. Annotation A6 now requires tracing input lineage rather than inferring it from section order.
- Preserved consequential details: source spans, field-map precedence, inference failures, source-document identity,
  filter limits, request-relative freshness, and unordered deduplication rather than an invented newest-row guarantee.
- Made SearchFields' `metadata`, `content`, and `metadata+content` result scopes explicit. SearchDocuments' feedback
  item now states the actual fixed 80/20 blend, and its experiment counterpart retains the 90/10 change.
- Preserved the distinction between repeat clicks and clicked impressions, reported CTR and its thresholded blend
  contribution, generated-zero label precedence, judged metric eligibility, and batch association versus time filtering.
- Checked external assignments against current source. Removed an incorrect helper chapter reference during review:
  `AllScoringTargets` is not Offline's `AllDocumentTargets`. General style and S8c now require actual destination
  coverage, not an assumption based on package membership or similar names.

### Preservation and scope

For every family, Python listing contents, container headings, group membership/order, italic intents, and existing
independent Code numbering streams match the pre-edit snapshots. No internal class description became a numbered
step. Revised prose is consistent across its annotation owner and collected/extended/formatted copies, allowing only
Code prefixes and line wrapping. Form Code remains byte-identical to Extend Code.

The entire pre-Code body of every extended and formatted chapter is byte-identical to its snapshot, including
Implementation explanations, formula notation, return definitions, and preambles. Drafts and application sources
were not edited. A final cross-family read compared 148 phase/annotation files with the verified snapshots, including
the excluded Indexing and Scoring files; all matched.

This is focused Code-prose QA, not certification of untouched chapter content. In particular, the pre-existing
Similarities Implementation reference claiming that the Scoring chapter defines `AllScoringTargets` remains outside
this pass; the current Scoring chapter does not expand that helper. No formula rendering or application build was run.
Ignored `close/` outputs were checked directly, and Git whitespace checks passed.

## Annotation prose pilot — September 11

Scope: Indexing, verified before proceeding to Scoring. Revised the method-group explanatory paragraphs in their
existing annotations, then propagated them into collected, extended Code, and formatted Code. This is a focused
Code-prose revision, not chapter regeneration. Draft and all pre-Code content remain unchanged.

### Diagnosis and rule change

The reported long paragraphs are already present in `LexIndex.anno.md` and the lexical `ScoreOverlap.anno.md`;
downstream Code faithfully carries them. The outputs are Git-ignored, so their regeneration history cannot be
established from commits. Current filesystem modification dates for those annotations are September 6, not proof
of a September 5 run. Reviewed prompt history and the changes in `7a4ab5d7` and `d9dd5006`; those changes do not
establish a new instruction to lengthen explanations.

The control gap was competing emphasis: repeated brevity advice alongside requests to describe workings, explain
points, and mention grain differences, without a concrete test of explanatory value beside visible code. Replaced
that scattered guidance with the Annotation operator's purpose-first, usually-one-sentence contract and A1–A8 checks.
The contract explicitly separates code-adjacent prose from chapter narrative depth and retains consequential details.
Removed the old permission to insert numbered callouts into Python, which conflicted with exact-source preservation.
Updated Prose's operator reference and linked the Annotation checks from shared QA.

### Results

| Family | Method/helper groups | Python listings | Plain explanation words, before → after |
|---|---:|---:|---:|
| Indexing | 23 | 26 | 775 → 314 |
| Scoring | 17 | 23 | 466 → 280 |

Counts exclude short italic intents and are descriptive, not acceptance targets. Compared with `close/2/` Code prose
for purpose-first phrasing and reading load, while checking every revised claim against current source. The pilot
retains the distinction between document frequency and repetition, field positions before stop-word removal,
the ten-minute watermark, missing-term weights, BM25 defaults, normalization populations, and vector validation failures.
Removed repetitive key hierarchies and generic downstream-use explanations. Kept the already-concise overlap guard.
Older Scoring group omissions and unseparated listings were not adopted with its narrative style.

For each family, checked all selected annotation blocks against the current Python source and the complete ordered
method inventory, including private helpers: 23 Indexing methods and 35 Scoring methods. All blocks match source.
Compared against snapshots taken before editing: Python block contents, headings, group membership/order, short
intents, and Code numbering are unchanged (Indexing 1–23; Scoring 1–17). Every revised paragraph agrees between
annotation and Collect after whitespace normalization; Extend differs only by its Code prefix and wrapping;
Form Code is byte-identical to Extend Code. Both chapters' entire pre-Code bodies are byte-identical to their snapshots,
including Implementation explanations, formulas, return definitions, and preambles. One surplus trailing blank line
was removed from the LexIndex annotation.

Reviewed neighboring paragraphs for accessible language and repetition after the mechanical checks. No family was
parallelized, no chapter was copied wholesale, and no prose-generation script was used. Read-only comparisons checked
the hand-authored edits and their propagation. Git whitespace checks passed; ignored outputs were checked directly.
This pilot does not certify untouched chapter content, other annotation modules, visual rendering, or runtime behavior.
No application code changed and no application build was run.

## Fourth reading-prose batch — September 10

Sequential scope: Cohorts, Clicks, Relevance, Labeling, Training, Experiments, Evaluation.

### Completion check — September 11

Both requested batches are complete: SearchDocuments/SearchFields/SearchSimilarity/Offline/Online and
Cohorts/Clicks/Relevance/Labeling/Training/Experiments/Evaluation. Families were edited sequentially and checked before
the next. Final checks also include the feedback changes to Scoring and Similarities: all fourteen families preserve
Code exactly, retain Extend's non-projected prose, agree between Draft/Extend openings, and have one formula per
text-notation block. Display environments balance; no text/LaTeX notation fences or reduced-size commands remain in
Form's pre-Code body. Fixed three missing blank separators before Online formulas during this check.

The four available `3/` family references retain identical Implementation heading trees in SearchDocuments,
SearchFields, SearchSimilarity, and Offline. Solution depth is retained rather than compressed (approximate word
counts including notation: 179→363, 394→452, 302→374, 147→331 respectively). Reference behavior does not override current
source: notably SearchDocuments keeps its current single feedback-option method. Later requested families have no
corresponding `3/` references here. No old chapter was copied wholesale. Source-based return ledgers and family checks
are recorded below; these are not a visual rendering test or a runtime test. Git whitespace checks passed; ignored
close/ outputs were checked directly. No application code or collected Python listings changed in these batches.

The actor-language feedback was also applied to Fields, Inference, Filtering, and Similarities; genuine passage-reading
examples in Chunking remain appropriate. Prompt changes are confined to the owning narrative contracts and QA:
precise central calculations, first-reading rationale, accurate actor names, explicit external-chapter references,
and preservation of observable discriminator values in concise explanations.

### Cohorts

Added Intent and a developed Problem about overlapping context and sparse feedback. There is no `2/` Cohorts draft;
used current source and scope. Retained Solution's concrete locale/device example, context-key formula, catalog-version
qualification, and fallback interpretation. Eight Form groups now use concise Code-grounded explanations; null
user_band_id, band_id, and user_band_fallback_id meanings remain explicit. Complete preamble and Code are preserved.
All 14 public methods, 8 items, 10 formula/text blocks, and the sole step shape remain, with no synthetic Result.
First-return ledger is complete: Band 12, BandMatch 4, BandAncestor 3, UserBandPath 2, UserBand 2, SingletonUserBand 3,
UserBandMembership 2, BandMembership 3, BandFallback 3. Checked against ResolveCohortBands and both schema modules;
pass-through and helper-produced first returns are fully defined. Code/preamble/opening parity passes.

### Clicks

Added Intent and a developed Problem distinguishing exposure, response, replay, and cross-day attribution. Retained
Solution's midnight example, click-versus-clicked-impression distinction, explicit dwell-credit formula and thresholds,
and the complete preamble. The two independent step roots retain one numbered group each, no Result, two full return
definitions (DailyImpressions 8; DailyClicks 12), and five mapped formula/text blocks. All explicit projected fields
and daily grain keys match Impressions.py, Clicks.py, and clicks schemas. Two concise Form explanations preserve the
24-hour attribution, exposure day, response measures, and null band meaning; Code is untouched. Parity checks pass.

### Relevance

Added Intent and a developed Problem about position bias, age, sparse contexts, and interpretable evidence. Retained
the weighted-CTR model and added a precise log-dwell normalization, exposure gate, and direct weighted-blend model.
The new explanation defines populations and parameters and states denominator/propensity/half-life assumptions without
inventing guards or normalized configuration weights. The gate affects normalized_ctr_score, not reported CTR.
Four Form groups now derive concise prose from Code, preserving global null context and non-deduplicating expansion.
All 12 methods, four items, one step shape, and seven formula/text blocks remain; Code and preamble are unchanged.

First returns checked against BuildRelevanceSignals and relevance/clicks/build schemas: ContextDailyImpressions 8,
ContextDailyClicks 12, QueryDocumentSignalTotals 17, DocumentPopularityTotals 16 fields. Public QueryDocumentSignals
and DocumentPopularity projections show all five explicit rate/normalization overrides plus context, exposure, and
weighted-dwell/CTR evidence, eliding unchanged raw activity totals. The first returns are nonempty and essential keys
remain visible. Formula/text and prose parity pass. The new calculation QA covers this case without another rule.

### Labeling

Added Intent and a developed Problem about hidden query-population regressions and inconsistent label meaning. Kept
Solution's configured pattern example, missing-locale default, zero-versus-absence distinction, and exact overlay
precedence. Five Form items are now concise, with value=0, en_US, generated-zero precedence, labels, and both derived
flags explicit. The raw matcher remains in its group and shape with its declared data signature; runtime arguments
and method-local imports remain in unchanged Code. All 11 methods, five groups, eight formulas, two shapes and Result
remain; assigned stage dependencies and exact preamble/Code/opening parity pass.

First-return ledger: Intent 2, IntentPattern 3, QueryIntentLabel 6, QueryLabelAssignmentEntries 2, QueryLabelAssignments 2,
QueryLabel 3 fields fully defined. SearchQuery projection shows id, labels, is_question, is_time_sensitive, eliding
unchanged query context and preserving all explicit overrides on both merges. Checked both source transforms and
label/search schemas. No new QA exception needed.

### Training

Added Intent and a developed Problem about judged examples and consistent feature meaning. Retained the feature
example and standardized linear formula, adding the nonzero-scale assumption without claiming a validation guard.
Two concise public Form explanations preserve exclusion of unjudged pairs and the exact score_rank/experiment_id
assignments. The composed Training root retains its external Features stage and Result; the independent ranker retains
its step shape and fresh counter. Its private scoring helper remains Code-only, so Code has an extra group.
Seven formula/text blocks, three Implementation items, and both public methods remain. DocumentTrainingData first
defines all eight fields; the first candidate projection retains identity, lexical score, score_rank and experiment_id
while eliding unchanged retrieval context. Checked BuildTrainingData, RankDocumentCandidates and schemas. Exact Code,
preamble, and opening parity pass.

### Experiments

Added Intent and a developed Problem separating variant effects from population differences. Retained the BM25
parameter explanation, explicit 90/10 feedback calculation/example, production/null identity, and causal limitations.
Five concise public Form items retain active/null selection and observed-request versus expanded-query populations;
the external BM25 item remains circled/plain with its Scoring reference. Five independent roots keep local counters;
the exact-base-plus-replacements formulas retain both composed Results and three specialized/ordinary step shapes
plus the internal reranker shape. All eight local public methods, six items, and twelve mapped formula/text blocks
remain. Code and preamble are unchanged.

First-return ledger: DocumentScore 6, SectionScore 7, ParagraphScore 8, SentenceScore 9, EvaluationQuery 5, and
BehaviorRequest 7 fields fully defined. Candidate projection retains all five overrides plus query/user-band/candidate
identity; EvaluationResult retains four overrides and rank/grade while eliding inherited window/params. Source checks
cover experiment selectors and both evaluation specializations; later inherited metrics are not claimed to gain new
isolation guarantees merely from the local result join. Parity checks pass.

### Evaluation

Added Intent and a developed Problem distinguishing quality, engagement, incomplete judgments, and empty served lists.
Retained the precision/recall/DCG/nDCG example and model; added exact success/reciprocal-rank eligibility and defined
the inverse-propensity long-click and dwell rates with their denominators and null behavior. Fourteen concise Form
items derive from their owning Code groups, not coincident item numbers. Eight independent roots retain separate
counters, both base inventories, six exact-base selector specializations, and no invented Result.
All 24 public methods (8 judged-base, 10 behavior-base, 6 selector replacements), 14 items, 8 shapes, and 24 formula/text
blocks remain. The private eligibility helper group remains Code-only. Code and preamble are unchanged; parity passes.

First-return ledger checked against judged_quality.py/behavior.py and returned constructions: EvaluationQuery 5;
EvaluationResult projects context/query/document/rank/grade; EvaluationJudgment projects query/band/grade/ideal_rank;
EvaluationJudgmentTotals 6; EvaluationIdealDcg 8; EvaluationResultTotals 18; DocumentQueryEvaluation 25;
DocumentEvaluationSummary 23; BehaviorRequest 7; BehaviorImpression 15; BehaviorRequestTotals 17;
BehaviorRequestMetrics projects request/model/context/first-long-click/reciprocal-rank and raw counts;
DocumentSearchRequestBehavior 15; BehaviorExposure 8; BehaviorDailyCounts 18; DailyDocumentSearchBehavior projects
daily context, request/empty/clicked/long-clicked request counts, and both explicit IPS rates. Unchanged context or
activity details are elided only on the listed projections. Added the daily request counts to preserve the published
population alongside its rates. Existing F6 covers this correction. Formula checks are structural, not visual renders.

## Third reading-prose batch — September 10

Sequential scope: SearchDocuments, SearchFields, SearchSimilarity, Offline, Online.

### SearchDocuments

Added Intent and a two-paragraph Problem informed by `2/`'s recognizable, bounded result-page framing. Solution keeps
its account-recovery example, RRF model and lexical-only fallback, feedback role, and presentation coverage, with a
plainer description of the retrieval sequence. Eleven internal Form explanation groups now derive concisely from Code;
three external descriptions remain plain and circled. Preamble, method coverage, and Code are preserved.

Return ledger: DocumentSearchCandidate (21 fields), DocumentFeedbackOption (explicit fallback fields plus candidate
identity and query; other candidate context elided), QueryDocumentFeedback (6), PopularityFeedback (6), and
DocumentSearchResult (rank, identity, title/URL, and ranking score on first projection; full 19-field definition on
publication). Added title/URL to the first result projection so the defining presentation payload is not hidden.
Checked against search.py and rerank return constructions, retaining every explicit projection field. The 10,000,
1,000, and 100 bounds remain in their existing roles. Final prose names score versus retrieval_score to avoid
misdescribing the normalization numerator. Existing F6/F12 cover these cases without an additional exception.

### SearchFields

Added Intent and a two-paragraph Problem using `2/`'s contrast between the reader's clues and differing matching rules.
The developed Solution already explains all four query examples, phrase offsets, candidate restrictions, and result
scopes accessibly, so it is retained. Eight internal Form explanations now use concise matching Code prose; the
external SearchDocuments item stays plain and circled. The preamble retains the 10,000-target bound and explicitly
distinguishes the desired field-qualified ranking from the current unrestricted-rank limitation.

Return ledger checked against field_search.py, delegate.py, publish.py and schemas: term match (7), clause match (4),
document match (4), field query (12), delegation (2), document target (3), and field result (4) are fully defined.
SearchQuery projects id/content/labels, omitting unchanged request context; SearchRequest projects id/query_id/query,
omitting user/experiment/version/time context. Explicit overrides and defining payload remain visible. All 13 public
methods, 9 items, 14 notation/formula blocks, three step shapes and the four-call Result are present. Result dependencies
use resolved/delegation/delegated producers correctly. Code and preamble equal Extend; opening sections agree across
all three phases. Existing acceptance rules suffice; no prompt change was needed.

### SearchSimilarity

Added Intent and developed the Problem from `2/`'s complementary-neighbor/noisy-ordering framing, retaining the current
document-grain scope. The accessible Solution retains its discovery example, two-lane explanation, interpreted RRF
model, defined adapter, missing-lane behavior, and presentation tradeoffs. Seven Form explanations now use concise
Code-grounded prose; the detailed Extend items, complete preamble, and Code remain unchanged.

Return ledger: DocumentFusedSimilarityCandidate is fully defined with 16 fields; SimilarityFusionPolicy with 5.
IndexedSimilarDocument first exposes all explicit ranking/provenance fields plus id/title/url, eliding inherited
document context; its later pass-through supplies the full 30-field definition. The repeated score projection retains
rrf_score and the pair/rank context. Checked constructions in adopt.py, fusion.py, rerank.py against current schemas.
All 11 public methods, 7 items, 13 formula/text blocks, four step shapes, and the four-call Result remain. Fusion reads
distinct lexical/vector producer outputs; reranking reads fused.document_candidates. Code, preamble, and opening
parity pass. No additional prompt rule was needed.

### Offline

Added Intent and a developed Problem about selecting changing demand and interpreting aged, scoped artifacts, informed
by `2/` without its synthetic sequential workflow. Retained the developed Solution and component-level preamble;
shortened six internal Form groups from their Code explanations. External descriptions now identify their defining
chapters. Three independent composed roots retain their own Results and independent Implementation/Code sequences:
OfflineFiltering 2 items, OfflineVectorization 5, OfflineScoring 5. No package Result or imported external methods.

Return ledger: DocumentSearchTarget first defines all 3 fields; QueryPopularity all 2; PopularQueryCandidate shows
id/content/impression_count/popularity_rank with unchanged query context elided; SearchQuery first defines all 8
fields at the pure projection from PopularQueryCandidate. Subsequent returns reuse those definitions. Source checks
cover the seven public methods, explicit scope/rank assignments, implicit AllDocumentTargets method, and current
popular/recent selection. The 10,000 target bound, 1,000 popular-query default, and inclusive 0–7-day recent window
remain distinct. Formula/text mapping and exact Code/preamble/opening checks pass; the three Results preserve their
actual assigned producers. Detailed Extend and collected Code are retained.

### Online

Added Intent and a two-paragraph Problem informed by `2/`'s request-budget and incompatible-artifact framing, without
claiming a mandatory parent workflow. Retained the developed Solution, its interpreted freshness formula and example,
and the complete preamble. Twenty-one public Form explanations now derive concisely from Code; four external items
remain plain/circled with chapter references. Three roots retain 5/7/13 Implementation items and 5/7/14 Code groups;
the private freshness helper accounts for the final stream difference. No numbering was synchronized.

First-return ledger checked against filtering.py, search.py, text.py, indexing/vector.py, and scoring/intermediate.py:
FilterQueryAvailability (1), SearchQuery (8), DocumentFilterScore (5), DocumentSearchTarget (3), Document (19),
SearchQueryVectorEmbedding (6), DocumentVectorIndex (6), ScoreQueryAvailability (1), DocumentScore (6),
DocumentVectorScore (11), ParagraphVectorScore (15). All are fully defined at first return, including pass-throughs;
the target projection keeps its explicit scope_id. Source predicates confirm query identity checks versus document
numeric validation, unordered deduplication, query-wide invalidation, and paragraph keys. All 23 public methods,
39 notation/formula blocks, ten step shapes, and three assigned-call Results remain. Code, preamble, and opening
parity pass. No visual renderer was used; formula verification is structural and source-based.

### Mid-batch feedback regression

Scoring now defines weighted overlap, BM25, and cosine as well as IDF, with symbols, parameters, examples, and source-backed
missing/zero behavior. QueryToken deduplicates before normalization: collisions after normalization can contribute
multiple matching rows, so the overlap definition includes that multiplicity and does not promise a unit bound.
Checked ScoreOverlap, ScoreBm25, ScoreBase, QueryToken and Vectors. Scoring now has 26 mapped formula/text blocks,
with 17 Implementation groups and unchanged Code/preamble. Similarities external stages explicitly refer to Scoring;
SearchDocuments explains the common-language rationale for Reciprocal Rank Fusion; SearchFields preserves all three
match_scope values in concise publication explanations. Generic search actors are named as users in the touched prose.
Added shared calculation/actor/chapter QA and a discriminator-retention check to the owning style contracts and QA.

## Second reading-prose batch — September 10

Sequential scope: Inference, Vectorization, Filtering, Scoring, Similarities. Each family is checked before the next.

### Inference

Added Intent and a two-paragraph Problem using `2/`'s lifecycle/failure framing without its obsolete guarantees.
Solution retains its example, geometric model, adapter explanation, compatibility, reuse, and fallback coverage;
the compatibility paragraph now uses concrete language. Seven Form explanations derive from their Code groups.
Verified seven first-return definitions against inference.py and indexing/vector.py: policy (7 fields), query and
document adapter results (5 each), query and document embeddings (6 each including inherited fields), and statuses
(8 each). No return expansion was needed. All fourteen formula blocks, seven items, five step shapes, and Result
remain present; producer-qualified publisher inputs remain distinct. Code and preamble equal Extend. Detailed Extend
items are retained. The strengthened F6/F12 checks cover this case without another prompt exception.

### Vectorization

Added Intent and a developed Problem contrasting reusable representations and request/source identity. Solution keeps
its model, lifecycle, and responsibility coverage while defining embeddings plainly and adding ordinary-versus-source
request examples. Used by now names Online and Offline chapters rather than their component classes. The two Form
method explanations retain the concrete query_document_id distinction. First DocumentVectorQuery return now exposes
all seven fields, including inherited compatibility metadata; the later projection retains vector and both explicit
identity assignments, eliding already-defined compatibility fields. Verified against both binder implementations and
indexing/vector.py. Three independent numbering roots remain: one external Inference call and two step binders.
Seven formula blocks, nested Result, two step shapes, exact Code, and unchanged preamble pass review. Existing F6/F12
and principal-topic rules address the defects; no new special case was introduced.

### Filtering

The first batch already applied Intent/Problem and concise Form prose. Rechecked current FilterOverlap and filtering
schemas rather than rewriting accepted prose. QueryTerm has both fields; DocumentFilterMatch exposes all four on
first return and on rank projection; DocumentFilterScore exposes all five, including scored_at. Four method items,
seven formula blocks, step shape, and composed Result remain correct, with exact Extend/Form Code. The 10,000 bound
and timestamp-versus-freshness distinction are retained. No document or prompt change was needed for this family.

### Scoring

Added Intent and a developed Problem using the `2/` framing of distinct relevance clues. Solution keeps the current
source-backed IDF formula (not the different older formula), defines grain before use, and adds a concrete two-term
example distinguishing coverage from repetition and length. Seventeen Form explanations are concise Code-derived
accounts. Inherited query preparation resolves to ScoreBase's Code groups, not the similarly numbered private overlap
or BM25 helper; clarified that ownership rule in Implementation.style and F12 without altering Code numbering.

Return ledger covers QueryTerm, QueryTermCount, four posting schemas, QueryTermIdf, QueryIdfTotal, four overlap-match
schemas, four overlap-score schemas, four BM25-score schemas, four selected-score schemas, VectorIndexPolicy, and
document/paragraph vector scores. All 27 first schema returns already expose their full fields, including inherited
grain keys, scope, and contributed values. Repeated full definitions may remain named references. Verified schemas
against lexical index, scoring, search, and vector declarations; no field expansion was required. All 23 formula
blocks, 17 items, four step shapes, composed Result, exact Code, and unchanged preamble remain present. Private Code
helpers remain outside Implementation, and every lexical/vector grain path remains in its signature and shape.

### Similarities

Added Intent and a developed Problem about same-grain neighbors and directional evidence. The `2/` chapter's broad
lexical/vector discussion is not current implementation scope, so its accessibility informed the writing without
reintroducing hybrid retrieval duties. Solution retains the aurora example, both symmetric-summary formulas, direction,
pair identity, and local neighbor limits; a plain A/B explanation now introduces canonical pair identity.
Shortened the longer Form public groups from their Code explanations, retaining already-concise groups unchanged.
External calls remain plain circled descriptions, while Code preserves its independent intent-led numbering.

Return ledger: SimilarityPolicy; four grain query-text schemas; SearchQuery; four typed source-query schemas; and
candidate, pair, and ranked similarity schemas for each of four grains (22 distinct first schema returns). All have
field definitions, including complete source/target identities and directional scores. Reversed pairs retain every
explicit identity/score assignment and elide only the already-defined unchanged overlap value. Checked against
similarity.py, similarities/intermediate.py, SearchQuery, and query/reducer return constructions. No formula expansion
was required. Sixteen formula blocks, twelve Implementation items, thirteen independent Code items, two step shapes,
three external boundaries, and Result remain present. Code and preamble equal Extend.

Corrected the misleading phrase "inclusive (0, 1]" to "greater than zero and at most one" in annotation, collected,
Extend, and Form. Source allows null or that numeric range. This is the sole Code-prose correction in the batch;
listings and group boundaries are unchanged. Existing predicate-based S9 applies; no topic-specific QA exception is needed.

### Batch completion

All five families pass opening-section parity, Draft/Extend Solution equality, Form Solution equality after model
typography conversion, exact Extend/Form Code equality, and preamble preservation. Solution prose remains developed:
approximately 383, 353, 377, 355, and 320 words respectively, excluding models and headings. Return ledgers above were
checked separately from formula counts; preservation alone was not treated as correctness. Prompt whitespace checks
pass. No application code, historical variants, generation scripts, or parallel agents were used; verification was
source-based and structural, not a visual math render or runtime test.

## Return-definition and concrete-reference correction — September 10

The reading-prose pilot preserved existing formulas rather than validating their return fields. That preservation
check could not detect preexisting defects. Notation's project/base branch also permitted a bare schema name when
explicit and selected fields were empty, even on the first return occurrence. Removed that loophole and reinforced
Format/F6 with a first-return ledger, per-method checks within groups, and source-backed essential-field review.

Chunking's ledger, in method order: MarkedDocumentLine (11 fields), ParagraphLine (7), SectionHeading (5),
ParagraphLineGroup (7), ParagraphContent (7), ParagraphDraft (7), Paragraph (6), SectionKey (6), Section (7),
MaterializedParagraph (content visible; six unchanged Paragraph fields elided), Sentence (8). All first schema returns
now have definitions. The grouped-paragraph record includes document_id, section_ordinal, and paragraph_group as well
as every explicit assignment. These small records retain all essential fields without unnecessary ellipses.
The materialized-text projection still illustrates legitimate elision of already-established, unchanged context.
The non-step helper retains its complete list/dict return type; it is not a schema-class return.

Implementation.style and F12 now require selective concrete references where generic nouns obscure the value being
discussed, without requiring field names in most items. Fields' two Form items identify Document.fields; flattening
produces "a DocumentField row per non-empty key." Code and detailed Extend prose are not rewritten by this correction.

Verified Chunking against current intermediate/text schemas and method return expressions: twelve formula blocks,
eight items, unchanged method inventory and Code. Fields retains both items and its original formulas and Code.
This is source/structural verification; no visual render or runtime-test claim is made. The unchanged-formula statements
in the earlier pilot record describe that earlier pass, not this intentional notation correction.

## Intent and reading-prose pilot — September 10

Scope: Chunking, then Fields, Indexing, and Filtering, each reviewed before starting the next family. The accepted
change adds Intent before a developed Problem and allows Format alone to shorten public Implementation explanations
from matching Code groups. Detailed Extend explanations and collected/Code content remain knowledge references.
Older `close/2/` chapters supply problem framing and accessible style, not implementation authority. No generation
scripts or parallel family work are used. Formula rendering is unchanged; checks below are structural, not visual.

### Chunking

Reviewed `2/` Problem, Solution, and all eight Implementation items. Restored the passage-versus-context problem in two
paragraphs, while retaining the former one-sentence Problem as Intent in Draft/Extend/Form. Solution still explains
the hierarchy, span model and numerical example, replaceable segmentation, and downstream value; simpler language
introduces grain and ordinal where used. Solution volume is maintained (approximately 364 to 374 words including
the unchanged model). All eight Form explanations now use concise versions of their matching Code groups.

Verified all three opening sections, eight unchanged markers/intents, twelve unchanged formula blocks, and the complete
Extend/Form diff. Preamble and Code remain identical between Extend and Form; baseline hashes also confirm unchanged
Form Code, formulas, and collected file. The two internal stages, twelve public methods, two shapes, and composed
Result remain covered. Detailed Extend explanation paragraphs were not changed. Shared S3/S8a and Format F12 enforce
the new distinctions; the operator, model, and templates agree on the narrow prose exception.

### Fields

Reviewed `2/` for the typed-versus-mapped metadata problem and its two short explanations. The new Problem also makes
cross-field phrase ambiguity concrete. Solution keeps all five paragraphs, the title-precedence example, text/keyword
distinction, analyzer rules, and aggregate metadata behavior; it explains the terminology with familiar words rather
than reducing coverage (approximately 310 to 329 words). Both Form explanations derive from their Code groups.

Verified the complete Extend/Form diff, three unchanged formula blocks, both unchanged markers/intents, unchanged
Code and collected-file hashes, opening-section parity, and exact preamble preservation. Fields remains a step main
with two methods and one shape, without Workflow or Result. The current preamble retains the important distinction
that both methods read the supplied documents; the obsolete `2/` account of a sequential completion/flattening path
was not restored. Detailed Extend items remain intact. No additional exception beyond S3/S8a/F12 was needed.

### Indexing

Reviewed `2/` Problem and all 23 explanation items. The new Problem develops repeated preparation cost and the need
for consistent term interpretation and statistical scope. Solution retains all five paragraphs, rarity/repetition/
length theory, the aurora example, all four grains, positional metadata, and ranking independence (approximately
352 to 379 words). It now introduces normalization, posting, and grain directly in the explanation.

All 23 Form items map to the corresponding LexIndex or FieldIndex Code group. The 22 lexical explanations use one
sentence each; FieldIndex uses two to retain both text/keyword behavior and the reason for preserving positions.
Verified unchanged markers/intents, all 26 formula blocks by baseline hash, both shapes, the composed Result, and
all Code and collected content by hash. After accounting for the declared presentation and explanation substitutions,
the complete Extend/Form text and tree match. Draft/Extend Solution and all opening sections match; preamble is
unchanged. No per-grain method, return field, or notation layout was removed to shorten prose. S8a/F12 suffice.

### Filtering

Reviewed `2/` Problem and four explanation items. The restored Problem explains the cost/coverage tension without
prescribing overlap counting. Solution retains its five paragraphs, worked query example and intersection model,
limitations of shared-word selection, and separation of production from reuse (approximately 368 to 381 words,
including the model). The language now explains candidates and selection directly rather than using abstract
admission terminology throughout. All four Form explanations use their matching Code groups.

Verified the complete Extend/Form diff, seven unchanged formula blocks and four unchanged intents/markers, unchanged
Code and collected-file hashes, opening-section parity, and exact preamble preservation. The 10,000 publication bound
remains in both preamble and the shortened publication item. The old `2/` claims that Filtering owns freshness checks
and target publication were not revived: current Filtering produces DocumentFilterScore, while Online owns reuse.
Detailed Extend explanations remain intact; existing S9 plus the new S3/S8a/F12 cover these distinctions.

### Shared language cleanup and final checks

Capitalized definition sentences in the eight affected current families across Draft/Extend/Form: Inference,
Vectorization, Scoring, Similarities, SearchDocuments, SearchFields, SearchSimilarity, and Offline. Updated prose
references to explicitly say chapter, using the Online chapter for its OnlineFiltering/OnlineVectorization/OnlineScoring
subsections. Where the reference occurred in Code, corrected collected prose too; the experiment annotation also
carries the same wording. This was mechanical language maintenance, not a narrative regeneration of those families.

The final scoped Definitions scan found no lowercase sentence starts. All twenty current Form Code sections still
equal their Extend Code sections. No bare "See Topic" chapter references remain in current Draft/Collect/Extend/Form.
Prompt diff whitespace checks pass. Intent/Problem and concise Implementation changes remain limited to the four
pilot families; historical `2/` and `3/` files are untouched. Their old structure is not the acceptance oracle for the
new Intent section or the intentional Extend/Form explanation difference. No application code or formula layout was
changed, and no visual rendering or runtime test claim is made.

## Filtering family — September 10

The family had annotations and `close/3/` collected references but no current Draft/Collect/Extend/Form files. Created
all four under `search/transforms/filtering/`. Filtering is a sole composed main with one internal FilterOverlap stage,
four public methods/groups, one step shape, and one concluding Result. Workflow appears first only in Code; neither
class introduction consumes a number. Both independent item streams contain four correctly placed intent-led items.

Seven Extend notation blocks map to seven Form displays: the Solution intersection model, four signatures, step shape,
and Result. The Result retains the `overlap` assignment, three positional relation references, all four method names,
unqualified stage output, typed final output, and no parent name. Return records show both QueryTerm fields, all four
DocumentFilterMatch fields, and all five DocumentFilterScore fields in schema order; no fields are hidden unnecessarily.

Compared both `close/3/` collected references with current source. The older aggregate contains a removed
select_distinct_query_terms method, DocumentIndexTerm/token instead of DocumentTerm/term, and cross_join instead of
param_join. None was reintroduced. Restored exact method indentation in annotation and placed the match explanation
in its single intent-led paragraph before the listing. The current collected-to-Code diff contains only heading depths
and four prefixes. The complete Extend/Form diff contains only Definitions, Stages typography, and notation changes;
the five-paragraph Solution, five-paragraph preamble, and Code are preserved. No prior narrative phases exist in `3/`.

Source-backed distinctions include deduplication before normalization versus distinct matching afterward, no rows for
empty/unmatched queries, the publication-only 10,000 bound, and timestamp production versus Online cache validation.
Existing narrative/coverage checks cover those behaviors. Added S0 family reconciliation to catch a family missing
from a batch despite its appearance as a stage in other chapters. Structural/manual QA only; no scripts, agents,
application changes, execution tests, or visual math-render claims.

## Evaluation family — September 10

Completed Draft/Collect/Extend/Form and repaired all eight annotations. Two fully expanded step roots provide eight
judged-ranking and ten behavior methods. Six independent selector specializations add one local override each, for
24 public declarations and four private helpers. There is no composed parent, child call, Workflow, or Result.
Every root has one step shape. Implementation has four items per base and one per selector (14); Code has five judged
base groups, four behavior base groups, and one per selector (15), with private eligibility helpers numbered only in Code.

Twenty-three Extend notation blocks map to 23 Form displays:
1 Solution + 5 judged base + 6 judged selectors + 5 behavior base + 6 behavior selectors = 23. Each public signature and
parallel cutoff field is present; every explicit projection addition/override was checked against source. Full
EvaluationQuery, BehaviorRequest, exposure, and summary constructions retain their required field definitions; later
full constructions reuse previously defined records. Partial projections retain keys and contributed evidence.

Read the full Extend/Form diff: only mathematical notation, Definitions, and Stages typography differ. Code is unchanged;
the seven-paragraph Solution and six-paragraph Implementation preamble survive both downstream phases. The complete
collected-to-Code tail diff contains only transform-heading depths and 15 independent number prefixes. Compared all
changes against the `close/3/` collected reference: no method body was removed or changed. Intentional differences are
separate class listings, module-disambiguated headings and base references, an impression-engagement group, a separate
private-helper group, and corrected explanations. There are no old Draft/Extend/Form files for narrative comparison.

Source review corrected claims that judged selection filters timestamps, and distinguished fixed-cutoff precision,
judged recall, null metric eligibility, raw clicks versus clicked impressions, and nullable empty-request flags.
Documented actual join/partition boundaries instead of promising arbitrary slice isolation. Added S9c to require these
evidence/denominator/context checks. Label selectors retain the actual repeated-name predicate semantics; null band
selection is not an all-band wildcard. No application code was changed to conceal these limitations.

Final review used direct source reading, literal patches, and read-only diffs/searches, sequentially and without scripts
or agents. Formula delimiters, environments, signatures, fields, and non-shrinking notation were reviewed structurally;
visual rendering and application execution were not performed. All requested remaining families through Evaluation
now have their four phase documents.

## Experiments family — September 9

Completed all four phases and repaired five annotations. Five independent roots contain six owned classes, eight local
public methods, and one external replacement call. The selector groups its four grain paths; reranking has one local
group; judged evaluation has two; behavior evaluation has one. Each root restarts its counters. Scoring and search each
have a Result; the selector, reranker, and two evaluators have four step shapes in total. Twelve text notation blocks
map to twelve Form displays. External BM25 has a plain circled description and complete inherited boundary inputs.

Checked the full Extend/Form diff: only Definitions, Stages typography, and notation differ; Code and developed narrative
are unchanged. Collected/Code differences are heading rebasing and six number prefixes. Checked all current local
declarations against source and the collected reference in `close/3/`; separated previously merged class/method listings,
repaired reranker indentation, and preserved all local replacements. No old narrative reference exists for this family.

Inheritance required an explicit specialization branch in Definitions, Extend, Notation, templates, and S2a: exact named
base plus every replacement, with complete effective inputs/outputs, is distinct from an ordinary shortened step shape.
Checked Scoring's 16 inputs/14 outputs and SearchDocuments' 23 inputs/output against source; replacement calls retain
source aliases, producer references, constants, and output names. Return definitions preserve all four full score records,
all five explicit candidate updates, complete EvaluationQuery/BehaviorRequest records, and result identity/evidence.

Source predicates, not aspirational background, establish null production identity, filtering instead of exceptions,
the behavior evaluator's exclusion of production, and the absence of assignment/promotion or guaranteed context isolation.
Solution has six developed paragraphs and the preamble has five, preserved through Format. Structural review only;
no visual math render, execution validation, scripts, or application-code changes.

## Training family — September 9

Completed all four phases under `search/transforms/training/`. Training is a composed root with external Features,
internal BuildTrainingData, and its own Result. RankDocumentCandidates is a separate step root, not a child of Training.
There are two public methods and one private helper. Implementation counters are ①–② for Training and ① for the ranker;
Code counters are 1–2 for each root, with the private helper consuming only the ranker's second Code number.

Seven Extend notation blocks map to seven Form displays: Solution; Features; build signature/shape; Training Result;
rank signature/shape. Checked the complete eight-field constructed training return and the candidate projection's two
explicit updates plus source-backed identity/evidence. Result uses the two assigned aliases and both producer-qualified
feature relations. External Features has plain circled prose, complete inputs/outputs, and no method expansion.

Full Extend/Form diff inspection found only formulas, Definitions, and Stages typography. The five-paragraph preamble,
Solution, group prose, and Code are preserved. Collected/Code differences are heading depths and independent prefixes.
Compared the three collected `close/3/` references: restored current `param_join`, exact source indentation/docstring,
and the private helper intent. No old Draft/Extend/Form reference is available.

Kept the actual package scope: preparation does not fit/split/promote a model, and scoring does not reorder candidates
or validate artifact versions/zero scales. Current S1a, S2, S9, and root-local numbering checks cover these distinctions;
no extra rule was necessary. No scripts or application-code changes; visual math rendering remains unverified.

## Labeling family — September 9

Completed Draft/Collect/Extend/Form and repaired all three annotations before collection. The composed root owns two
internal steps: CreateQueryLabels has six public methods in three groups; MergeQueryLabels has five in two groups.
All eleven methods, five global circled items, five independent Code items, two step shapes, and one Result are present.
Eight Extend notation blocks map to eight Form displays. The Result retains `created.labels` in the second call,
assigned stage aliases, complete step vectors, unqualified output names, and no parent-transform name.

Inspected the complete Extend/Form diff: only Definitions, Stages typography, and math differ; the developed Solution,
five-paragraph preamble, group prose, and Code are preserved. Collected/Code changes are heading depths and five number
prefixes only. Compared all three `close/3/` collected references against current source: repaired indentation, removed
duplicate annotation bodies, retained the raw method's local import, updated source docstrings and exact call punctuation,
and used current SearchQuery projections rather than older full constructors. No other phase reference is available.

Corrected stale claims that validation removes bad rows or caller labels win generated collisions. Added the raw-lane
notation rule with S2c, and overlay/validation guidance with S9b, before moving to Training. The raw method's logical
signature is explicitly identified as lane-derived, while Code retains every runtime parameter. Projected query returns
show identity plus all three explicit updates. No scripts or application-code changes; no visual render claimed.

## Relevance family — September 9

Completed all four phases under `search/transforms/relevance/`, restoring the incomplete annotation first. One step
root contains 12 public methods in four groups (4 exposure, 4 engagement, 2 aggregate, 2 normalize). All four groups
have circled Implementation items and independently numbered Code items. Six Extend blocks map to six Form displays:
one Solution model, four complete signature groups, and one complete step shape. No Workflow or Result is invented.

Reviewed the full Extend/Form diff: only formulas, Definitions, and Stages typography differ. Code and narrative are
unchanged, including the five-paragraph preamble. The collected/Code diff contains only heading rebasing and four
prefixes. Return notation includes all explicit projection fields, both complete aggregate grains, and the keys,
denominators, and contributed values explaining normalization. Subsequent context projections focus on the changed
context mapping after the full daily record has been introduced.

The `close/3/` collected excerpt omits nine method bodies and substitutes calculation fragments; it also says CTR is
zero below threshold. Current source retains reported ratios and gates only the normalized contribution. Restored
all bodies, distinguished context expansion from request-time fallback selection, and stated the absence of context
deduplication and CTR denominator guards. Strengthened the shared behavioral-claim rule and added S9a before advancing.
No old Draft/Extend/Form exists for narrative-volume comparison. No scripting or application-code changes; mathematical
coverage and syntax were inspected, but visual rendering remains unverified.

## Clicks family — September 9

Created all four phases under `search/transforms/clicks/`. Both independent streaming roots, Impressions and Clicks,
retain their implicit `summarize` methods. Repaired annotation to separate each class/interface listing from its
intent-led method group. Each root has one circled item, one independently numbered Code item, and one step shape;
there is no composed Result. Five Extend notation blocks map to five Form displays.

Inspected the complete Extend/Form diff and the collected/Code diff: prose and Code are preserved, with only permitted
presentation changes and independent number prefixes. Both projected returns retain every explicit field and the
complete exposure key. Compared the collected reference in `close/3/`: both current methods remain source-exact,
while added method-group prose fixes the older class-only listing structure. No Draft/Extend/Form reference exists.
The new Solution develops attribution and dwell examples; its five-paragraph preamble is preserved downstream.

Verified the seven-day watermarks, 24-hour attribution interval, impression-day/user ownership, distinct-impression
CTR basis, and separate raw/capped dwell measures against source. Did not promote background proposals for diagnostic
outputs or propensity validation into implemented claims. Existing coverage and behavioral-claim QA rules cover these
cases; no additional prompt rule was needed. No scripts, application-code changes, or claimed visual rendering.
Relevance, Labeling, Training, Experiments, and Evaluation remain pending.

## Cohorts family — September 9

Created Draft, Collect, Extend, and Form under `search/transforms/cohorts/`, completing the annotation of
`ResolveCohortBands`. This is one step root: 14 public methods in eight groups, eight circled Implementation items,
eight independently numbered Code items, one complete step shape, and no Workflow or Result. All ten Extend notation
blocks (one Solution model, eight signature groups, one shape) have corresponding Form displays.

Compared the complete Extend/Form diff: only Definitions, Stages typography, and formula conversion differ; the
developed Solution, five-paragraph preamble, group narratives, and Code remain unchanged. The collected/Extend Code
diff contains only heading rebasing and the eight number prefixes. Checked all return fields against the current
cohort schemas, including the first full Band return and the distinct direct/resolved membership identities.

The `close/3/` cohort reference is a partial collected excerpt, not a full chapter. Restored the omitted source methods
and intermediate lanes rather than matching that omission. No older Draft/Extend/Form narrative is available for a
volume comparison. Context keys hash ordered IDs, not catalog predicates or a revision; the chapter states that limit.
No generation scripts or application-code changes. Formula coverage and syntax were inspected; visual rendering was
not verified. Clicks, Relevance, Labeling, Training, Experiments, and Evaluation remain pending.

## Online family — September 7

Created Draft, aggregate Collect, Extend, and Form under `search/transforms/online/`. Source scope contains three
independent composed roots, ten internal step classes, 23 public methods, and one private freshness helper. The two
`SelectGapQueries` classes belong to different modules and remain in their own root subtrees. Filtering, Vectorization,
VectorizeSearchQueries, and Scoring are external calls, with plain circled Implementation descriptions and independent
intent-led Code items. Restored ten missing helper annotations and the incomplete OnlineFiltering parent listing.

| Root | Public methods | Implementation items | Code items | Implementation notation blocks |
|---|---:|---:|---:|---:|
| OnlineFiltering | 4 | 5 | 5 | 8 |
| OnlineVectorization | 5 | 7 | 7 | 12 |
| OnlineScoring | 14 | 13 | 14 | 18 |

One Solution model plus 38 Implementation blocks map to 39 Form displays (78 delimiters). Each composed root has its
own Result; no synthetic Online workflow exists. Manual diff inspection finds only mathematical/Definitions/Stages
presentation changes between Extend and Form, with unchanged prose and Code. Collected/Code comparison shows heading
rebasing and independent number prefixes; method coverage includes the trailing paragraph-vector publisher.

The available `close/3/` references for these roots are collected excerpts, not Draft/Extend/Form chapters. Inspected
each collected root against current source: the new collection restores omitted internal methods, lanes, outputs, and
source-exact parent bindings. No prior narrative-volume comparison is possible for this family because those reference
phases are absent. The new Solution develops six prose paragraphs and one explained freshness model; the preamble has
six component-level paragraphs preserved through Format.

Source-backed distinctions retained: field constraints intersect filter ranks rather than refill them; score-gap markers
are not filter-hit markers; query embedding merges test identity rather than numeric validity; binding is not numeric
normalization; unordered deduplication is not latest-row selection; vector invalidation excludes whole query groups.
Added one shared behavioral-claim rule and matching QA assertion. No application code changed, no generation scripts
were used, and visual math rendering remains unverified. Cohorts through Evaluation remain pending.

## SearchDocuments Implementation and formula repair — September 7

The extended chapter still contained class-level placeholders, so formatting the existing body could not recover the
missing method account. Replaced those placeholders upstream, developed each method-group explanation from current
source, and converted the complete Implementation to formulas. Retained the developed Solution and six-paragraph
preamble, correcting the latter's claim that reranking normalizes fused retrieval evidence.

| Subtree | Public methods | Circled items | Implementation formula blocks |
|---|---:|---:|---:|
| OnlineFiltering / OnlineVectorization / OnlineScoring | External boundaries only | 1–3 | 3 |
| RetrieveDocuments | 4 | 4–6 | 4: three groups and one shape |
| FuseDocuments | 10 | 7–10 | 5: four groups and one shape |
| RerankDocuments | 7 | 11–14 | 5: four groups and one shape |
| Result | Six actual calls | None | 1 |

Including Solution, 19 Extend notation blocks map to 19 Form display blocks (38 delimiters). Manual source/schema
inspection checked all 21 public signatures, explicit projection fields, stage contracts, assigned aliases, and producer
references. No placeholder or text-notation fences remain in Form. Extend/Form diff hunks are confined to math and
Stages typography: their prose and Code agree. Collected/Code differences are heading rebasing and 15 independent
decimal prefixes, including the private candidate helper. Restored the three internal class/interface listings.

Compared the Implementation and Result against `close/3/form/search/transforms/searching/search_docs/SearchDocuments.form.md`.
Intentional differences: current source has one feedback-option method in place of three; external descriptions are
plain circled prose; the filter-target and cached-vector bindings are retained; projected feedback options expose their
added fields and essential identity rather than the entire inherited record. Current reranking divides lexical score
by maximum retrieval score, not RRF by maximum RRF, and declares no zero-denominator guard; the prose now says so.

This was a focused Implementation/Code repair, not a fresh Draft-generation or all-family acceptance run. No generation
or verification scripts were used. Formula syntax and coverage were inspected; visual rendering was not verified.

## Current external-description clarification

The September 7 review restores plain circled descriptions for external stages in Implementation. No separate italicized
intent is needed there because the subsection heading identifies the operation. Collect and Code remain independent:
their external-call items retain the collected italicized intent and decimal numbering. Earlier claims below that both
streams use intent-led external items are superseded. Definitions, Implementation style, Extend, its template, and QA
now distinguish the streams explicitly.

## Current correction: Inference dependencies and reader continuity

The September 6 Inference review supersedes earlier claims below that removing Result assignments and qualified
inputs was correct. That rule erased the distinction between query and document result producers. Result calls now
retain source aliases and producer-qualified input references; their output vectors still use unqualified names without
types, and the overall Result still omits its transform name. QA F9 now requires tracing the two publisher dependencies.
The older Chunking checker still encodes the rejected rule and was not run or modified during this prose-only task.

Inference was compared directly with `close/3/form/search/transforms/inference/Inference.form.md` for narrative and
dependency shape, not copied wholesale. The revision restores principal-topic references and the Inference definition,
introduces the inference adapter before use, and improves continuity while preserving rationales. It deliberately keeps
current source-exact Code, separate class/method groups, complete return fields, and formula font-size consistency rather
than reproducing the reference's older code and typed stage-output vectors.

Manual Extend/Form diff inspection confirms unchanged prose and Code except prescribed presentation changes. The
chapter has seven Implementation items, seven independent Code items, and fourteen formula blocks. No generation or
verification scripts were run, and visual rendering has not been verified in this pass.

## Outcome

Refactored Draft, Collect, Extend, Format, their templates, shared definitions/styles, Notation, and the chapter registry
in Prose.md. Added a Collect template and one shared QA contract. Rules now have explicit owners; operator procedures
instantiate a recursive chapter model rather than repeating growing exception lists.

The four operator files decreased from 13,926 to 1,228 whitespace-delimited words. Operator/support files under
`docs/dev/auto/prose` decreased from approximately 21,262 to 5,500 words (about 74%), including the new template and QA.
This measurement excludes the unchanged Annotation operator, this report, and Prose.md; it is not a token estimate.

## Decisions and causes

| Previous conflict or failure | Refactored contract |
|---|---|
| Filename/package heuristics invented or hid a workflow | Explicit actual main, independent roots, and call ownership inventory |
| "No parent means no Result" suppressed real compositions | Every internal composed transform ends in Result, including an internal composed child |
| Step shape used for a composed class | Step gets one Resulting transform shape; composed gets children then Result |
| Class prose was treated as a method group | Internal class descriptions remain plain; method groups and external calls are item types |
| Missing external Code numbers and intents | External calls are single numbered intent-led items in both streams |
| Private helper numbering conflicted with preserved collected intents | Code numbers every collected intent-led helper group; private helpers remain absent from Implementation |
| Code regenerated from Implementation | Code has an independent collected-source stream and counter; Format preserves it verbatim |
| Complete coverage allowed "same pattern" omissions | Every public method has a complete standalone signature, including every grain path |
| Projection exceptions hid InferencePolicy | Identity projection is pass-through; unseen full records are defined; partial projections have separate state |
| Solution had to be both rewritten and preserved | Draft authors; Extend retains/enriches conceptual coverage; Format changes presentation only |
| Shortness rules erased explanation and limits | Narrative contracts prescribe progression and content, not sentence caps |
| Templates duplicated conflicting prose rules | Templates contain shape/substitution slots; Definitions and Notation own behavior |

The latest review clarification supersedes the earlier plain external-description rule: external calls now have a short
italic intent and number in both Implementation and Code. Internal class descriptions remain unnumbered.

## Verification artifacts

All generated material is isolated under
[close/prose-verification/2026-09-05-refactor](../../../../close/prose-verification/2026-09-05-refactor).
Existing published chapter families and application code were not edited.

- `before/`: copies of the old prompts and representative existing outputs.
- `after/fields/`: a newly authored complete Draft/Collect/Extend/Form family and its corrected annotation input.
- `after/structures/`: fresh collected outputs and source-driven Implementation/Code excerpts for Chunking, Similarities,
  and the three-root Offline chapter scope; a synthetic nested-composed fixture in text/form notation; return-schema cases.
- `inventory.json`: explicit source-backed transform/group/call inventory used for the structural checks.
- `check.cjs` and `results.json`: reproducible assertions and recorded results.

The close directory is ignored by Git. These local evidence files are not part of the tracked prompt changes.

## Results

390 assertions pass. Checks include exact collected-to-extended Python listing contents and order, Code prose preservation,
paragraph-adjacent item ownership, independent Code numbering, root-local circled sequences, complete public signatures,
live-source method AST equality for the structural families, and Result/step-shape ownership. Fields and the nested
fixture also pass extended-to-formatted prose/Code preservation, delimiter, environment, and identifier checks.
All 66 checked local documentation links resolve, and the scoped Git whitespace check passes.

| Case | Public methods covered | Code listings: old -> candidate | Code items: old -> candidate | Results / step shapes |
|---|---:|---:|---:|---:|
| Fields: step main | 2 | 3 -> 3 | 2 -> 2 | 0 / 1 |
| Chunking: internal workflow | 12 | 15 -> 11 | 8 -> 8 | 1 / 2 |
| Similarities: mixed ownership | 38 | 13 -> 16 | 10 -> 13 | 1 / 2 |
| Offline: independent roots | 7 | 14 -> 20 | 6 -> 12 | 3 / 5 |

The synthetic composed parent wraps OfflineFiltering as an internal composed stage. Its child `#### Result` precedes
the parent's `### Result`, while Filtering remains external and SelectOfflineFilterTargets remains internal.
The return fixtures cover unseen same-schema pass-through, repeated full definitions, projection to a different schema,
projection with explicit fields, a different projection, and full construction after partial projections.

### Intentional differences from existing outputs

- Chunking retains the current annotation's complete method-group listings. The older extended output split some of
  those listings; the candidate has fewer fences, not fewer methods. Every method AST still matches live source.
- Similarities adds the three external call sections and their numbered intents. Its private helper group remains
  numbered in Code without becoming an Implementation group; the streams are independently derived.
- Offline adds six external Code call items. Its independent root counters restart, all seven public methods remain,
  and the duplicate AllDocumentTargets boundary block is absent.
- Fields now uses circled Implementation markers in Extend as well as Format. The projected Document return shows every
  explicit field supplied by the return expression; the older Form omitted that definition.

### Input defects found, not prompt regressions

Fields annotation and older chapter prose described flattening the completed map, but both source steps read
`source_documents`. The isolated annotation changes "completed" to "input," and the fresh narrative explains the
actual parallel bindings. It does not change code to manufacture the previously described pipeline. Its Solution
retains logical fields, precedence, dynamic metadata, profiles/analyzers, aggregate boundaries, and separate body retrieval.

SelectRecentQueries annotation used `cross_join(policy, allow_cartesian=True)` while live source uses `param_join(policy)`.
An AST comparison detected the mismatch. The isolated annotation, collected output, and downstream excerpt were corrected
together. The published annotation and existing Offline family remain untouched.

These defects demonstrate why source validation must precede comparisons with "good" chapter examples. They are recorded
rather than treated as fidelity targets.

## Re-run

From the repository root in PowerShell:

~~~powershell
$prosePython = python -c "import sys; print(sys.executable)"
node close/prose-verification/2026-09-05-refactor/check.cjs $prosePython
git -c core.safecrlf=false diff --check -- docs/dev/auto
~~~

The checker reads candidate files; it does not call a model or regenerate prose. It uses Python only to compare parsed
method syntax, allowing annotation indentation and harmless layout differences without overlooking changed operations.

## Limits

This is one agent's manual prompt application and deterministic structural regression, not an independent old/new-model
A/B experiment. The historical comparison uses saved existing outputs, not fresh executions of the old prompts.
Fields was authored end to end; large-family outputs exercise collection and Implementation/Code structure, not new
Problem/Solution narratives or complete Format passes. Source-derived prose in those structural excerpts is not a
claim that their narrative quality was independently reviewed to publication standard.

Math was checked structurally, not visually rendered. The synthetic fixture exercises nesting, not application behavior.
Repeated calls to the same internal class, mixed method/stage transforms, and numbering beyond 20 remain unexercised.
No application build or runtime tests were run for this documentation-only refactor.

## Focused Chunking recovery

The later full-family pass did not satisfy the contracts above: it copied a structural excerpt without its preamble
into Chunking and left text notation in Form. Its compressed Solution also lost the development present in
`close/3/extended/search/transforms/chunking/Chunking.ext.md`. Artifact counts and a clean whitespace diff did not
establish narrative or formula correctness.

The focused recovery updates Chunking Draft, Extend, and Form together. Solution develops contextual retrieval,
hierarchy, parent-local ordering, an explained span model and numeric example, replaceable boundaries, and downstream
value. The preamble covers actual component flow, intermediate text, source coordinates, and segmentation limits.
Unlike the older reference, it distinguishes desired validation from implemented checks and states the current
LF-normalization prerequisite. Collected prose/listings and the downstream Code section remain unchanged.

The accepted reference has 239 Solution prose words and 199 preamble words; the revised sections have 344 and 383.
These counts support the editorial comparison, not a general chapter length requirement. Form preserves the new prose
and replaces all 12 notation blocks with display formulas, retaining all 12 public signatures and source-derived
return fields. Result omits the parent name and stage-alias assignments; stage-call vectors contain unqualified input
and output names only. Full bindings remain in Draft/Extend text notation and Code. Every formula uses the same font
size, with wrapping available for longer calls.

`node docs/dev/auto/prose/checks/chunking.cjs` passes against the actual output family. Eight negative mutations confirm
that residual text notation, missing preamble, compressed Solution, omitted method/return field, reduced formula size,
named Result, and assigned stage inputs fail acceptance. The checker also verifies exact collected/extended Code,
Extend/Form paragraph preservation, source signatures, method vectors, return projections, and numbering.

All 12 formulas were rendered with KaTeX 0.18.6 in headless Edge. At a 1,000-pixel formula panel width none overflowed;
representative method, full schema, projection, shape, helper, and Result screenshots were inspected. The preview is
under `close/prose-verification/chunking-render/`. This verifies that renderer and width, not every consuming Markdown
application or viewport. Other families were not repaired or revalidated by this focused recovery.

## Narrative recovery through Offline

Applied the accepted Chunking narrative standard to the remaining ten families in `close/draft`, `close/extended`,
and `close/form`: 30 documents. This is a scoped Solution/preamble repair, not another full-family regeneration.
Fields already had a developed Solution, which was retained; its preamble now illustrates the actual parallel input
bindings. The other Solutions were authored again from topic background and current source contracts, using `close/3`
to compare explanatory depth rather than copying its claims.

Each Solution develops a general use case into a concrete model or example and explains the tradeoffs and value.
Each Implementation preamble explains component-level data movement, responsibilities, and relevant bounds.
Draft and Extend carry the same repaired narratives; Form preserves them, converting the seven Solution models to
display formulas. No operator prompts or templates were restructured for this application.

### Depth comparison

Numbers are Solution / preamble prose words, excluding displayed notation. They flag compression, not literary quality
or a required chapter quota.

| Family / formatted chapter | Accepted reference | Repaired |
|---|---:|---:|
| [Fields](../../../../close/form/search/transforms/fields/Fields.form.md) | 282 / 183 | 294 / 268 |
| [Indexing](../../../../close/form/search/transforms/indexing/Indexing.form.md) | 259 / 259 | 347 / 320 |
| [Inference](../../../../close/form/search/transforms/inference/Inference.form.md) | 188 / 150 | 290 / 287 |
| [Vectorization](../../../../close/form/search/transforms/vectorization/Vectorization.form.md) | 263 / 94 | 318 / 285 |
| [Scoring](../../../../close/form/search/transforms/scoring/Scoring.form.md) | 162 / 207 | 331 / 314 |
| [Similarities](../../../../close/form/search/transforms/similarity/lexical/Similarities.form.md) | 160 / 205 | 294 / 287 |
| [SearchDocuments](../../../../close/form/search/transforms/searching/search_docs/SearchDocuments.form.md) | 173 / 159 | 326 / 373 |
| [SearchFields](../../../../close/form/search/transforms/searching/search_fields/SearchFields.form.md) | 378 / 274 | 442 / 389 |
| [SearchSimilarity](../../../../close/form/search/transforms/searching/search_similarity/SearchSimilarity.form.md) | 296 / 254 | 348 / 312 |
| [Offline](../../../../close/form/search/transforms/offline/Offline.form.md) | 151 / 107 | 328 / 327 |

Editorial review checked the reasoning represented by these counts: grain-specific statistics in Indexing; compatibility
and status in Inference; artifact lifecycle and separate identity binding in Vectorization; distinct evidence families
in Scoring; reciprocal direction and per-source limits in Similarities; admission/fusion/feedback in SearchDocuments;
field/body/mixed/meta examples in SearchFields; replaceable candidate production and missing-lane semantics in
SearchSimilarity; and independent preparation paths in Offline.

### Source fidelity

- SearchDocuments retains distinct 10,000-target, 1,000-feedback-candidate, and 100-result limits and its bounded,
  batch-oriented execution boundary.
- Offline's 1,000-query default bounds only the popular branch. Recent queries are added separately using inclusive
  ages zero through seven days; the combined, deduplicated population can exceed 1,000.
- Vectorization's query-binding transforms are separate caller-composable transforms, not extra children of its
  Inference workflow. Scoring's `ScoreVectors` contains document and paragraph step methods, not child transforms.
- Similarities produces its lexical scoring evidence internally through calls; the main transform does not accept
  those scores as additional input relations.
- Older SearchFields prose claimed field restriction before filter-rank calculation. Current `SearchFields` imports
  canonical `SearchDocuments`; `OnlineFiltering` invokes `Filtering` without field targets, and `SelectFilterTargets`
  intersects the resulting scores with field targets before checking the rank cap. No companion implementation exists
  at the path described by the background. The repaired preamble distinguishes the desired pre-ranking restriction
  from the implemented intersection; eligible documents beyond the unrestricted top 10,000 can still be lost.
  No search code was changed.

### Verification and limits

`node docs/dev/auto/prose/checks/narratives.cjs` passes for all ten families. It rejects 37 negative mutations:
compressed Solutions, missing preambles, Form prose drift, and residual text models in Form Solutions.
The existing Chunking acceptance check also passes unchanged.

The before-snapshot is `close/prose-verification/narrative-rollout/before.json`. Running
`node close/prose-verification/narrative-rollout/preserve.cjs --check` confirms that every byte outside Solution and
the Implementation preamble is unchanged in all 30 documents. All ten collected documents are unchanged as well.
This includes the detailed stage sections, formulas, numbering, Code prose, listings, and their original line endings.

All seven restored Solution formulas were rendered with KaTeX 0.18.6 in headless Edge and visually inspected. They
use the same 19.36-pixel computed formula font and fit the 1,000-pixel preview panels without overflow.
The preview and measurements are under `close/prose-verification/narrative-rollout/`.

This pass does **not** certify the existing stage trees, input/output inventories, signatures, return definitions,
or Implementation formulas in these ten families. Older structural and text-notation defects remain outside the
narrative repair; preserving those sections is not a claim that they satisfy full chapter QA. No runtime behavior,
application build, or search tests were exercised by this documentation-only change.

## Focused Indexing repair

Repaired the four Indexing phase documents and their three annotated source units. The accepted Solution and
Implementation preamble remain unchanged (347 and 320 prose words). Other chapter families were not regenerated.

Compared each phase separately with its matching `close/3/` document and checked current source contracts:

| Phase | Checked result |
|---|---|
| Draft | Same section tree as the reference; continuous Implementation; complete named signatures, input/output vectors, and child-call bindings in Notation; collected reference only in Code. |
| Collect | Workflow, LexIndex, FieldIndex containers; all 23 method groups use short italic intents and explanation before their listings; no method subsections or item numbers. |
| Extend | LexIndex and FieldIndex followed by Result; 23 circled public groups through ㉓; two complete step shapes; complete composed Result. Code has only the three transform containers and an independent 1–23 group sequence. |
| Form | Same tree and explanatory paragraphs as current Extend; all 26 text notation blocks converted to formulas; no pre-Code fences; Code identical to Extend. |

The reference comparison also exposed incomplete step input/output vectors, a placeholder Result, missing Normalization
definition, and obsolete argument names. Those were corrected from source. Deliberate differences from `close/3/`:
Collect uses one H1 and exact transform container names; method arguments use current source names; projected returns
show explicit additions/overrides under the current notation contract; Result omits aliases and its parent transform
name, uses untyped/unqualified stage input/output names, and retains typed overall inputs/outputs. FieldIndex's stage
output is `terms`; the parent publishes it as `field_terms`.

All 26 Python listings match the current source class headers and method bodies, without module imports. Ordered
signatures, per-stage method groups, circled glyphs, decimal numbering, return fields, and prose/Code preservation
passed focused checks. Source code was not changed. The earlier narrow narrative check still passes.

All 26 formulas rendered with KaTeX 0.18.6 in headless Edge. Two long summary signatures were wrapped at the arrow;
every formula now fits the 1,000-pixel panel at the same 19.36-pixel computed font size. Projection, full-return,
step-shape, and composed-Result previews were inspected. Phase comparisons and render measurements are saved under
`close/prose-verification/indexing-recovery/`. This verifies that renderer and width, not every Markdown application.

The owning prompt/QA rules now explicitly reject placeholder stage introductions, method headings in Code, internal
class-level items substituted for public groups, and reuse of stale Form structure. Acceptance requires a phase-by-phase
reference comparison and conversion of the entire pre-Code notation inventory, not just Solution models.

### Indexing layout and projected-return refinement

The fixed 1,000-pixel acceptance panel above proved too restrictive: it prompted line breaks in otherwise readable
expressions. Indexing now keeps each method, step shape, and stage call intact. All 26 formulas render at the same font
size; the preview supports horizontal scrolling at narrow widths instead of changing the mathematical expression.
The reading application's own scrolling behavior is not controlled by the Markdown chapter.

Projected returns now retain every explicit addition/override plus the grain keys and contributed payload needed to
explain the operation. MaterializedSentence shows ellipses and content; occurrences, grouped counts, target statistics,
and assembled postings expose their core identities and values. Ellipses appear only when fields are actually hidden.
This restores the useful evidence shown in `close/3/` without expanding unchanged sentence coordinates and metadata.

Checks confirmed intact expressions, complete explicit and essential fields, 26 formula blocks, and unchanged
Implementation prose, numbering, and Code. The method, LexIndex shape, and public-posting previews were inspected.
The four prose prompts also restore named process/operator sections without changing their input/output boundaries.
