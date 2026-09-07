# Chapter operator refactor verification

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
