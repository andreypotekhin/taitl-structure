# Chapter operator refactor verification

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
