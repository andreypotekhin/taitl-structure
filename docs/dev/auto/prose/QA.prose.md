# Chapter acceptance checks

Run shared checks plus the producing operator's checks before delivering a chapter. Use the inventory from
[Definitions.prose.md](Definitions.prose.md) as the oracle, not item counts guessed from another output.
A failed input check is repaired at its owning operator when authorized; otherwise report it. Never conceal it downstream.

## Shared

| ID | Assertion |
|---|---|
| S1 Scope | Every root is evidenced; every call has an alias, bindings, outputs, and explicit ownership. No filename-based main or synthetic package workflow. |
| S1a Independent companions | A main plus independent topic transforms uses separate root containers and counters; inspect actual call assignments, not the chapter's flat Stages list. Vectorization's binders are not children of Vectorization. |
| S2 Coverage | Ordered public-method and stage inventories equal output coverage. Include every grain path, implicit step, public helper, and trailing publisher. |
| S2a Inheritance | Concrete-stage contracts include inherited inputs and public groups; Code lists the declaring base once before use. Reject a fabricated base-class stage call and missing inherited query preparation in Scoring. |
| S2b External contracts | Compare each external boundary with the actual call and effective class inputs, including inheritance. Similarities' scoring boundaries must retain queries, all four term relations, and targets, not only locally declared summaries. |
| S3 Narrative | Problem states need, not challenges. Solution follows context -> bridge -> concrete example/model -> value. Preamble explains actual data flow and important limits. |
| S4 Language | Essential terms are defined; exact identifiers use inline code in prose. No production terminology. "Search engine" occurs at most once. |
| S5 Layout | One H1; correct section tree; blank lines around blocks. Every code fence has preceding descriptive prose; no adjacent listings separated only by whitespace/headings. |
| S6 Narrative depth | Compare Solution and preamble with the supplied or accepted reference: retain explanatory progression, examples, reasoning, and tradeoffs, not just keywords. Record before/after prose counts and investigate substantial shrinkage. A structural excerpt cannot establish full-chapter quality. |
| S7 Stage explanation | Each internal introduction identifies the incoming evidence, substantive operation, and useful output; reject generic "Implement/Run X" placeholders. It remains unnumbered and does not replace method-group items. |
| S8 Reader continuity | Define the central activity and unfamiliar integration roles before use; principal-topic references stay general. Read adjacent items for progression plus rationale, and reject parameter-validation detail that overshadows the work itself. |

## Draft

~~~text
D1 top_level_sections == [Problem, Solution, Builds on, Used by, Definitions, Inputs, Outputs,
                Stages, Notation, Design, Implementation, Code]
D2 Implementation == continuous_prose; no stage subsections, numbered items, Workflow, or Result
D3 Notation covers all roots + public signatures + calls + concrete inputs/returns/outputs
D4 Code == collected_reference(s); no Python listings
~~~

Check Solution examples against supported behavior and Design requirements against their proposed/implemented status.

## Collect

~~~text
C1 python_listings == annotated_listings minus module-level imports, in source order
C2 each class/method listing occurs once per root
   # intentional exception: an external assignment also appears in its own call section
C3 internal container -> plain description -> class listing
C4 method/helper group -> short italic intent + original explanation in ONE paragraph -> listing
C5 external call -> short italic intent + source-backed explanation -> complete assignment
C6 item_numbers == none; containers follow inventory topology
C7 container_headings == transform_tree; method/helper headings == none
C8 removed_imports == complete module-level statements; no dangling imported names or closing parentheses
C9 every internal class has its declaration/interface listing before its first method group
~~~

Inspect the paragraph immediately preceding EACH method listing, not just total intent counts. Compare its explanation
with annotation after removing the converted heading; reject duplicate paraphrases, a numbered wrapper plus an
unnumbered explanation, or missing separators. Retain all internal code and no external implementation.

## Extend

~~~text
E1 top_level_sections == Draft.top_level_sections - [Notation, Design]
E2 tree == render(inventory):
       internal step -> groups + exactly one Resulting transform shape
       internal composed -> all child subsections + exactly one concluding Result
       external -> exactly one circled plain description + boundary notation, no italic intent/methods/Result
E3 Implementation items == root_local_circled(public_groups + external_calls)
E4 Code items == root_local_decimal(collected_intent_groups + external_calls)
E5 strip_number_prefixes(Code prose) == collected prose
E6 Code listing sequence and contents == collected listing sequence and contents
E7 signatures == complete named typed arguments + concrete returns for EVERY public method
E8 preamble = prose between Implementation heading and first subsection
   preamble is nonempty and develops Draft's component-level account under Implementation.style
E9 for each internal_step: ordered_item_method_sets == collected_public_group_method_sets
   # a single class-level item or a complete shape alone cannot satisfy group coverage
   each item explains its concrete data transition and rationale; an intent plus a signature alone fails
E10 Code headings == collected transform containers; method/helper headings == none
~~~

Compare within each subtree, not the document total: a composed internal child needs its own Result even when its parent
has one. Internal class intros consume no number; external items do. Never reset at ordinary children or derive Code's
counter from Implementation. An internal step's named standalone shape appears only after its shape label.
Check the streams independently: external Implementation descriptions have circled numbers and no italicized intent;
their Code items still preserve collected intents and decimal numbers. Do not strip Code intents to match Implementation.

Check each composed notation against actual call arguments, policies/constants, aliases, and relation names. Stage arrows
expose unqualified output names, not types or qualified paths; final outputs are named and typed, without assignments.

## Format

~~~text
F1 headings/tree/item order == Extend
F2 all prose == Extend, except declared Definitions/Stages/math presentation changes
F3 Code == Extend.Code verbatim
F4 every notation block outside Code -> one corresponding display-math block
   residual text/LaTeX fences and plain signature/shape blocks outside Code == none
F5 formula method coverage == Extend signatures == inventory public methods
F6 return definitions == Notation.chapter_profile.return_definition_state
F7 step shapes, external boundaries, and composed Results each use their distinct notation profile
F8 preamble is nonempty; preamble paragraphs == Extend.preamble paragraphs
F9 Result formula has no parent transform name; every call retains its source assignment alias
   inputs == source relation references, producer-qualified for earlier-stage outputs (resolve forwarding aliases)
   outputs == unqualified relation names; no argument-keyword assignments or schema types inside calls
F10 all formula font sizes are equal; method/shape/call expressions stay intact unless the intended layout requires a break
F11 for each internal_step: Form.item_method_sets == current_Extend.item_method_sets
    # compare the full current subtree, not a previously generated Form
~~~

Check F2 paragraph by paragraph, including preamble, external descriptions, and Result; a prose-count match is insufficient.
For F9 trace an actual dependency, not just identifier counts: Inference must distinguish
`inferred_queries.results` from `inferred_documents.results` at the two publishers. Reject both alias-free calls and
bare `results` inputs that erase that distinction. When a reference is requested, inspect this relationship in its
Result formula explicitly; matching the current prompt alone cannot establish reference parity.
Count formula blocks and validate their ordered mapping to source notation; checking that zero math delimiters are
balanced cannot pass F4. Preserve a complete Solution and preamble through Format before checking individual formulas.
Record this mapping by stage, not just as a family total. For SearchDocuments, check all three external boundaries,
RetrieveDocuments' four methods, FuseDocuments' ten methods, RerankDocuments' current seven methods, all three step shapes,
and the six-call Result. The older three feedback-option methods in `3/` must not displace the current single method.
For F6 test: unseen full schema, input=return schema (including identity project/base), pure projection to a different
schema, projection with additions, repeated full schema,
and a different projection of the same schema. Full signature coverage is mandatory in every case.

For every projected return, check `visible_fields` against the source's explicit return keywords plus the keys and
contributed payload needed to explain that operation. Reject missing explicit overrides even on repeated projections,
loss of grain identity, and a joined public posting that hides its defining statistics. Also reject unexplained
inherited-field dumps and ellipses when no fields are omitted. Use Indexing's materialization, occurrence, count, and
assembled-posting returns as the contrasting cases; one projection rule must not flatten them into the same shape.

For F10, inspect the intact expression before accepting a line break. Reject automatic breaks for multiple arguments,
matrix height, or a nominal 1,000-pixel preview width. Materialize sentence, summarize document index, the LexIndex
shape, and the FieldIndex method/shape must remain single horizontal expressions in the Indexing chapter. Scrolling
is a presentation-container choice, not permission to shrink fonts, hide fields, or split a transform gratuitously.

Validate balanced $$ delimiters/environments, escaped identifier underscores, vertical multi-argument calls, complete
return types, and the chapter spacing rules. Where available, render representative formulas and inspect wrapping,
alignment, visible markup, and matrix legibility. Record separately what was checked structurally and visually.

## Regression protocol

Use Fields (step main), Chunking (internal workflow), Similarities (internal/external workflow), and Offline (independent
composed roots). Add a nested internal composed case when those samples do not exercise it.

Save old prompts and outputs before changes. Generate candidate outputs in an isolated directory with the same current
source inputs. Compare section topology, inventories, signatures, bindings, prose preservation, and exact Code; review
new Draft/Extend narrative for conceptual coverage rather than literal equality. Report each intentional difference
from older examples and every untested branch. Old outputs are references, not authority over current requirements.

When `close/3/` is the accepted reference, compare every resulting phase document separately: heading tree, ordered
transform/method coverage, group boundaries, numbering glyphs and scope, Code prose/listing placement, and notation
coverage. A family-wide count cannot hide a missing subtree or certify another phase. Record each intentional departure
from the reference against current source or an explicit contract. A focused narrative pass is not full-chapter QA.

The older `checks/chunking.cjs` contains an obsolete assertion forbidding Result stage assignments. It is not an
acceptance oracle for the current notation contract until updated. For a prose-only run, perform the checks above
directly against source, current upstream documents, and the requested reference; do not invoke generation scripts.

When scripting is permitted, `node docs/dev/auto/prose/checks/narratives.cjs` can supplement narrative review across
the families through Offline. It checks reference-depth alarms, paragraph preservation across
Draft/Extend/Form, explained Solution-model conversion, and topic-specific examples and limits. Its negative tests
reject shared compression, missing preambles, downstream prose drift, and text models left in Form's Solution.
This focused check does not replace the stage, signature, return-schema, numbering, or full-document formula checks above.
When scripting is prohibited, inspect those properties directly. Never treat a narrative-only pass as completion of
a family whose Implementation still contains placeholders or whose Form retains text notation.
