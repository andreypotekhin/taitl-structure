# Intent, Problem, and Solution contract

Apply when authoring Draft or Extend. Format checks this contract but preserves its input prose.

## Intent

Open the chapter with the motivating use case and desired outcome in one or two active, concrete sentences.
Keep mechanisms, limits, risks, and challenge inventories out of Intent. Develop the use-case difficulty in Problem;
place the answer in Solution and implementation-specific constraints in Design or Implementation.
Do not require an explicit reference to building a search engine.

## Problem

Develop the need stated in Intent: familiar situation -> topic-specific difficulty -> consequence for the search user or
application. Usually one or two connected paragraphs suffice. Explain what makes the need nontrivial, without answering
it yet. Concrete contrasts help: a passage without context, the same metadata in two storage forms, repeated corpus
analysis, or expensive ranking applied too broadly. Do not duplicate Intent, enumerate implementation defects, prescribe
components, or turn the section into Design's requirements checklist.

When using an older chapter, borrow its problem framing and accessible explanation, not obsolete behavior or scope.
The `close/2/` Chunking, Fields, Indexing, and Filtering Problems illustrate this progression; their references to
particular components and implementation guarantees still require checking against current source.

## Solution

~~~text
general domain practice + user goal
    -> conceptual bridge: why this model answers that goal
    -> concrete abstraction + explained example/model/formula when useful
    -> enabled behavior + semantic tradeoffs + practical value
~~~

Preserve this order, not a rigid paragraph count. Usually three to five substantive paragraphs give the explanation room
to develop. The opening is not a project component, algorithm, schema, or formula. Explain concepts before relying on
them; a formula must have introduced symbols and an interpretation in prose.

For query/request structures, show representative supported examples early in the concrete explanation and explain their
meaning. SearchFields, for example, needs readers to understand field-only, body-only, mixed, and aggregate requests,
not just the internal names. Use only syntax supported by the inputs.

Keep the conceptual explanation distinct from architecture and method inventories. Explain why the ideas work and what
they make possible; put limits, component responsibilities, validation, and implementation mechanics in Implementation
(or Draft Design). Do not replace a developed explanation with a short component summary, and do not pad it to a quota.

Develop the explanation as a book chapter: each paragraph introduces an idea, explains its meaning through the topic,
and prepares the next idea. Preserve the reasoning, examples, and tradeoffs in a successful reference; a compressed
abstract with the same keywords is not equivalent coverage. When a reference is supplied, compare both explanatory
depth and prose volume. A substantial reduction requires restoring the missing development or explaining why it was
redundant; word counts flag regression but do not substitute for editorial review.

Make that depth accessible on a first reading: use familiar words, concrete examples, and sentences with one main idea.
Prefer an explanation of why an idea helps over a compressed technical contrast. Explain that rank gives retrieval
methods a common language and that Reciprocal Rank Fusion rewards high positions across methods; "uses lane positions"
alone does not explain the benefit. Preserve conventional algorithm names and capitalization.
Explain a technical term when it first becomes necessary instead of stacking abstractions or relying on Definitions
to rescue the paragraph. Simplify language, not coverage: keep the theory, examples, reasoning, and practical tradeoffs.
Read the section without Code nearby; understanding the proposal must not require reconstructing its implementation.

Define the chapter's central calculations, not merely their names. Give a conceptual formula or equally precise rule,
introduce every symbol, population, weight, and parameter, and explain its effect with an example. Definitions lists
summarize these meanings; they do not replace the calculation. For Scoring, IDF alone does not define weighted overlap
or BM25. State numerator/denominator, repetition and length effects, and applicable zero/missing-evidence behavior.
Check the implemented variant, including multiplicities, against source; distinguish a conceptual ideal from behavior
that differs. Use text models in Draft/Extend and corresponding display formulas in Form.

Extend retains the Draft's conceptual coverage and improves it using current background and source. Format preserves
that narrative and paragraph order, converting mathematical typography only.
