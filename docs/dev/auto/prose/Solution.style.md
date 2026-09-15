# Intent, Problem, and Solution contract

Apply when authoring Draft or Extend. Format checks this contract but preserves its input prose.

## Intent

Open the chapter with the motivating purpose and desired outcome in one or two clear, expressive sentences.
Prefer active, concrete language over vague abstractions, while retaining useful domain terminology and meaningful
distinctions. Simplify wording to improve understanding, not merely to shorten it or make it more conversational.
Retain an existing Intent when it already meets this contract; a style pass does not require different wording.
Keep mechanisms, limits, risks, and challenge inventories out of Intent. Develop the use-case difficulty in Problem;
place the answer in Solution and implementation-specific constraints in Design or Implementation.
Do not require an explicit reference to building a search engine.

## Problem

Develop the need stated in Intent: gentle domain introduction -> familiar situation -> topic-specific difficulty ->
practical consequence. First explain the activity and any basic concepts a newcomer needs, using ordinary practice or
a small example. Do not open with a failure, dense technical contrast, or assumed domain knowledge. Give the setup room
to develop before explaining what makes the need nontrivial. Use a recognizable search-user situation to make the need
concrete, then develop the distinctions that matter; do not constrain the explanation to a paragraph quota.
Keep the proposed answer in Solution. Do not duplicate Intent, enumerate implementation defects, prescribe components,
or turn the section into Design's requirements checklist.

When using an older chapter, borrow its problem framing and accessible explanation, not obsolete behavior or scope.
The `close/2/` Chunking, Fields, Indexing, and Filtering Problems illustrate this progression; their references to
particular components and implementation guarantees still require checking against current source.

## Solution

Aim for *graspability*: how readily a reader understands the explanation, including when skim-reading it.
Let paragraph openings carry the main progression in familiar language, with terminology and detail developed in
context. This is a reading test, not a requirement for slogans, extra headings, or a summary sentence in every paragraph.
Introduce what an idea does or why it matters before relying on its technical name; retain names and distinctions
needed for precision, but avoid stacking specialized terms where a direct explanation works better.
Smooth changes in difficulty by giving harder connections more explanation, examples, or separate paragraphs.
Preserve generality, theory, expressiveness, and narrative pace: improve the path through the content rather than
turning it into a shorter overview. A skim should convey the approach; a close reading should explain why it works.

~~~text
general domain practice + user goal
    -> conceptual bridge: why this model answers that goal
    -> concrete abstraction + explained example/model/formula when useful
    -> enabled behavior + semantic tradeoffs + practical value
~~~

Preserve this order, not a rigid paragraph count. Give each idea the space its explanation needs, even when this
requires substantially more paragraphs than a concise overview. Explain concepts before relying on them and show
how they connect; a definition alone may not explain how an idea helps. The opening is not a project component,
algorithm, schema, or formula. A formula must have introduced symbols and an interpretation in prose.
Balance three jobs in the opening paragraph: ground the topic in a relevant theory or familiar practice, connect that
grounding to the chapter's purpose, and introduce the concepts needed for the proposed approach. Make these one
connected explanation, not three checklist sentences. A concrete example can establish the connection. Do not replace
grounding with an imperative slogan such as "Keep the evidence families distinct" or a restatement of Intent; explain
why that choice helps. Equally, avoid a detached theory survey or unexplained terminology before the reader sees its
purpose. Later paragraphs develop the approach rather than compensating for an abrupt opening.
Follow General style for selective italicized inline definitions and glossary coverage. Write symbolic references
in prose as inline mathematics under Notation; a literal L_d or the word alpha is not a formatted symbol reference.

For query/request structures, show representative supported examples early in the concrete explanation and explain their
meaning. SearchFields, for example, needs readers to understand field-only, body-only, mixed, and aggregate requests,
not just the internal names. Use only syntax supported by the inputs.

Keep the conceptual explanation distinct from architecture and method inventories. Explain why the ideas work and what
they make possible; put limits, component responsibilities, validation, and implementation mechanics in Implementation
(or Draft Design). Do not replace a developed explanation with a short component summary, and do not pad it to a quota.

Develop the explanation as a book chapter: introduce ideas, explain their meaning and purpose, and connect them through
examples and consequences. Let this account unfold across paragraphs without requiring a fixed pattern in each one.
Preserve the reasoning, examples, and tradeoffs in a successful reference; a compressed abstract with the same keywords
is not equivalent coverage. When a reference is supplied, compare both explanatory depth and prose volume.
A substantial reduction requires restoring the missing development or explaining why it was
redundant; word counts flag regression but do not substitute for editorial review.

Make that depth accessible on a first reading: use familiar words, concrete examples, and sentences with one main idea.
Use the same purpose-first language as good Implementation and Code explanations, but not their item-level length
limits or italic intents. Explain the connections a newcomer needs instead of expecting the reader to supply them.
An example, consequence, or explanation of a relationship can develop an established concept without introducing a
new one. Preserve these connections when editing; improve flow through clear wording and transitions, not by assuming
that a definition makes its applications obvious.

Keep each conceptual thread together: introduce the idea, develop its important effects or limits, then connect it to
the next idea. For example, finish normalization before moving to compatible document/query analysis; do not interrupt
normalization with compatibility and then restart it. Introduce a measure before its worked example, and preserve the
general-to-concrete progression within the section.

Prefer concrete subjects and direct verbs to layered explanations about abstractions. Name the referent when a generic
subject such as "the application" or "the aggregate" requires the reader to guess; for example, "the combined meta
field." Shorten or split a sentence that nests several choices and consequences. Retain clear, expressive wording that
already works, such as "Choosing boundaries is a separate task" and "A field's meaning does not have to depend on its
storage location." Do not turn a statement of flexibility into an unexplained requirement.

Use contrasts where they help explain a distinction, with enough context to understand it. Phrases such as ", not",
"rather than", and "a separate task" are not by themselves evidence of excessive explanation.
Prefer an explanation of why an idea helps over a compressed technical contrast. Explain that rank gives retrieval
methods a common language and that Reciprocal Rank Fusion rewards high positions across methods; "uses lane positions"
alone does not explain the benefit. Preserve conventional algorithm names and capitalization.
Explain a technical term when it first becomes necessary instead of stacking abstractions or relying on Definitions
to rescue the paragraph. Simplify language, not coverage: keep the theory, examples, reasoning, and practical tradeoffs.
Read the section without Code nearby; understanding the proposal must not require reconstructing its implementation.

Unwind a difficult paragraph into a sequence: introduce the idea in familiar terms, explain its purpose, then show its
consequence or example. Separate distinct concepts before relating them; adding line breaks to dense prose is not
enough. Prefer a small recurring search-user example, such as finding password-reset instructions in a help collection,
so readers can follow the same query, document, or result through the explanation. Adapt the example to the topic;
do not force one domain on every chapter. Show supported query examples where useful and identify illustrative
numbers as examples, not measured outcomes or fixed system settings. Expansion adds understanding, not code mechanics.

Define the chapter's central calculations, not merely their names. Give a conceptual formula or equally precise rule,
introduce every symbol, population, weight, and parameter, and explain its effect with an example. Definitions lists
summarize these meanings; they do not replace the calculation. For Scoring, IDF alone does not define weighted overlap
or BM25. State numerator/denominator, repetition and length effects, and applicable zero/missing-evidence behavior.
Check the implemented variant, including multiplicities, against source; distinguish a conceptual ideal from behavior
that differs. Use text models in Draft/Extend and corresponding display formulas in Form.
Give every conceptual formula at least one dedicated explanatory paragraph, normally immediately after it. Introduce
its symbols before it; afterwards explain what the operation or result means, using a worked example where useful.
When one block contains several calculations, explain each separately or split the block to interleave explanations.
A symbol list, a one-line caption, or a paragraph discussing several unrelated formulas does not satisfy this rule.
Do not add a formula merely to meet a quota. This rule concerns conceptual Problem/Solution exposition, not the concise
Implementation items or their signature/return-schema notation. Format preserves this expanded development and maps
each revised text-model block to its corresponding formula without losing calculations.

Extend retains the Draft's conceptual coverage and improves it using current background and source. Format preserves
that narrative and paragraph order, converting mathematical typography only.
