# Problem and Solution contract

Apply when authoring Draft or Extend. Format checks this contract but preserves its input prose.

## Problem

State the motivating use case and desired outcome in active, concrete language. One vivid sentence may suffice.
Keep implementation mechanisms, limits, risks, and challenge inventories out; discuss them in Solution, Design, or
Implementation according to their purpose. Do not require an explicit reference to building a search engine.

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

Extend retains the Draft's conceptual coverage and improves it using current background and source. Format preserves
that narrative and paragraph order, converting mathematical typography only.
