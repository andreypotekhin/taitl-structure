# Implementation contract

Implementation connects the conceptual Solution to executable behavior. The structural rules belong to
[Definitions.prose.md](Definitions.prose.md); this contract governs prose.

## Preamble

The preamble is a required body of prose immediately after `## Implementation`, before the first subsection. Author it
in Draft, develop it in Extend, and preserve it paragraph for paragraph in Format. Stage introductions, signatures,
and Result prose do not replace it. A structural excerpt without a preamble is not a complete chapter.

Explain purpose, major data movement, important limits, and the reasons responsibilities are separated. Use enough
continuous paragraphs to make that account understandable; neither an arbitrary sentence cap nor a component inventory
is a substitute. Retain source-backed constraints such as a 10,000-target bound, ownership, freshness, fallback, and
observable outputs when they affect behavior. Distinguish established behavior from proposed requirements.

Stay at component level. Name the main transform and actual data flow, or explain the independent transforms when no
main exists. Avoid individual method references unless necessary to explain an architectural boundary.
Keep configuration in proportion: a policy parameterizes the work; its validation and routing should not displace
the account of what the work achieves. Introduce unfamiliar integration roles before relying on their short names.

For a composed workflow, develop the reader's path from incoming data through the major intermediate representations
to the published result, explaining why the stages are separated. Several connected paragraphs are normally needed.
Match the explanatory depth of an accepted chapter reference; do not condense it into a one-paragraph stage inventory.

## Subsections

- Internal introduction: plain, unnumbered active prose that connects the stage's incoming evidence, substantive
  operation, and useful output. A transform-name subject or an imperative is equally valid; use enough explanation
  to distinguish the stage's responsibility. "Implement LexIndex" or "Apply the declared transformation" fails this
  contract. For example: "`LexIndex` materializes source-faithful sentence text, produces one normalized occurrence
  stream, and assembles public term and summary relations for every lexical grain."
- Method group: one short italic intent sentence followed by explanation in the same numbered item. Extend retains
  the detailed account as a knowledge reference; Format presents the concise reading version described below.
  Do not italicize the explanation. Typed signatures supply mechanics, not a replacement for explanation.
  Read consecutive items as a continuous explanation: carry established context forward, state the next useful
  transition plainly, and give its rationale. Do not bury that progression under repeated constraints or defensive
  caveats; put exceptional behavior with the operation that owns it or in the preamble.
- External call: one source-backed plain description with its circled marker, followed by boundary notation. Do not add
  an italicized intent that repeats the external stage heading. This does not change Code's independent intent-led items.
  When the operation is defined in another chapter, name that chapter and the transform, for example
  "See the Scoring chapter for the definition of `ScoreOverlap`." Do not import its method groups to supply the definition.
- Result: one self-contained sentence explaining what the composed transform publishes from its inputs.

Keep source group order. Avoid duplicated intent/explanation text, generic production commentary, and opaque terminology
where a concrete data or domain name would be clearer.

## Format explanation projection

Match each public Implementation group to its Code group by transform and method membership, never by item number.
For inherited methods, resolve that membership to the documented owning base's Code group; reusing its explanation
does not duplicate Code or synchronize the two numbering streams.
Keep the Implementation marker and short italic intent. Base its explanation on the matching Code explanation, using
it unchanged when already clear and concise; otherwise remove excess detail or simplify the wording. Prefer one short
sentence after the intent, allowing a second when needed to preserve meaning. Do not pack a paragraph into one long
sentence. Explain the useful data transition and, where helpful, its reason without requiring nearby Python to follow it.

Use a concrete schema or field reference when a generic noun would leave the reader guessing which value is meant.
For example, name the metadata map as `Document.fields`; do not make the reader infer it from "the map" in a formula
that also contains named document attributes. This is selective grounding, not a field-reference quota: most items do
not need field names. Preserve readable, short prose rather than enumerating every input or return field.
Describe collection outputs without suggesting a singleton: "a `DocumentField` row per non-empty key" is clearer than
"one `DocumentField`" when the operation expands a map into rows. State exact cardinalities only when they matter.

Retain assigned values that distinguish an operation's observable meaning, not just the field name. For example,
SearchFields publication items must say `match_scope="metadata"`, `"content"`, or `"metadata+content"` as appropriate.
Such a discriminator is part of the explanation, not excess implementation detail; a field-only formula cannot supply it.

This is a one-way reading projection, not a Code rewrite. Keep the detailed Extend explanation and all collected/Code
prose and listings intact. Preserve every method, signature, return schema, formula, group boundary, and numbering scope.
Preamble paragraphs, internal introductions, external descriptions, and Result prose are unchanged by this projection.
Private Code helpers do not acquire Implementation items. If a matching public group is missing, repair the upstream
coverage rather than inventing a summary or borrowing the next numbered Code item.
