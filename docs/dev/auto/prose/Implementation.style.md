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
- Method group: one short italic intent sentence followed by an explanatory paragraph, kept in the same numbered item.
  Explain the data transition, responsibility, and rationale at useful depth; do not limit the explanation to one sentence
  or italicize the whole paragraph. Typed signatures supply mechanics, not a replacement for explanation.
  Read consecutive items as a continuous explanation: carry established context forward, state the next useful
  transition plainly, and give its rationale. Do not bury that progression under repeated constraints or defensive
  caveats; put exceptional behavior with the operation that owns it or in the preamble.
- External call: one source-backed plain description with its circled marker, followed by boundary notation. Do not add
  an italicized intent that repeats the external stage heading. This does not change Code's independent intent-led items.
  If useful, add a direct reference such as "See Scoring for score production details."
- Result: one self-contained sentence explaining what the composed transform publishes from its inputs.

Keep source group order. Avoid duplicated intent/explanation text, generic production commentary, and opaque terminology
where a concrete data or domain name would be clearer.
