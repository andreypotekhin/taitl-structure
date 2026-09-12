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

- Internal introduction: plain, unnumbered active prose under the Transform descriptions contract below.
  This applies to step transforms, composed internal stages, and independent roots alike.
- Method group: one short italic intent sentence followed by explanation in the same numbered item. Extend retains
  the detailed account as a knowledge reference; Format presents the concise reading version described below.
  Do not italicize the explanation. Typed signatures supply mechanics, not a replacement for explanation.
  Read consecutive items as a continuous explanation: carry established context forward, state the next useful
  transition plainly, and give its rationale. Do not bury that progression under repeated constraints or defensive
  caveats; put exceptional behavior with the operation that owns it or in the preamble.
- External call: one concise description under the same contract, with its circled marker, followed by boundary notation. Do not add
  an italicized intent that repeats the external stage heading. This does not change Code's independent intent-led items.
  When the operation is defined in another chapter, add a short reference such as "See the Scoring chapter."
  The subsection heading already identifies the transform; do not retell its definition or import its method groups.
- Result: one self-contained sentence explaining what the composed transform publishes from its inputs.

Keep source group order. Avoid duplicated intent/explanation text, generic production commentary, and opaque terminology
where a concrete data or domain name would be clearer.

## Transform descriptions

Use plain, purpose-first language for someone meeting the transform for the first time. Internal descriptions may
use one or two connected sentences: explain what the transform contributes, then add the context needed to understand
its inputs or result. Do not force a single sentence or trade familiar words for compressed terms such as
"request-valid relation" or "admitted evidence population." This is orientation, not a second preamble or method inventory.

Use the matching Code description as the starting point:

~~~text
internal description = accessible_orientation(Code's plain class description, one_or_two_sentences)
external description = circled marker + call(supplied_inputs, external_transform, useful_result) + chapter_reference_if_documented
Format description = exact(Extend description)
~~~

Match by the owning root and transform or actual call, not by heading alone or item number. Online's two
`SelectGapQueries` classes have different responsibilities. Preserve a clear Code sentence when it already works;
otherwise adapt it for reading without nearby Python. Do not assemble a description by concatenating method items.
Check the actual call before reusing Code wording. If Code misstates responsibility, correct its owning prose when
authorized; do not reproduce the mistake in Implementation.

For an external stage, describe the delegation: which already-selected inputs the caller passes to the named transform
and what it obtains. Prefer a short call-focused sentence plus a chapter reference. Do not attribute the caller's
selection, gap detection, cache reconciliation, or publication to a callee that only processes supplied inputs.
For example: "Pass the queries selected for recalculation to `Filtering` to produce lexical filter scores. See the
Filtering chapter." "Fill lexical filter gaps" hides this division of responsibility.

Retain a limit or distinction when it helps explain the stage, without listing every argument or defensive check.
Complete inputs, outputs, and bindings remain in notation; internal methods explain their own details, and external
chapters own theirs. A second internal sentence should help the newcomer, not merely repeat the first.

Read descriptions together: each should move the story forward without repeating the preamble or its neighbors.
An external description names the called transform even though it appears in the heading: here the name identifies
the recipient of the inputs, rather than restating a title. Do not expand its implementation or enumerate score families.

Author this reading version in Extend and preserve it in Format. It does not rewrite Code or annotation, shorten the
Implementation preamble or Result, change method-group explanations, or alter any notation, ownership, or numbering.

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
