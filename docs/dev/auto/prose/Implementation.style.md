# Implementation narrative style

Apply this style to prose in Implementation sections, including the narrative body, preamble, stage introductions,
explanatory items, and external-stage explanations. Operator documents retain responsibility for section order, source
coverage, notation, and formatting rules.

## Narrative

- Treat Implementation as a narrative between the conceptual Solution and the mechanical Code section.
- Move from workflow purpose and main data movement to the relations, objects, policies, and boundaries that make the
  flow reliable.
- Introduce a concept before its first use. Add it to Definitions when a reusable domain explanation will help
  elsewhere.
- Use concrete subjects and active verbs. Give each sentence one main data transition, rationale, or connection.
- Explain why a boundary exists and what it guarantees to the next boundary. Include relevant validation, identity,
  ownership, failure, fallback, freshness, concurrency, and observability behavior.
- Distinguish caller-owned, transform-owned, provider-owned, and backend-owned responsibilities.
- Prefer thoughtful description and intent over prescription, status, checklists, or low-level operator inventories.
- Use the workflow notation as the source of truth without repeating every notation line mechanically.
- Keep field-level mechanics and source code in the detailed method prose or Code section. End with the implementation
  shape and observable behavior it enables.

## Preamble

The preamble before the first stage subsection or method group should be short and continuous. Introduce the workflow
purpose and main data movement first, then the core relation or object concepts, and finish with the policy or boundary
rationale that makes the flow reliable. For a composed workflow, name the actual parent transform and its actual stage
flow; for a standalone transform, name the input evidence, central transformation, and observable output. Occasional
connective words such as “then” or “from there” may make the movement easier to follow, but do not make progression the
organizing subject of the whole preamble. Keep it to the essential context, usually one to three short paragraphs. Do
not use bullets, exhaustive inventories, or references to numbered items, explanation prose, code, formatting, or other
document structure.

## Stage and item explanations

- Begin each stage subsection that has prose with one self-contained sentence describing its main data transition and
  responsibility.
- Give every explanatory item one self-contained sentence that explains the most meaningful data transition and why it
  performs it. Use an imperative opening only when it reads naturally; otherwise use a subject-led sentence.
- Keep explanatory prose accessible to a first-time reader. Prefer clear domain subjects such as scores, lanes,
  candidates, evidence, feedback, and results over vague subjects such as “the stage.”
- Preserve the source order and group boundaries established by the operator. Do not duplicate intent or explanatory
  prose in a separate numbered item or trailing paragraph.
- For external stages, use one source-backed sentence focused on the stage's responsibility and pair it with its
  canonical stage notation. Do not invent an explanation when no source-backed prose exists.

## Quality assurance

Verify that every Implementation narrative, preamble, stage introduction, and explanatory item follows the same
general-to-specific, concrete, active, rationale-aware style. Allow light connective cues where they improve readability,
but do not require a step-by-step progression. Reject prose that is merely a method inventory, repeats
code mechanics, hides ownership or failure behavior that the reader needs, or adds generic document-production
commentary. Reject preambles that describe what later document parts contain instead of explaining the live data flow.
