# Problem and Solution narrative style

Apply this style to Problem and Solution sections in draft, extended, and formatted chapter documents. Operator
documents retain responsibility for section order, source coverage, and notation.

## Problem

- Ground the opening in the motivating use case, then narrow to the topic's specific industry requirement in
  preferably one focused paragraph. A Problem section may be a single vivid sentence when that fully states the need.
- Introduce only the domain concepts needed to make that use-case requirement clear, then state the user's desired
  outcome. Do not turn Problem into a catalog of implementation challenges, risks, or possible failure modes.
- Use concrete subjects and active, lively verbs. Prefer expressive language over abstract status or requirement
  language.
- State the problem and its consequences, then stop. Do not explain how to solve it, name a proposed abstraction,
  describe a transform or workflow, introduce solution algorithms, or prescribe implementation requirements.
- Treat boundaries, policies, indexes, formulas, stages, and data structures as possible solution material. If a sentence
  tells the reader what the system should build or how it should behave, move it to Solution, Design, or Implementation.
- Avoid document-production commentary and phrases such as “The problem is to” when a direct statement is clearer.
- Avoid 'tension', 'problem', 'issue' in section text (evident from section heading).

## Solution

In this section, “solution” means the conceptual and practical answer to the stated problem, not the system's design or
implementation plan.

- Make Solution the conceptual center: explain the general theory and practice that answer the
  stated use-case problem, then move to the topic's central abstraction, behavior, and tradeoffs.
- Begin with a short general introduction to the domain practice before naming project-specific components or describing
  the concrete data structure. When the topic centers on query or request structure, include representative examples
  early enough for a first-time reader to see how the abstraction is used.
- Write direct narrative. Do not announce the section with “The solution is” or refer to “this Solution section.”
- Use active, expressive language with concrete subjects and varied sentence rhythm. Favor explanatory verbs such as
  represent, compare, preserve, connect, and recover. Avoid sketches that merely name components without explaining how
  their ideas fit together.
- Define concepts before using them, and explain why each concept matters to the user or system.
- Explain the reason the approach works and the tradeoffs that shape it, such as precision versus recall,
  context versus focus, flexibility versus consistency, or freshness versus cost.
- Keep transform names, stage mechanics, method inventories, schemas, implementation requirements, and code in later
  sections. Named components may appear only when they clarify the conceptual model; do not turn Solution into a system
  design or architecture list.
- Avoid 'solution', 'approach', 'design', 'practice' in section text (evident from section heading).

## Formula and continuity

- Include a formula, model, or monochrome diagram when it makes the concept materially clearer, and introduce its
  symbols in prose before displaying it.
- In extended documents, use GitHub/Typora display math with `\[` and `\]` (or an equivalent supported display block).
- In formatted documents, convert every display formula to a balanced `$$ ... $$` block. Never leave LaTeX delimiters or
  formula text as a lone `$` or as plain prose.
- Preserve the same conceptual narrative across Problem and Solution while allowing the formatted document to change
  only the required mathematical delimiters and typography.

## Quality assurance

Verify that Problem is grounded in the motivating use case and contains the need, difficulty, and consequences only.
Verify that Solution gives a complete, general, expressive account of the theory and practical answer without drifting
into design or implementation. Neither section may refer to document-production structure, and each formula must render
as display math in the target format.
