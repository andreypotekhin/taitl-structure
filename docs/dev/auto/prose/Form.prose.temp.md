# {{Topic}}

<!-- Format.prose.md preserves the full Extend template tree and prose except the declared public-group explanation projection.
This template specifies substitutions, not a second independently authored chapter.
Emit unchanged sections/paragraphs in their original positions, not these placeholder summaries. -->

<!-- A specialization keeps Extend's exact base/replacement contract. Render it with Notation's normal-size replacement
vector, preserving every effective input/output and local replacement. Never render inheritance as a called stage. -->

{{Exact Intent, Problem, Solution, Builds on, and Used by sections; display math uses $$ delimiters.}}

## Definitions

- **{{Concept}}**
  - {{Exact definition sentence.}}

{{Exact Inputs and Outputs sections.}}

## Stages

- **{{StageName}}**: {{unchanged plain inputs and outputs}}

## Implementation

{{Exact complete preamble paragraphs, before any subsection heading; this slot is mandatory.}}

{{Exact internal/external headings, introductions, markers, intents, and Result prose, in their existing order.
For each public method group only, use the matching Code explanation as the base for a concise explanation,
preferably one sentence after the unchanged short italic intent. Match by transform and methods, not Code number.}}

<!-- Substitute each notation block in place using Notation.md#chapter-profile.
These fragments illustrate the distinct block types; do not insert extra blocks or headings.
Every source notation block outside Code must be replaced by a formula. No text or LaTeX fences survive here. -->

<!-- Keep the method and its return on one equation row by default; multiple arguments use a vertical pmatrix,
not an automatic equation break. Each first return has a field definition, including pure projections and every method
in a group. A projected return shows explicit additions/overrides and essential carried fields (including grouping keys
and defining payload), with vdots only for actual omitted context. Recheck unchanged formulas against source. -->
$$
\operatorname{method}(ArgumentSchema) \rightarrow ReturnSchema :
\begin{pmatrix}
field \\
field
\end{pmatrix}
$$

<!-- Internal step summary, only following the existing Resulting transform shape label. -->
$$
\operatorname{InternalStepName} :
\begin{pmatrix} InputSchema \end{pmatrix}
\begin{Bmatrix} \operatorname{method} \\ \operatorname{other\_method} \end{Bmatrix}
\rightarrow \begin{pmatrix} OutputSchema \end{pmatrix}
$$

<!-- External boundary: no methods, no shape label, no expansion of the external class. -->
$$
\operatorname{ExternalStageName} :
\begin{pmatrix} InputSchema \end{pmatrix}
\rightarrow \begin{pmatrix} OutputSchema \end{pmatrix}
$$

<!-- Composed Result: nested at its existing depth, after its children; no parent transform name.
Keep each stage call intact on one equation row and use the same font size. Wrap only for an established reading-layout
constraint, not a fixed test-panel width. -->
$$
\begin{aligned}
& \begin{pmatrix} input\_name : InputSchema \end{pmatrix} \\
\\
\\
& stage\_alias = \operatorname{StageName}\!\begin{pmatrix} earlier\_stage.output\_relation \end{pmatrix}
\rightarrow \begin{pmatrix} output\_relation \end{pmatrix} \\
\\
\\
& \begin{pmatrix} output\_name : OutputSchema \end{pmatrix}
\end{aligned}
$$

<!-- Repeat every real call with its assignment alias and producer-qualified input references where applicable.
Output names stay unqualified; omit argument-keyword assignments and types inside calls, not producer identity.
Complete bindings remain in Extend text and Code. Internal step calls also carry their complete method-name vector.
Return-field vectors follow Notation's definition-state rules, not the illustrative two-field vector above. -->

## Code

{{Entire Code body copied verbatim from .ext.md, including headings, prose, numbers, intents, and Python listings.}}
