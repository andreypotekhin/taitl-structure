# {{Topic}}

<!--
Formatted output template for the Format operator.
Input: the completed .ext.md. Preserve its prose and paragraph order; convert only the prescribed notation and
formatting. No top-level Notation section is emitted.
Use the phrase search engine at most once, and omit it when it adds no value.

Shape variants:
- Standalone transform / no composed workflow: one transform subsection with one Resulting transform shape and no
  parent Result.
- Composed internal-only workflow, as in Chunking: Implementation has internal stage shapes followed by one parent
  Result; Code has Workflow first, followed by the collected stage/class subsections.
- Composed internal-plus-external workflow, as in Similarities: Implementation has internal stage shapes, external
  standalone stage-call formulas without Resulting transform shape, then one parent Result; Code has Workflow first,
  followed by the collected stage/class subsections.
-->

## Problem

{{Exact Problem prose from .ext.md; convert display formulas to balanced $$ blocks only.}}

## Solution

{{Exact Solution prose and paragraph order from .ext.md; convert display formulas to balanced $$ blocks only.}}

## Builds on

- {{Plain canonical principal topic or workflow}}

## Used by

- {{Plain canonical principal topic or workflow}}

## Definitions

- **{{Concept}}**
  - {{Exactly one indented definition sentence.}}

<!-- Include every essential domain term used by the chapter, including Term when normalized term artifacts appear. -->

## Inputs

- {{Plain input schema or relation}}

## Outputs

- {{Plain output schema or relation}}

## Stages

- **{{StageName}}**: {{schema names and arrows remain plain}}

<!-- Stage names only are bold. Do not emit ## Notation. -->

## Implementation

{{Exact Implementation preamble and stage-introduction prose from .ext.md. Preserve continuous prose and paragraph
order.}}

### {{InternalStageName}}

{{Exact stage introduction and numbered group prose from .ext.md. Preserve circled markers and italic formatting.}}

$$
\operatorname{{public\_method}}\!\begin{pmatrix}
{{ArgumentSchema}} \\
{{ArgumentSchema}}
\end{pmatrix}
\rightarrow {{ReturnSchema}} :
\begin{pmatrix}
{{introduced\_field}} \\
{{introduced\_field}}
\end{pmatrix}
$$

<!-- For project/base returns, show \vdots together with every field introduced by the projection. Use a complete field
vector when the returned schema has not yet been defined; never replace known fields with a lone \vdots. -->

Resulting transform shape:

$$
\operatorname{{InternalStageName}} :
\begin{pmatrix}
{{InputSchema}} \\
{{InputSchema}}
\end{pmatrix}
\begin{Bmatrix}
\operatorname{{public\_method}} \\
\operatorname{{public\_method}}
\end{Bmatrix}
\rightarrow
\begin{pmatrix}
{{OutputSchema}} \\
{{OutputSchema}}
\end{pmatrix}
$$

### {{ExternalStageName}}

{{Exact plain, source-backed external-stage description from .ext.md, including its circled marker. Do not italicize
it or invent an intent.}}

$$
\operatorname{{ExternalStageName}} :
\begin{pmatrix}
{{InputSchema}} \\
{{InputSchema}}
\end{pmatrix}
\rightarrow
\begin{pmatrix}
{{OutputSchema}} \\
{{OutputSchema}}
\end{pmatrix}
$$

<!-- External stages have no typed step methods, method vector, or Resulting transform shape. -->

### Result

{{Exact Result prose from .ext.md.}}

$$
\begin{aligned}
&\begin{pmatrix}
{{input\_name}} : {{InputSchema}} \\
{{input\_name}} : {{InputSchema}}
\end{pmatrix} \\
\\
\\
&{{stage\_alias}} = \operatorname{{StageName}}\!\begin{pmatrix}
{{stage\_input\_name}} \\
{{stage\_input\_name}}
\end{pmatrix}
\begin{Bmatrix}
\operatorname{{stage\_method}} \\
\operatorname{{stage\_method}}
\end{Bmatrix}
\rightarrow
\begin{pmatrix}
{{output\_relation\_name}} \\
{{output\_relation\_name}}
\end{pmatrix} \\
\\
\\
&\begin{pmatrix}
{{output\_name}} : {{OutputSchema}} \\
{{output\_name}} : {{OutputSchema}}
\end{pmatrix}
\end{aligned}
$$

<!--
Result is present only for an exact composed parent/workflow class. Assigned stage arrows expose unqualified
relation names, never schema types or qualified references. Final outputs are typed and unassigned.
-->

## Code

<!-- Include Workflow only when the extended document represents a composed parent/workflow class. -->

### Workflow

{{Preserve the Workflow prose from .ext.md. If it has no source-grounded description, place one concise workflow
summary here. Never place an intro/scope line between Code and Workflow.}}

~~~python
{{Exact Python listing copied from .ext.md / .cnd.md.}}
~~~

<!-- For a standalone transform, omit Workflow above and begin with the transform class subsection. -->

### {{InternalStageName}}

{{Preserve every Code prose clause exactly, including one short italicized intent and its independent non-circled
number for each public method group. Keep Workflow, class, stage-assignment, plain explanatory, and private/helper
clauses unnumbered.}}

~~~python
{{Exact Python listing copied without alteration.}}
~~~

<!--
Code method-group numbering is independent from circled Implementation numbering and never follows Implementation
content. Every collected listing appears once, in source order. Separate adjacent code listings with ordinary prose.
-->

<!--
QA checklist:
- Problem and Solution prose and paragraph order are preserved.
- Builds on, Used by, Inputs, and Outputs are plain lists; Stages bolds stage names only.
- Definitions use bold names without colons and one indented definition sentence per item.
- There is no top-level Notation section.
- Every Implementation group keeps its circled marker and is followed by exactly one formula.
- Every internal stage has complete public method coverage and one Resulting transform shape.
- Every external stage has one plain numbered description and one standalone formula, with no Resulting transform shape.
- Every composed Result formula uses typed named workflow inputs, unqualified stage-output relation names, and typed
  final outputs.
- All displayed formulas use balanced $$ delimiters and balanced LaTeX environments with escaped identifier
  underscores.
- Code listings and Code prose are preserved exactly; Code numbering remains an independent sequence.
- No unnumbered transform/stage prose was converted into a long italic intent, and no method group is placed after its
  listing.
-->
