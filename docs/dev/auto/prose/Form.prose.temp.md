# {{Topic}}

<!--
Formatted output template for the Format operator.
Input: the completed .ext.md. Preserve its prose and paragraph order; convert only the prescribed notation and
formatting. No top-level Notation section is emitted.
Use the phrase search engine at most once, and omit it when it adds no value.

Shape variants:
- Standalone transform / no composed workflow: one transform subsection with one Resulting transform shape and no
  parent Result.
- No main/workflow transform but multiple independent collected composed transforms, as in Offline: one formatted
  top-level transform subsection per transform; each has local Implementation and Code numbering, nested internal/
  external stage subsections, and one nested Result. Do not synthesize a package-level parent, Workflow, or Result.
- Composed internal-only workflow, as in Chunking: Implementation has internal stage shapes followed by one parent
  Result; Code has Workflow first, followed by the collected stage/class subsections.
- Composed internal-plus-external workflow, as in Similarities: Implementation has internal stage shapes, external
  standalone stage-call formulas without Resulting transform shape, then one parent Result; Code has Workflow first,
  followed by the collected stage/class subsections.
-->

## Problem

{{Exact Problem prose from .ext.md; convert display formulas to balanced $$ blocks only.}}

## Solution

{{Exact Solution prose and paragraph order from .ext.md; preserve the Solution contract's general domain opening,
conceptual bridge, concrete abstraction/formula progression, and closing account of behavior, tradeoffs, and value;
convert display formulas to balanced $$ blocks only.}}

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

{{Exact substantive Implementation preamble and active stage-introduction prose from .ext.md. Preserve continuous prose,
paragraph order, inline class/schema formatting, and the explanation of important limits.}}

### {{InternalStageName}}

{{Exact plain stage introduction plus numbered group prose from .ext.md. Keep the stage introduction unnumbered and plain;
preserve circled markers and italic formatting only for method-group items. Include every assigned child stage in nested
subsections, using boundary-only formulas for external children. For an independent composed top-level transform, keep
its child stages and nested Result together and restart its circled sequence at ①.}}

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

<!-- For a composed stage, replace the step-transform shape above with composed-transform notation: named typed inputs,
assigned child-stage calls in source order, the assigned internal child's method vector when applicable, unqualified relation
outputs, and typed final outputs. For an independent composed top-level transform, put this whole-transform notation in a
nested Result subsection instead of under Resulting transform shape. -->

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

<!-- For a common parent/workflow, use the following root Result. For independent composed transforms, repeat the same
block as a nested `#### Result` inside each top-level transform subsection, after that transform's child stages. -->

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
<!-- For a no-parent topic, also omit Workflow and begin with the first collected internal transform subsection in source order. -->

### {{InternalStageName}}

{{Preserve every Code prose clause exactly, including one short italicized intent and its independent non-circled
number for each public method group. Use one sequence across a common workflow, but restart it for each independent
composed top-level transform. Keep Workflow, class, stage-assignment, plain explanatory, and private/helper clauses
unnumbered. Keep each Code stage transform description plain and unnumbered before its class listing. Place the numbered
italicized intent and method-group explanation together in one item immediately before the method listing it owns; never
leave a separate unnumbered explanation after that item.}}

<!-- The stage description precedes the unnumbered class listing. Each public method group follows that listing as one
numbered italicized item plus its explanation, immediately followed by that group's exact method listing. -->

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
- Solution satisfies the contract inherited from `.ext.md`: general domain/user framing first, conceptual bridge second,
  concrete abstraction and formulas later, and behavior/tradeoffs/value at the end.
- Builds on, Used by, Inputs, and Outputs are plain lists; Stages bolds stage names only.
- Definitions use bold names without colons and one indented definition sentence per item.
- There is no top-level Notation section.
- Every Implementation group keeps its circled marker and is followed by exactly one formula.
- Every internal step stage has complete public method coverage and one Resulting transform shape. Every independent
  composed top-level transform has complete nested child-stage coverage and one nested Result.
- When no parent/workflow class exists, every collected transform class is still internal. Standalone classes get complete
  Code and Implementation coverage with a step-transform shape; independent composed classes get complete nested stage
  coverage and one nested Result, with no synthetic package-level parent subsection or Result.
- For an internal step child, reject any standalone transform formula before its method formulas; its transform notation
  appears once under Resulting transform shape. Keep generated prose free of text-operator terminology; use a direct
  chapter reference when external detail is omitted.
- Audit each internal step transform for duplicate named notation; for example, AllDocumentTargets may appear once under
  Resulting transform shape and nowhere else in its internal stage subsection.
- Resolve internal child assignments recursively across collected family directories before classifying boundaries; a child
  called by an internal stage retains its complete method-group formulas and exact Code listing in the owning subsection.
- For a no-parent composed class with no public methods, preserve its single source-backed numbered transform-group
  description in both sections without creating method formulas or extra Code items.
- For a nested external call from a no-parent internal class, preserve only its boundary subsection and canonical formula,
  without importing external methods or Code.
- Every external stage has one plain numbered description and one standalone formula, with no Resulting transform shape.
- Every composed Result formula uses typed named workflow inputs, unqualified stage-output relation names, and typed
  final outputs. For independent composed top-level transforms, require one nested Result per transform and restart both
  circled Implementation numbering and Code method-group numbering for each transform.
- All displayed formulas use balanced $$ delimiters and balanced LaTeX environments with escaped identifier
  underscores.
- Code listings and Code prose are preserved exactly; Code numbering remains an independent sequence.
- Every public method listing has one immediately preceding numbered item with a short italicized intent and its
  method-group explanation, with no separate unnumbered explanation after that item. Stage descriptions and
  class/interface listings remain unnumbered and do not consume a method-group number.
- No unnumbered transform/stage prose was converted into a long italic intent, and no method group is placed after its
  listing.
-->
