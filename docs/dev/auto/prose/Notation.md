# Structure Formula Notation

Use compact mathematical notation to show transform data flow, public methods, and returned records. This is a
presentation of source contracts, not permission to reduce their coverage.

## Chapter profile

Draft/Extend use complete text notation; Format uses the profile below. These selections are canonical for chapters;
the vocabulary at the end describes optional variants for other uses.

### Text coverage

Every public method has its own full named, typed argument list and concrete return type(s), even across similar grains.
For an untyped `@raw` method with declared data lanes, derive its logical data signature from `inout` and those lane
schemas. State that this is the declared data contract, not a Python annotation; preserve runtime-injected arguments
such as `spark` and `ctx` in Code without inventing schema types for them. Include the raw operation in its group and
shape. If neither annotations nor lane declarations establish the contract, report the missing evidence.
Transform blocks enumerate all named inputs/outputs and all methods or actual child calls. Stage calls retain every
binding, including policy constants; their arrows name unqualified output relations. Final outputs remain named and
typed. Never replace known paths or types with "same pattern," "etc.," or ellipses.

### Formula selection

| Construct | Chapter rendering |
|---|---|
| One-argument public method | `\operatorname{method}(Schema) \rightarrow Return`; no `\!` |
| Multi-argument public method | `\operatorname{method}\!\begin{pmatrix} A \\ B \end{pmatrix} \rightarrow Return` |
| Multiple returns | Complete return-schema vector, not a selected subset |
| Public typed non-step helper | Non-step Method Notation; preserve its complete Python type expression |
| Internal step shape | Named transform + input-schema vector + complete method-name Bmatrix + output-schema vector |
| External boundary | Named transform + input-schema vector -> output-schema vector; no method vector |
| Composed Result | Unnamed composition: named typed inputs + all actual stage calls + named typed final outputs |

In step shapes, list method names only; their complete signatures already appear in the method-group formulas.
No `\odot`. In formula Results, omit the parent transform name, but retain each stage's assigned-to alias:
`inferred_documents = InferDocuments(...)`. Call input vectors contain source relation references, including dot
qualification when consuming an earlier stage's output; `results=inferred_documents.results` becomes
`inferred_documents.results`, not an ambiguous `results`. Resolve local forwarding aliases to their producer where
needed, so `valid_policy = validated.valid_policy` is shown as `validated.valid_policy` when consumed. Do not put
argument-keyword assignments or schema types inside calls. Stage output vectors retain unqualified output names only.
Keep complete source bindings in Draft/Extend text notation and Code for verification. The composition's
opening inputs and final outputs remain named and typed, without value assignments. Internal step calls carry their complete
method-name vector; external and composed calls do not acquire a fictitious step vector. Each internal composed
transform has its own Result, including a composed child.

### Specializations

Apply the specialization model in Definitions only when an exact named base is documented elsewhere or already
expanded in this chapter. Text notation lists the complete effective inputs/outputs and identifies that base plus
every local method replacement, stage replacement, output rebinding, and parameter change. Give local methods their
ordinary complete signatures and returned-record definitions. Do not invent an inheritance stage or silently discard
unchanged inherited members: the named base supplies them.

In Form, render the effective body as `Base` followed by a normal-size bracketed replacement vector. Method replacements
reference their fully defined local methods; replacement calls retain aliases, complete source references/constants,
and unqualified output names. A specialized step uses this body in its named Resulting transform shape; a specialized
composition uses it in its unnamed Result between complete named typed inputs and outputs. Keep replacement vectors
at normal formula size, not in subscripts. This explicit base-plus-replacements expression is not permission to shorten
ordinary step shapes or composed Results.

### Return definition state

Resolve fields from source, never from guesses. A projected return should explain the operation, not merely echo its
last keyword arguments or reproduce an entire inherited record. Select its visible fields by this rule:

~~~text
visible_fields = explicit_additions_and_overrides + essential_carried_fields
~~~

Always show every field explicitly supplied by the returned construction, including fields after `.project()` or
`.base()`, even when a similar projection appeared earlier. Add inherited fields only when they establish the returned
grain/key or carry the evidence this operation assembles. Ground that choice in the grouping/join/rekeying logic and
the method-group explanation, not a fixed field-count quota or possible future uses. Preserve the schema's field order.

For example, sentence materialization can show `vdots, content`: its coordinates and identity are unchanged context.
A normalized occurrence needs its document/section/paragraph/sentence keys and term; a grouped count needs its complete
grain key, term, and frequency. A public posting assembled from counts, target statistics, and population frequency
must expose those contributed facts rather than reducing its return to `vdots, term, term_frequency`.

Use ellipses only when fields are actually omitted. A small core record may legitimately show every field without
ellipses. Full construction and identity pass-through retain the existing first-definition rule. Track full definitions
across the whole document, independently of numbering roots; a partial projection never counts as a full definition.

~~~text
render_return(schema, construction, essential_carried_fields, seen):
    if construction is identity project/base (same input schema, no explicit fields):
        construction = pass_through
    if construction is project/base:
        visible = schema_order(ALL explicit_fields + essential_carried_fields)
        if visible is empty: return schema.name
        omitted = schema.fields - visible
        if omitted is empty: record schema in seen.full
        return schema.name : vector(vdots if omitted, visible)
    if schema in seen.full:
        return schema.name
    return schema.name : vector(ALL schema.fields); record schema in seen.full
~~~

Full construction or pass-through of an unseen schema requires the full field vector even when input and return types
match (including `InferencePolicy.project(policy)`). A partial projection does not count as a full definition.
Omit field type annotations, not selected field names. Never emit a lone `\vdots` vector, hide an explicit override,
or remove an essential key or contributed value solely to make a formula narrower.

### Display

Use balanced `$$` blocks. Escape identifier underscores as `\_`; no backticks inside formulas. Use `\operatorname` for
method/transform names. Left-align multiline formulas with `aligned`. Separate standalone method formulas and composed
stage rows with two dedicated empty `\\` rows; compact matrix rows may use `\\[2pt]`.

Use the same font size for every formula; never shrink a Result or long call to fit. The default is one horizontal
expression per method, step shape, or stage call: name, input vector, method vector when applicable, arrow, and output
stay together. Matrix rows are not a reason to break the surrounding expression. Multiple inputs or a long method
inventory do not automatically require multiple equation rows.

Do not treat an arbitrary test panel (such as 1,000 pixels) or LaTeX character count as a chapter page-width limit.
Check the intact expression first. Where the medium supports horizontal scrolling, preserve the mathematical shape
and let the container scroll. Wrap only for a genuine constraint of the intended reading layout, recording the reason;
then break at the arrow before separating a transform name from its input/method vectors. Keep vectors intact.

## Text notation

Use fenced text in Draft Notation and Extend Implementation. These are grammar examples, not literal chapter content.

~~~text
method(argument_name: ArgumentSchema, other_name: OtherSchema) -> ReturnSchema

StepTransform:
  inputs:
    input_name: InputSchema
  methods:
    method: ArgumentSchema, OtherSchema -> ReturnSchema
  outputs:
    output_name: OutputSchema

ComposedTransform:
  inputs:
    input_name: InputSchema
  stages:
    stage_alias = StageName(input_name=input_name, policy=policy) -> output_relation
  outputs:
    output_name: OutputSchema
~~~

Repeat every real input, method, stage, binding, and output. An external boundary uses the named input/output form without
a method or stage inventory. Root and internal composed-transform blocks belong in Result, not under the step-shape
label. Draft's single Notation section inventories the same contracts without Implementation subsections.

## Formula examples

### Schema notation

Full definition:

~~~latex
SchemaName : \begin{pmatrix} first\_field \\ second\_field \end{pmatrix}
~~~

Projected return with an explicit field:

~~~latex
SchemaName : \begin{pmatrix} \vdots \\ supplied\_field \end{pmatrix}
~~~

### Step and non-step method notation

~~~latex
\operatorname{single}(InputSchema) \rightarrow ReturnSchema

\operatorname{multiple}\!\begin{pmatrix} InputSchema \\ OtherSchema \end{pmatrix}
\rightarrow \begin{pmatrix} FirstReturn \\ SecondReturn \end{pmatrix}

\operatorname{default\_sentence\_spans}(Any)
\rightarrow \operatorname{list}\{\operatorname{dict}\{str, object\}\}
~~~

Apply return-definition state to every returned schema. Public typed helpers retain complete Python type expressions
without being reclassified as steps. Omit only receiver parameters such as `self` from method arguments.

### Step transform notation

~~~latex
\operatorname{StepTransform} :
\begin{pmatrix} InputSchema \end{pmatrix}
\begin{Bmatrix} \operatorname{first\_method} \\ \operatorname{second\_method} \end{Bmatrix}
\rightarrow \begin{pmatrix} OutputSchema \end{pmatrix}
~~~

### Stage call and composed transform notation

~~~latex
\begin{aligned}
& \begin{pmatrix} input\_name : InputSchema \\ policy : PolicySchema \end{pmatrix} \\
\\
\\
& internal = \operatorname{InternalStep}\!\begin{pmatrix}
input\_name \\ policy
\end{pmatrix}
\begin{Bmatrix} \operatorname{first\_method} \\ \operatorname{second\_method} \end{Bmatrix}
\rightarrow \begin{pmatrix} output\_relation \end{pmatrix} \\
\\
\\
& \begin{pmatrix} output\_name : OutputSchema \end{pmatrix}
\end{aligned}
~~~

Repeat each stage call in source order, keeping its assignment alias and all input/output names. Producer-qualified
input references preserve dependencies; output names remain unqualified. Do not label the whole Result with the parent
transform name. External calls omit the method vector. Composed internal calls also omit it;
their own stages are shown in their nested Result, not replaced by fictitious methods.

## Variant vocabulary

Variants describe presentation choices, not source omissions. Chapter outputs use the profile above; do not select a
different compact variant ad hoc.

| Variant | Meaning |
|---|---|
| with_name | Prefix a schema or transform definition with its name and colon |
| as_expression | Replace a transform's defining colon with `\!` for use in a call |
| with_projection | Elide inherited fields, retain explicit fields under the definition-state rules |
| omit_argument_names | Keep argument types, remove parameter names and their colons |
| omit_argument_types | Keep argument names, remove their type annotations |
| omit_input_names / omit_output_names | Keep relation schemas, remove relation names and colons |
| omit_input_types / omit_output_types | Keep relation names, remove schema annotations |
| omit_steps | Omit the method vector from a call |
| omit_return_types | Show method names without signatures inside a transform's method vector |
| use_parentheses / use_brackets | Select pmatrix or bmatrix/Bmatrix delimiters |

"Canonical" means the profile-selected variant for that construct; it does not imply the same fields are shown in a
standalone method, a step summary, and an assigned stage call. Complete standalone signatures remain mandatory.
