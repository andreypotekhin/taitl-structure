# Format operator

## Shared Prose context
This chapter operator is governed by the common concepts and conventions in [Prose.md](../Prose.md). Read its
text-process model, and shared authoring guidance before applying this file.
Definitions: [Definitions](Definitions.prose.md),
Styles: [Implementation narrative style](Implementation.style.md)

## Format
Create formatted documents (.form.md), based on extended documents (.ext.md).

### Format process
- Inputs: close/extended, .ext.md
- Output: close/form
- Scope: close/extended/search
- Name: Format. Usage: Format(dir)
- Invocation: manual

### Format operator
- Name: format(), usage: format(dir)
- Input: .ext.md
- Output: .form.md
- Goal: create formatted documents based on extended documents.

### Format operator instructions
Structure Formula Notation: see [Notation.md](Notation.md)

## Format operator instructions - General rules
When applying Structure Formula Notation:

Step method:
- Use the canonic step-method form:
  - omit method argument names and separating colons while retaining argument schema
    types;
  - omit field type annotations in the result schema definitions, while retaining its schema name and field names.
- Use 'Schema Notation - With Projection' notation for return schemas:
  - when source code returns Schema.project(...), Schema.base(...), or a projected call with added fields,
    use : show \\vdots for inherited/projected fields and list only fields introduced by that return expression.
  - Omit return schema definition (colon and vector), and only show return schema name(s), if:
    - If return schema definition for the schema is already shown in preceding formulas of same document.
    - If return schema is same as one of the argument schemas.
    - If the only content of return schema definition vector is lone ellipses (\\vdots).
  - Maintain a document-wide set of emitted schema definitions while formatting, including definitions emitted in
    other stage subsections. Once a schema's complete field vector has appeared, later returns of that same schema
    use only the schema name. Emit a return definition again only when the returned field set is genuinely different,
    such as a distinct projection.
- Keep short step-method formulas in one formula flow. Use 140 characters as the soft wrapping limit for the longest
  rendered arrow-bearing row, not for the sum of vertically stacked matrix rows. Wrap once at the arrow only when that
  row reaches the limit or visibly exceeds the viewer's content width because of long identifiers. Do not split the
  method call or return schema internally.

Step methods:
- Separate consecutive standalone step method formulas with two consecutive dedicated full lines (\\) rather than
  adjusting line height of the leading formula's final row;
- Every method with more than one argument uses vertical step-method notation: `\\operatorname{method}\\!\begin{pmatrix}`
  with one argument type per row. Horizontal comma-separated argument lists are prohibited in method formulas.

Non-step methods:
- Preserve every Python code listing from the extended document in the Code section.
- Convert every method listing to a formula, including non-step `@special` helpers and typed `@raw` helpers.
  When the method is not a transform step, use 'Non-step Method Notation' from `Notation.md` .

Convent preservation:
- In Implementation section, every numbered method group must use Structure formula notation inside `$$ ... $$`.
  Fenced `text` signatures belong only to `.ext.md` - never carry text notation into a formatted method group.
- Preserve each intent and explanation content.
- Preserve the extended document's Solution prose and paragraph order exactly; convert display-math delimiters only as
  required by the formatting rules.
- Preserve the extended document's Problem prose and paragraph order exactly; apply only the required formatting
  conversions.
- Preserve the extended document's Implementation preamble, stage-introduction prose, and italic formatting exactly.
  Do not add italic markup to plain upstream prose, do not force-break a sentence, and do not invent an italicized
  intent sentence. The preamble must remain continuous prose that moves from workflow purpose to core concepts, then to
  boundary or policy rationale; retain its short, essential shape rather than turning it into a list or adding
  document-structure commentary.
- Preserve the extended document's external-stage explanations and `Result` prose exactly; these passages follow the
  same style contract: one self-contained sentence when prose is present, concrete data transition, active voice when
  natural, and no generic inventory or document-production commentary.
- Preserve input document’s Stage group boundaries and method order.
- Private/helper methods remain Code listings and are not numbered Implementation method groups or Implementation
  formulas unless the source explicitly treats them as public methods.

Transform - standalone (as in 'Resulting transform shape' sections of .form.md docs):
- Use canonic transform notation: transform name, colon, input vector on the left, step-method vector in the middle
  and the output vector on the right. Omit \\odot.
- Put exactly one schema type on each input and output vector row; do not collapse multiple comma-separated types into one
  row, even when several methods consume or produce the same relation set.
- Preserve transform name in formula, including workflow stages such as Features; omit transform name in workflow Result formula.
- In the canonic transform shape, the middle method vector contains method names only. Do not place argument types,
  argument vectors, arrows, return schemas, or full step signatures inside method vector; those belong in the explanatory
  step formulas.
- Define external stages by source package: a stage is external when its transform class is outside the package tree rooted at
  the main/workflow transform; imports alone do not make a same-package or child-package stage external. For an external
  stage whose implementation and step methods are not discussed in the document, keep one canonical standalone transform
  notation: transform name followed by colon, schema types with omitted input/output names, and no `Resulting transform
  shape:` label or block. Internal stages retain their methods and one `Resulting transform shape:` block.

Stage call:
- Use 'Stage Call Notation' for stages in a workflow.
- In a composed-transform shape, retain one `name : Type` pair per input/output row and include the assigned stage's
  canonic method vector; do not substitute the child transform's method signatures for the stage call.

Main Transform - e.g. workflow transform in Result section of .form.md docs
- Omit transform name and colon.
- Show workflow inputs and final workflow outputs as name : Type pairs without value assignments.
- Use a smaller gap such as \\[2pt] between methods inside a dense workflow method vector.
- Add two dedicated full \\ rows between workflow inputs, each stage and final outputs so adjacent vectors do not
  visually merge.
- Apply the same two-row separation to every composed-transform formula, including a stage subsection's
  `Resulting transform shape:` and its multi-stage explanatory flow; do not let adjacent stage calls share a visual row.

Additional Rules
- Keep the Inputs, Outputs, and Stages sections in their source text form; formulas are applied to the individual
  step methods, non-step typed helpers, standalone transforms, and the workflow transform.
- Preserve every Code-section listing and provide formula notation for every typed method it contains. Code listings are
  source evidence; formulas are the compact typed representation of the same methods.
- Do not repeat the 'Resulting transform shape': label in the workflow's Result section when the
  transform notation is already shown in the preceding sections.

### Format operator - Quality assurance

- For every `.form.md` output, audit every formula block in the document.
- Verify the Implementation preamble remains before the first Stage subsection, method group, or numbered item; it must
  use continuous general-to-specific prose, introduce concepts before use, and contain no list markers or
  document-structure commentary.
- Outside fenced code, structured text notation, and display math, require ordinary paragraph and numbered-item
  continuation lines to start at column zero. Reject indentation introduced only by wrapping prose, because Typora
  renders those leading spaces as visible whitespace.
- Verify every display formula has balanced, properly nested `\\begin{...}`/`\\end{...}` environments. Reject an
  unclosed nested `aligned`, `gathered`, `pmatrix`, or `Bmatrix` environment, including when one is embedded in another.
- For every transform class, inventory all public `@step`, `@special`, and `@raw` methods in the collected source and
  verify that each appears exactly once in the corresponding Code section and has a matching Implementation group and
  formula. Include trailing methods that publish declared outputs; do not drop a method because a neighboring section
  contains a private helper.
- Step methods must use the canonic step method notation from `Notation.md`: argument names and separating colons
  are omitted, argument schema types remain, and every returned schema retains its name plus a field-name projection.
  Reject a bare return schema, lone ellipses (\\vdots) return schema, a missing `return_schema_definitions` projection,
  or an invented `\\vdots` projection when the source schema fields are available.
- Non-step method formulas must use Non-step Method Notation from `Notation.md`. Preserve the complete typed Python
  return expression for helpers rather than forcing it into a schema projection.
- Use the single-argument form `\\operatorname{method}(Type)` for exactly one argument. Never render a one-argument
  matrix, and never use `\\!` before its parentheses. The compact-space `\\!` is permitted only before a multi-argument
  matrix or a stage call; reject `\\operatorname{method}\\!(Type)` and any one-argument `pmatrix`.
- Keep formulas left anchored. Every `aligned` block must anchor its rows with `&`; do not use right-aligned display
  formulas or a leading unanchored continuation row. Separate consecutive standalone step formulas with two dedicated
  full `\\` rows, and use short spacing only between methods inside a dense workflow method vector.
- Escape every identifier underscore as `\\_` inside displayed formulas. A formula audit must reject any unescaped `_`,
  because subscripts are not part of Structure formula notation.
- Each standalone `Resulting transform shape:` must be the canonic step-transform shape: transform name and colon,
  typed input vector on the left, method `Bmatrix` in the middle, `\\rightarrow`, and output vector on the right. Do not
  replace that structure with a vertically stacked prose/list rendering or duplicate the transform name elsewhere.
- In every canonic transform method `Bmatrix`, each entry must be a method name only. Reject argument parentheses,
  argument matrices or types, return arrows, return-schema definitions, and any other full step-method notation inside
  the transform vector; full signatures belong only to the explanatory method formulas.
- In every canonic standalone transform input and output vector, put exactly one schema type on each row. Reject
  comma-separated type lists collapsed into one matrix row; multiple inputs or outputs must occupy separate rows.
- Whenever a step method, stage call, or other formula returns multiple schemas, represent the return schemas as one
  output `pmatrix` with exactly one schema per row. Reject comma-separated return lists, including lists in abstract or
  variant stage formulas.
- Require every typed `@special` and `@raw` method from the Code section to appear in formula notation. Use
  'Non-step Method Notation' for helpers and 'Step-method notation' for actual steps; reject any typed method omitted from
  the numbered item, its owning transform shape, or its applicable workflow method vector.
- For every typed step signature in an extended explanatory item, require exactly one corresponding step formula and
  require its method name in the owning standalone transform shape and any composed-transform stage method vector.
- For every numbered Implementation item, require exactly one immediately following method formula. The formula must
  describe the method named by that item, including a non-step helper when the item documents one.
- Reject generated prose that discusses document-production stages, says a helper is omitted from a formula, or refers
  to Code/Format as the reason a method is or is not represented.
- Use the GitHub/Typora-compatible `\\operatorname{...}` command for every displayed method name. Reject the invalid
  `\\operator{...}` form, raw text such as `extract: Document -> Document`, and any standalone shape whose name is not the
  exact source transform class.
- A workflow Result section must use canonic 'Composed Transform Notation': typed `name : Type` workflow inputs, assigned stage
  calls with name-only stage arguments, each stage's method vector and output vector, and typed final outputs without
  assignments. Omit the workflow name and do not repeat the `Resulting transform shape:` label in `Result`.
- For a composed root transform, use a `### Result` section for the parent composition; never emit the parent as an
  additional `Resulting transform shape:` block. Reserve that label for internal standalone stage shapes.
- Emit `### Result` only after verifying an exact parent/workflow class with composed stage assignments in the collected
  Code. If the topic contains no such class, reject any Result section, parent-named implementation narrative, or
  invented package-level transform notation.
- Classify a named shape as standalone or composed from the source transform before formatting. A composed transform must
  use `Composed Transform Notation - With name`: `TransformClassName :`, typed input/output name pairs, and assigned stage
  calls with their canonic stage method vectors. Never render a composed transform as a standalone step-transform shape.
- Check formula width and vertical spacing document-wide: keep short step-method formulas in one flow. Apply 140
  character threshold to the longest rendered arrow-bearing row, not to the aggregate source length of a formula with
  vertical matrices. Also inspect rendered width for long identifiers; wrap any row that still exceeds the viewer
  content width once at the arrow. Never split the method call or return schema internally. Matrix components may use
  natural rows, while standalone methods remain visually separated and method vectors stay denser than stage call gaps
  of a composed transform.
- For every `gathered` block containing consecutive standalone step-method formulas, require exactly two dedicated full
  `\\` rows between adjacent formulas. Count only full rows outside any `Bmatrix` method vector; reject zero or one
  separator rows and reject artificial `\\[12pt]` spacing used in their place.
- For the workflow formula in Result section, require exactly two dedicated full `\\` rows between the typed input
  vector, each assigned stage, and the typed final-output vector. Reject a directly adjacent row or a single separator row;
  the check must cover every workflow document, not only `SearchDocuments`.
- For every composed-transform formula outside `Result`, including internal stage shapes and explanatory stage flows,
  require exactly two dedicated full `\\` rows between adjacent stage calls and between the input/output vectors and
  neighboring stage calls. Reject directly adjacent calls or a single separator row.
- After applying the 140-character rule, reject any arrow-centered `aligned` split whose normalized call-plus-arrow row is
  at most 140 characters. The height of a return matrix must never trigger wrapping of an otherwise viewer-safe method.
- Preserve exact transform class name in every 'Resulting transform shape' formula, including workflow stages such as Features.
  The main transform Result section notation omits its transform name when the prose already identifies it.
- Inspect each source return expression for .project() and .base(). Use 'Schema Notation - With Projection' in the matching
  step formula: retain return schema name, show \\vdots for inherited/projected fields, and list only fields introduced
  by the projection call.
- Track emitted schema field vectors across the entire document, including all stage subsections. After a schema's
  complete definition has been shown once, reject later identical return-schema vectors and require the schema name
  alone. A repeated schema may show a definition only when its fields differ because of a distinct projection.
- Verify every arrow-bearing row at or above 140 characters, and every row that visibly exceeds the viewer content
  width, has at most one arrow-centered wrap. Do not wrap a formula merely because its vertically stacked matrix rows
  make the aggregate source block longer; verify shorter, viewer-safe formulas have no artificial wrap.
- Classify every stage by source package before formatting: only a transform outside main transform package tree is
  external. External stages may have one transform formula with a colon and unnamed schema vectors, but must
  not have a `Resulting transform shape:` block when their step methods are not discussed. Internal stages must
  retain their documented methods and exactly one `Resulting transform shape:` block.
- Verify every standalone transform notation names its exact transform class, composed transforms have visible
  spacing between input, stage, and output rows, and consecutive standalone step formulas contain two dedicated
  full `\\` separator rows.
