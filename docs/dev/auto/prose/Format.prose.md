## Format

### Format process

Present the current extended chapter as a formatted document under `close/form/`, preserving its relative path.
This manually invoked process changes presentation and shortens public Implementation explanations for reading;
it preserves the chapter's other narrative, ownership, method inventory, and Code.

~~~text
Format(topic, extended, source_contracts) -> close/form/<topic-path>/<Topic>.form.md
~~~

### Format operator

Preserve the extended chapter while applying the chapter's mathematical and typographic conventions.
Read [Prose.md](../Prose.md), [Definitions.prose.md](Definitions.prose.md),
[Notation.md](Notation.md), and [QA.prose.md](QA.prose.md), then instantiate [Form.prose.temp.md](Form.prose.temp.md).
Check upstream narrative against [Solution.style.md](Solution.style.md) and
[Implementation.style.md](Implementation.style.md); only the latter's Format explanation projection permits rewriting.

1. Validate the extended chapter against its inventory. A missing stage, signature, group intent, Result, or explanation
   is an upstream defect, not permission to improvise one during formatting.
   Use the current extended document as the only structural/prose input: an existing Form is never a template for
   retaining stale groups or sections. Inventory and convert notation across the entire pre-Code body, not only Solution.
   Before conversion, account for each stage's actual methods, explanation groups, and notation blocks in the QA record;
   unresolved placeholders or a generic class-level item fail this gate even when Solution and the preamble are complete.
2. Preserve paragraph order, headings, group boundaries, circled Implementation markers, and decimal Code numbers.
   Apply Implementation.style's Format explanation projection to public method-group explanations only, matching Code
   by transform and method membership. Preserve all other prose, including Intent, Problem, Solution, and preamble.
   Preserve the entire Code section verbatim, including Python whitespace. Never use Implementation to edit Code.
3. Apply these presentation changes outside Code:
   - Definitions: bold concept name without colon, with its exact definition sentence in one indented sub-bullet.
   - Stages: bold the stage name only; Builds on, Used by, Inputs, and Outputs remain unbolded.
   - Text notation: replace every notation block outside Code, including Solution models and all Implementation
     signatures, step shapes, external boundaries, and composed Results, with display formulas under Notation's chapter
     profile. The only notation in Form is formula notation: no residual text/LaTeX fences or plain signature blocks.
     Keep every method and concrete return; types replace method argument names, not methods.
   - Display mathematics: balanced $$ blocks, including Solution formulas. Do not expose inline-dollar LaTeX as prose.
4. Resolve return-field definitions from source contracts using Notation's document-wide definition state. For a
   projection, keep every explicit addition/override plus the source-backed keys and payload needed to explain this
   operation; elide only the remaining context. Never infer fields from a schema name or omit an unseen full definition
   because input and return types match.
   Check every first return against the definition ledger, including each return in a grouped formula and projections
   with no explicit assignments. Revalidate essential fields even if the formula is unchanged from an earlier chapter.
5. Require a one-to-one source-notation/formula mapping and a nonempty preserved preamble before accepting the output.
   Run QA's shared and Format checks, including the explanation mapping, exact preserved-prose/Code comparisons,
   and a visual math check when a renderer is available. Report verification limits; do not claim a visual render
   from delimiter checks alone.

The output tree is the extended tree: every internal composed transform retains its own Result. Formatting neither
creates a workflow nor suppresses independent transforms.
