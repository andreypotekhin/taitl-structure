# Prose automation

## Reference
Documenting Automation: [Documenting.auto.md](Documenting.auto.md)
Documenting: [Documenting.md](../Documenting.md)
Source Annotation: [Annotation.prose.md](prose/Annotation.prose.md).
Definitions: [Definitions.prose.md](prose/Definitions.prose.md)
Implementation narrative style: [Implementation.style.md](prose/Implementation.style.md)
Problem and Solution narrative style: [Solution.style.md](prose/Solution.style.md)


## Text processes
Text process is the process of creating documentation content from existing code and texts.
It can be automated (e.g. triggered by code changes on task completion), or manual (triggered with a task).

Text process may be thought of as automated or semi-automated content pipeline.
Example: source annotation process is defined in [Annotation.prose.md](prose/Annotation.prose.md). It is an automated process.

Text process definition includes the following:
- Establishes the scope, inputs and outputs (dirs).
- Defines synchronization policy: e.g. source annotation automatically synchs on underlying code changes.
- Instructions on content transformation
- Points on authoring style/character
- Tips, notes, exceptions.

The process notation resembles a callable Python class, but is intended for communicating, not execution.
Ex: Annotate(dir_name) invocation in an automation task.

### Existing text processes
- Annotation, defined in [Annotation.prose.md](prose/Annotation.prose.md).
- Documentation, defined in 'Documentation pipeline' section of [Documenting.md](../Documenting.md)
  and [Documenting.auto.md](Documenting.auto.md)

## Text operators
Text operator is a code-to-text or text-to-text transformation bearing certain authoring style,
e.g. optimized for brevity, generality etc.

Example:
- Annotation.prose.md defines annotate() text operator in subsections 'Example', 'General tips' of 'Creating annotated code' section.
- It also describes how annotate() text operator applies to narrower contexts:
  - Annotated source for example code.
  - Annotated source for non-example code.

The operator notation resembles Python function call with named parameters, but is intended for communicating, not execution.
Ex: annotate(dir_name) invocation in an automation task.

Text operator definition only includes text transformation, not where to apply it,
so additional instructions (like dir_name parameter) is needed for invocation.

### Existing text operators
Existing text operators:
- baseline(): not explicitly mentioned; serving as baseline for all other operators
  - defined by 'Documenting ...' sections of [Documenting.md](../Documenting.md) and [Style.md](../Style.md)
- top_level(): defined by
  - section 'End-User documentation' of [Documenting.md](../Documenting.md)
  - section 'End-User top-level documentation' of [Documenting.auto.md](Documenting.auto.md)
- background(), reference(), recipes(): defined in
  - section 'End-User reference documentation' of [Documenting.auto.md](Documenting.auto.md)
  - section 'End-User documentation tips' of [Documenting.auto.md](Documenting.auto.md)
- developer(): defined in
  - section 'Developer documentation' of [Documenting.md](../Documenting.md)
  - sections 'Developer documentation - Top-level', 'Developer documentation - Other' of [Documenting.auto.md](Documenting.auto.md)
- annotate(): defined in [Annotation.prose.md](prose/Annotation.prose.md).

## Chapter operators

The chapter pipeline has two source branches. Operator notation is a prose procedure, not an executable API.

~~~text
background + scope + source contracts -> Draft ----\
                                                   Extend -> Format
annotated source + scope             -> Collect --/
~~~

| Operator | Output | Template |
|---|---|---|
| [Draft](prose/Draft.prose.md) | close/draft/.../Topic.draft.md | [Draft template](prose/Draft.prose.temp.md) |
| [Collect](prose/Collect.prose.md) | close/collected/.../Topic.code.md | [Collect template](prose/Collect.prose.temp.md) |
| [Extend](prose/Extend.prose.md) | close/extended/.../Topic.ext.md | [Extend template](prose/Extend.prose.temp.md) |
| [Format](prose/Format.prose.md) | close/form/.../Topic.form.md | [Form template](prose/Form.prose.temp.md) |

Preserve the topic's relative path between phase directories. Collect can also produce independent root documents;
Extend accepts that set when no aggregate collected document exists. All four chapter processes are manually invoked.
Ignore numbered prompt/output variants unless explicitly requested.

## Rule ownership

Read the selected operator and its linked contracts/template before applying it. The following division is normative;
templates instantiate the contracts rather than supplying competing rules.

| Owner | Responsibility |
|---|---|
| [Definitions](prose/Definitions.prose.md) | Inventory, ownership, recursive section tree, numbering |
| [General style](prose/General.style.md) | Shared language and Markdown conventions |
| [Solution style](prose/Solution.style.md) | Problem/Solution narrative contract |
| [Implementation style](prose/Implementation.style.md) | Implementation narrative contract |
| [Notation](prose/Notation.md#chapter-profile) | Text coverage and chapter formula profile |
| Operator | Input/output boundaries and permitted transformations |
| Operator template | Output skeleton and substitution slots |
| [QA](prose/QA.prose.md) | Acceptance assertions and regression procedure |

Current user requirements take precedence. Source declarations own implemented names, signatures, fields, call bindings,
and outputs; background/plans explain intent and proposed behavior. Collected text owns Code prose and listings for
Extend; extended text owns all prose and Code for Format. An older chapter is evidence of useful shape, not authority
to omit current source or override a contract.

Resolve a defect at the earliest owning input when the task authorizes it. Otherwise report the invalid input rather
than silently repairing a different layer. Normal operator runs write their output only. An operator refactor should
change the owning rule and its QA assertion together, without appending the same exception to every file.

## Shape references

| Case | Existing reference | Expected distinction |
|---|---|---|
| Step main | close/extended/search/transforms/fields/Fields.ext.md | Methods and one step shape; no Result or Workflow |
| Internal workflow | close/extended/search/transforms/chunking/Chunking.ext.md | Child stages then Result; Code Workflow first |
| Mixed ownership | close/extended/search/transforms/similarity/lexical/Similarities.ext.md | Internal detail and external call items |
| No main | close/extended/search/transforms/offline/Offline.ext.md | Independent roots, nested stages/Results, root-local counters |

Corresponding draft, collected, and form outputs are under the matching phase directories. Consult them for successful
structure, not narrative to copy into another topic. Validate examples against current source and QA; legacy defects
and intentional contract changes must be reported during regression comparisons.

Refactor evidence and known verification limits: [RefactorVerification.md](prose/RefactorVerification.md).
