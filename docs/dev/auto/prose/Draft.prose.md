## Draft

### Draft process

Create the chapter's first complete account from its background, intended scope, and current source contracts.
Write the draft under `close/draft/`, preserving the topic's relative path. This is a manually invoked process;
it does not update its inputs or generate the later chapter phases.

~~~text
Draft(topic, background, chapter_scope, source_contracts?) -> close/draft/<topic-path>/<Topic>.draft.md
~~~

### Draft operator

Author the chapter from general explanation to design and implementation. Read [Prose.md](../Prose.md), [Definitions.prose.md](Definitions.prose.md),
[General.style.md](General.style.md), [Solution.style.md](Solution.style.md), and
[Implementation.style.md](Implementation.style.md), then instantiate [Draft.prose.temp.md](Draft.prose.temp.md).

1. Establish the chapter inventory. Use the current background and intended chapter scope for narrative; consult source
   contracts for names, complete signatures, return schemas, and actual topology. Do not infer a workflow where none exists.
2. Author Intent, Problem, and Solution under the shared narrative contracts. Existing chapter outputs are shape references, not
   prose to copy; ignore numbered variants unless explicitly requested.
3. Fill the concise inventories: canonical principal topics in Builds on/Used by (empty when none), essential domain
   concepts in Definitions, schemas/relations in Inputs/Outputs, and the model's Stages inventory.
4. Write lossless text Notation covering every root, internal public method, stage call, input, and output. Include every
   named parallel path and concrete return schema; retain external boundary calls without importing external methods.
5. Write Design as requirements, invariants, decisions, and proposed behavior; write Implementation as a substantive
   continuous account of component responsibilities and data movement. Neither contains stage subsections or Result.
6. Set Code to the corresponding collected-document reference(s), not Python listings. Without an aggregate collected
   document, name the independent roots' collected files.
7. Run [QA.prose.md](QA.prose.md)'s shared and Draft checks. Emit only the chapter, not the inventory or QA report.

Preserve the template's section order. Draft alone has top-level Notation and Design. Do not modify inputs during a
normal run; report missing or inconsistent evidence instead of inventing signatures or behavior.
