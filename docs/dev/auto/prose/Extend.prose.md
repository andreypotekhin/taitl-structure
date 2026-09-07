## Extend

### Extend process

Expand the draft using collected source and relevant background. Write the extended chapter under `close/extended/`,
preserving the topic's relative path. This manually invoked process produces the complete implementation account
and Code section that Format will preserve.

~~~text
Extend(topic, draft, collected, background, plan?) -> close/extended/<topic-path>/<Topic>.ext.md
~~~

### Extend operator

Develop the narrative and explain every owned stage and public method group. Read [Prose.md](../Prose.md), [Definitions.prose.md](Definitions.prose.md),
[General.style.md](General.style.md), [Solution.style.md](Solution.style.md), and
[Implementation.style.md](Implementation.style.md), then instantiate [Extend.prose.temp.md](Extend.prose.temp.md).

1. Resolve the inventory against collected and source contracts. Require complete class, method-group, stage, binding,
   and return-schema coverage before writing. Use background and relevant plans for explanations, not as claims that
   proposed behavior is already implemented.
2. Retain the Draft's H1 and sections through Stages. Preserve conceptual coverage while enriching Problem/Solution under
   their shared contract. Carry Design's relevant implementation constraints into Implementation; omit top-level Design
   and Notation. Do not shrink useful examples or important limits.
3. Write the substantive Implementation preamble, then evaluate the rendering algebra in Definitions:
   - Step: plain introduction, every public group with circled intent/explanation and complete text signatures, then one
     Resulting transform shape. Include all public methods in the shape; never substitute "same pattern" for named paths.
   - Composed: recursively render every child, then a Result subsection with a source-grounded sentence and complete
     composed notation. This applies equally to workflow, independent root, and internal composed stage.
   - External: one circled intent/description and one canonical boundary block; no methods or Result.
   Reset the circled stream per root, not per child. Do not add a duplicate boundary block before an internal step's groups.
4. Build Code directly from collected, independently of Implementation. Keep internal container descriptions plain; keep
   exact listing contents and group prose. Prefix each intent-led group or external-call item with its Code number,
   resetting per root. Each item is one paragraph immediately before its listing: short italic intent plus original
   explanation, without an additional unnumbered explanation. Preserve private helper groups here too.
5. Run [QA.prose.md](QA.prose.md)'s shared and Extend checks. Emit the chapter only.

Do not silently repair collected prose, regroup methods, or alter code downstream. Resolve defects in the earliest owning
input when authorized; otherwise report the invalid input. No normal Extend invocation changes its inputs.
