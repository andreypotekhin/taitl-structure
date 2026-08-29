# Collect operator

## Shared Prose context
This chapter operator is governed by the common concepts and conventions in [Prose.md](../Prose.md). Read its
[Definitions](../Prose.md#definitions), text-process model, and shared authoring guidance before applying this file.

## Collection
Present annotated source as continuous narrative.

### Collect process
- Input: close/annotated
- Output: close/collected
- Scope: close/annotated/search
- Name: Collect. Usage: Collect(dir)
- Invocation: manual

### Collect operator
- Name: collect(), usage: collect(dir)
- Input: .anno.md describing a transform. Ex: rerank.anno.md
- Output: .cnd.md describing a transform in collectd form. Ex: rerank.cond.md
- Goal: present annotated source as continuous narrative.
- Instructions
  - Top header: convert to gerund, optionally extend to fuller phrase.
    - Ex: 'Rerank Documents' becomes 'Reranking the Documents'
  - Low-level headers: convert every step or helper section heading to an italicized intent-setting sentence at the beginning of its section paragraph.
    This includes sections whose listing contains `@step`, `@special`, or `@raw`; retain workflow and stage container headings.
    - Ex: 'Select fallback options' heading converted to italicized 'Select fallback options. ' added in front of section paragraph.
  - Avoid merging code listings - maintain a text sentence in between.
  - Drop import statements.
  - Workflow transform is the main transform in a package - a composed transform that rules other transforms in the package.
    Usually, the workflow transform is alphabetically the last file in dir (Ex: 'SearchDocuments'). Not all dirs contain the workflow transform.
    - Top header: as-is, do not convert to gerund
    - Replace subsections as described above
    - Move workflow class listing from intro section into a new section, 'Workflow'
    - The `Workflow` section thus contains the parent workflow class listing plus all former sections, except the intro
      section. It must retain every stage call in execution order, including calls to stages defined outside the workflow
      directory, so the complete parent orchestration is readable in one place.
    - Append workflow document with the content (.cnd.md) of stage transforms as additional sections to form continuous narrative.
      - Order stage transform sections according to their stage's order in the workflow transform.
      - Convert the headers of the appended content to one level lower, to maintain header structure.
      - Do not remove any headers: former top-level headers become section headers.
    - Treat the workflow directory as a package tree: inspect sibling source files and subpackages,
      - For internal stages, include the annotated source for the stage transform. If one source file defines
        multiple transform classes, include each class separately in workflow order.
    - Some stage transforms may be defined outside of workflow dir.
      - Internal stages: include them into main workflow doc as stage transforms, as described above.
      - External stages (stages defined outside the workflow dir):
        - Include a section heading and brief description, ", as described in 'doc_header'".
        - Keep the full stage call in the parent `Workflow` section and include the complete parameterized stage call in
          the external stage section as well, so the external reference is self-contained.
        - Describe the external stage in prose without repeating the assignment inline; the fenced listing is the single
          external reference for that call.
        - The intentional duplication is limited to stage assignment; never duplicate parent workflow class
          listing or a child transform narrative in external stage section.
        - For example, if the workflow class contains `features = Features(...)`, the external stage section contains the
          complete `features = Features(...)` assignment, not a second `class Training(Transform):` listing.
    - Quality assurance:
      - Verify transform sections follow the order of main transform stages, with no stages missing.
      - Verify the package stage inventory is complete: every same-package stage class from the workflow has a corresponding section in the collected output.
      - Verify every internal stage section contains its complete class declaration and all annotated step code,
        including files that define more than one transform class.
      - Verify the complete parent orchestration, including every external stage call, appears in `Workflow` in execution order.
      - Verify every external stage section contains the complete parameterized stage call and that the parent Workflow
        also contains the call in execution order.
      - Verify parent workflow class listings and child transform narratives are not duplicated as external listings.
      - Verify each output has exactly one top-level header, and that its first nonblank line is that header.
      - Verify each workflow output has exactly one parent `Workflow` section before inserted child sections.
      - For transform steps, verify code listing is present in each transform step section.
      - Enumerate every step or helper section heading below each transform or stage heading, including sections containing
        `@step`, `@special`, or `@raw` listings. Verify that each is represented exactly once in the collected output,
        in source order, with the heading removed and its exact title converted to an italicized intent sentence at the
        beginning of the section paragraph. A helper section such as `Default sentence spans` must not be treated as
        unheaded prose merely because it contains `@special` instead of `@step`.
    - Since, as result of content inclusion, stage sections carry more detail, the texts between code listings in the
    'Workflow' section may become redundant/duplicating.
      - Trim texts between code listings in the 'Workflow' to avoid repetition/duplication.
