# Prose automation

## Reference  
Documenting Automation: [Documenting.auto.md](Documenting.auto.md)
Documenting: [Documenting.md](../Documenting.md)
Source Annotation: [Annotation.auto.md](Annotation.auto.md).

## Text processes
Text process is the process of creating documentation content from existing code and texts.
It can be automated (e.g. triggered by code changes on task completion), or manual (triggered with a task). 

Text process may be thought of as automated or semi-automated content pipeline.
Example: source annotation process is defined in [Annotation.auto.md](Annotation.auto.md). It is an automated process.

Text process definition includes the following:
- Establishes the scope, inputs and outputs (dirs). 
- Defines synchronization policy: e.g. source annotation automatically synchs on underlying code changes.
- Instructions on content transformation
- Points on authoring style/character
- Tips, notes, exceptions.

The process notation resembles a callable Python class, but is intended for communicating, not execution. 
Ex: Annotate(dir_name) invocation in an automation task. 

### Existing text processes
- Annotation, defined in [Annotation.auto.md](Annotation.auto.md).
- Documentation, defined in 'Documentation pipeline' section of [Documenting.md](../Documenting.md) 
and [Documenting.auto.md](Documenting.auto.md)

## Text operators
Text operator is a code-to-text or text-to-text transformation bearing certain authoring style,
e.g. optimized for brevity, generality etc.

Example:
- Annotation.auto.md defines annotate() text operator in subsections 'Example', 'General tips' of 'Creating annotated code' section. 
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
- annotate(): defined in subsections 'Example', 'General tips' of 'Creating annotated code' section of [Annotation.auto.md](Annotation.auto.md). 

More operators and processes are defined below. 

## Collection

### Collect process
- Input: close/annotated
- Output: close/collected
- Scope: close/annotated/search
- Name: Collect. Usage: Collect(dir)
- Invocation: manual

### Collect operator
- Name: collect(), usage: collect(dir)
- Input: .py.md describing a transform. Ex: rerank.py.md
- Output: .cnd.md describing a transform in collectd form. Ex: rerank.cond.md
- Goal: present annotated source as continuous narrative.
- Instructions
  - Top header: convert to gerund, optionally extend to fuller phrase.
    - Ex: 'Rerank Documents' becomes 'Reranking the Documents'
  - Low-level headers: drop low-level headers. If that affects clarity, repeat the header as part of intro section paragraph.
    - Ex: 'Select fallback options' dropped, 'select fallback options' embedded in section paragraph.
  - Avoid merging code listings, maintain a text sentence in between.
  - Workflow transform is main transform in a package - a staged transform that rules other transforms in the package.   
  Usually, alphabetically the last file in dir (Ex: 'SearchDocuments'). Not all dirs contain the workflow transform. 
    - Top header: as-is, do not convert to gerund
    - Replace subsections as described above
    - Move workflow class listing from intro section into a new section, 'Workflow'
    - The `Workflow` section thus contains the parent workflow class listing plus all former sections, except the intro
      section. It must retain every stage call in execution order, including calls to stages defined outside the workflow
      directory, so the complete parent orchestration is readable in one place.
      - Since 'Workflow' section now starts with bare code listing, include a sentence before the listing describing that
      this is the workflow transform. 
    - Append workflow document with the content (.cnd.md) of subtransforms as additional sections to form continuous narrative.
      - Convert the headers of appended content to one step lower, to maintain header structure.
      - Do not remove any headers: former top-level headers become section headers. 
    - Some stages may be outside of workflow dir.
      - For stages in the immediate subdirs of workflow dir: include them into main workflow doc similar subtransforms, as described above.
      - For stages defined in different dirs outside of workflow dir:
        - Include a section heading and brief description, ", as described in 'doc_header'".
        - Keep the full stage call in the parent `Workflow` section and include the complete parameterized stage call in
          the external-stage section as well, so the external reference is self-contained.
        - Describe the external stage in prose without repeating the assignment inline; the fenced listing is the single
          external reference for that call.
        - The intentional duplication is limited to the stage assignment; never duplicate the parent workflow class
          listing or a child transform narrative in the external-stage section.
        - For example, if the workflow class contains `features = Features(...)`, the external-stage section contains
          the complete `features = Features(...)` assignment, not a second `class Training(Transform):` listing.
    - Quality assurance:
      - Verify transform sections follow the order of main transform stages, with no stages missing.
      - Verify the complete parent orchestration, including every external stage call, appears in `Workflow` in
        execution order.
      - Verify every external stage section contains the complete parameterized stage call and that the parent Workflow
        also contains the call in execution order.
      - Verify parent workflow class listings and child transform narratives are not duplicated as external listings.
      - Verify each output has exactly one top-level header, and that its first nonblank line is that header.
      - Verify each workflow output has exactly one parent `Workflow` section before inserted child sections. 
    - Thus, the texts between code listings in 'Workflow' section may become redundant, since the stages describe them in detail in the included content.
      - Trim texts between code listings in 'Workflow' to avoid repetition.
