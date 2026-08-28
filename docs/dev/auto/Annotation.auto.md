# Annotated code

Maintain annotated sources for certain parts of project code.

## Scope
Currently, we only maintain annotated code for these code dirs and their subdirs:
- examples/search/transforms/chunking
- examples/search/transforms/clicks
- examples/search/transforms/cohorts
- examples/search/transforms/features
- examples/search/transforms/filtering
- examples/search/transforms/inference
- examples/search/transforms/indexing
- examples/search/transforms/labeling
- examples/search/transforms/offline
- examples/search/transforms/online
- examples/search/transforms/relevance
- examples/search/transforms/scoring
- examples/search/transforms/vectorization
- examples/search/transforms/searching/search_docs
- examples/search/transforms/training
- examples/store/transforms/catalog
- examples/store/transforms/personalization
- examples/store/transforms/recommender
- examples/security/transforms
- src/structure/core/configuration
- src/structure/core/compiler
- src/structure/core/docs
- src/structure/core/plugins
- src/structure/core/runtime
- src/structure/core/sources
- src/structure/core/target

## Output
Destination: close/annotated/
- Example sources output without 'example': examples/search -> close/annotated/search 
- Main sources output without directly: src/structure/core/docs -> close/annotated/structure/core/docs 

## Creating annotated code
Maintain creation of annotated code documents for project source code:
- Annotated source documents are markdown (.anno.md) files describing the purpose and workings of single source unit (file)
- Ignore package and other dunder files (Ex: __init__.md)
- Annotated source document describes purpose and workings of a source unit, shows source code section, and then lists relevant points with explanations
- Multiple annotated source documents read as continuous story/narrative, top to bottom, focused on business details such as purpose, parameter values, formulas.  
- Annotated source dir structure and naming follows the original code dir structure and naming

Example: 
Original Source:
```
examples/
  search/
    schemas/
    transforms/
      searching/
        search_docs/
          admit.py 
          overlap.py
          rerank.py
          workflow.py          
  ...
```
Annotated source:
```
close/annotated/
  search/
    transforms/
      searching/
        search_docs/
          admit.anno.md
          overlap.anno.md
          rerank.anno.md
          workflow.anno.md
```
- Of example code, we only currently target transform code. Schema definitions are not listed, just named in transform code.
- Annotated source document structure for a Transform class: 
 - Heading (follows class name), Sections: Intro (no heading), Inputs (no heading), Step sections (follow step method names)

Example:
admit.anno.md contents follow the code of admit.py:
Heading: ## Retrieve Documents - We start at Heading 2 by convention (no Heading 1)
'Intro (no heading)': Description of transform purpose, with focus on business logic/purpose/goal
- Grounded in outer transform logic, if any, or in same-level workflow sequence.
- Prefer single paragraph. Prefer action-ariented vs state-descripted style. 
  - Instead of: 'The ... transform turns browsing events and fulfilled purchases into one interaction strength relation.'
  - Try this: 'Unify browsing events and purchases into a single interaction history relation.' 
- Avoid premature diving into implementation details far from actual code.  
 - e.g. explaining implementation details before inputs, 
 - mentioning subsequent steps before they are reached by the reader. 
- Purpose precedes code. Explanation follows code.
- Avoid overqualifying the nouns (e.g. 'nullable experiment' - prefer simply 'experiment').
- Avoid explaining what's obvious, e.g. specific lanes purpose.
- Tailored for sequential reader, who goes over the docs top-to-bottom. 
 - This implies no need to repeat what is established/explained in previous .anno.md docs on same level.
- Avoid sophisticated/too detailed language - stick to overview style; assume the reader will 
glance at the listed code and use subsequent bullets for focusing. Also assume the reader does
not read headings - heading text or its idea must be present in body text. 
- Do not split class/method names with spaces when mentioning in body text.
(Let's omit 'Inputs' section for now) 'Inputs (no heading)' - Text description of the essential inputs/outputs
- If listing transform inputs as is, include the full beginning of transform class definition code lines (including 'class', decorations, parameters)
'### Merge stored scores' - Section name following the transform step
- Text description of step purpose (see 'Intro' instructions for style)
- Code listing of step method
- (Optional) Bullet or numbered list of essential/relevant for understanding points with brief explanation
- Numbered list can be preceded with number markings (①) and place them in the referred lines in the code listing.
- Keep list sentences condensed and concise - just enough to focus user attention on a code line.
- If no need for a list, generally forgo text under code listing, unless there is something important to explain.
- Do not explain the obvious.
'### Merge streamed scores' - Section name following next transform step
- Section texts are grounded in step sequence of transform's logic.
- Step sections share context and read sequentially.  
- Don't be shy to use a variety of 'connecting' phrases, where justified, such as 'now that we ..., ...'
- But, refrain from serial repetition such as 'Then,...Then,...'
- The annotated source document should read as continuous piece.
- The annotated source documents under same dir read as continuous piece (top to bottom).
'### Private methods' - Section for private methods

General tips:
- Value brevity, avoid self-evident explanations/callouts. When in doubt, skip/ignore. Be succinct.
- Treat the text as a frame and code listing as essence - the text should highlight, not overshadow the code.
- Avoid using concepts that may be unclear (e.g. 'stored' in 'Join stored scores'),
without explaining (e.g. '(from cache batch)' when used first time).
- Avoid tautology (e.g. 'select streamed candidates from streamed documents')
- Avoid trying to say too much in one sentence. Ex:
 - Instead of: 'The `Browse` step keeps product events and sums their interaction weights by tenant, customer, session, product, and category.'
 - Try this: 'The `Browse` step summarizes interaction weights by tenant, customer, session, product, and category.'
 - Instead of: 'The `Score` step joins eligible products to preferences and matching history, then records each personal component and
the algorithm version used to calculate it.'
- Try this: 'The `Score` step joins products to preferences and matching history.'
- Do not explain the obvious.

### Quality assurance

Before considering an annotated source document complete, verify the following:
- For a transform with multiple step methods, split the source into one-step sections or coherent step-group
  sections. Never place the complete method inventory in one code listing merely because the methods share a class.
- Put a short, business-focused explanation immediately before each step or step-group code listing. Group repetitive
  grain variants only when the group has one clear purpose; mention the meaningful difference between the variants.
- Keep the source order and show every step exactly once. Do not omit a step because it is repetitive, and do not
  duplicate a step in both a group and a later detail section.
- Put the transform's essential inputs, lanes, and outputs in the initial class listing when they help the reader
  follow the flow. Do not create a separate `Inputs and outputs` section for them.
- Do not add generic framing sections such as `Reading the unit`, or boilerplate such as 'The file defines ... Its
  important executable boundaries are ...'. The introduction and step-group prose must explain the actual transform.
- Before delivery, compare the headings and code blocks with the source file and confirm that each non-trivial block
  has an explanation that tells the reader why the code matters.

### Non-example app annotated source 
Non-example app annotated source, e.g. annotated source for core packages, 
follows the above instructions for example code, with following adjustments:
- For top-level packages under core/, plugin/, plugin/pyspark, create Package document: package.anno.md
- In the package document, describe the purpose of the package and overall flow.  
- Package dirs, modules and methods do not typically follow 'top-to-bottom' sequence inherent to transforms.
Therefore, it may be more difficult to create a continuous narrative for top-to-bottom reader.
Instead, the responsibility of providing structure and clarity for the reader belongs to the package document.
There is still some top-to-bottom opportunity, since our api-commands-logic are alpha ordered, general-to-detail sequence. 
So the expectation is the reader is still following top-to-bottom, but because it stops at the subpackage, readers need
project doc for backtracking to the correct path.
- Can incorporate Readme.md into project doc, if it exists.

Tips:
- Drop 'package', 'flow' from package doc headings
