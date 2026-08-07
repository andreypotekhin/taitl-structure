# Automation Documenting

## References
### Documenting - Main
- Main: [Documenting.md](../Documenting.md)
- 'Documenting' section in [Style.md](../Style.md)

### Authoring automation
- Main: [Authoring.auto.md](../Authoring.auto.md)
- Definitions of text operators and processes. 

### Annotated source code
- Main: [Annotation.auto.md](../Annotation.auto.md)
- Maintain annotated sources per instructions and scope described in main doc. 

## Documentation style

### End-User top-level documentation 
Location: 
- docs/ (top level)

- Top level documents are tailored for the readers when they first encounter the system.
- Top level documents are optimized for top-to-bottom reading, with concepts introduced
before used, and refrain from referring things that are described down the document
(or give general idea/mention that it will be described further).

### End-User reference documentation 
Location: 
- docs/api/
- docs/reference/
- docs/background/
- docs/recipes/

- The reader's journey is typically from top-level documents to reference/, then to background/ or api/ docs.
- Reference documentation answers: “What can I declare or use?” It is the practical operation inventory for an
  end-user. It owns public operations, signatures, options, user-visible rules, and corrective examples. It should be
  organized for lookup while remaining readable top to bottom. Reference documents normally include a code example
  for each described topic, to help the reader build understanding.
- Background documents are long-reads thoroughly covering the topic. They are superbly organized/structured
for top-to-bottom reader. Background documents use design, specifications, architectural decisions
and code as source of truth. 
- Each background document focuses on a single topic. While new design/specifications/decisions are often added to the
  same topic, the set of background documents stays relatively stable.
- Background docs are more formal than top-level. Besides reference material, they include design, architecture, system
  behavior, and rationale based on docs/dev/design/ and docs/dev/specifications/.
- Readers of background docs are trying to dig deeper into a topic. They already have some context, but it may contain
  misunderstandings, so background documents must introduce the context first. They are past the scanning phase and are
  comfortable with specifics.
- Recipes documents are for hands-on users who are trying to solve an issue or apply the library to a
  specific problem or use case. They can be 'landing pages' where the user comes first, e.g. with web search,
  looking for a template solution.
- Apply annotated source authoring style to Recipe documents. The intro sections are around problem/solution,
with one centence for each. Include scenario table and details, if needed, as in recipes/EarliestRows.md.

#### End-User documentation tips 
- Use caution/consider alternative wording on corporate speak such as 'owns', 'boundary', 'intentional'
'downstream', qualifiers like 'complete' (when not the focus)
- Avoid formalisms like 'the following'. 
- In Reference documents, 
  - ensure concept is defined/purpose is explained before showing code snippet.
  - avoid folding several statements into a quick paragraph. Consider bullet lists and examples.
  - avoid overloading a sentence with several statements.
- In Recipe documents, avoid excess use of word 'recipe' (reader already realizes that).
- Code examples use wildcard imports, to save lines.
- Do not assume example apps code in known/can be referred. 
  - Ex: 'Keep tenant and other scope keys in the predicate.' - tenant is part of example app, not part of the project.  

### Developer documentation - Top-level
Location: docs/dev/ (top level)
Follow instructions in Documenting.md and Style.md

### Developer documentation - Other
Location: docs/dev/ subdirs
Follow instructions in Documenting.md and Style.md

## Documentation pipeline

### Changes introduced during planning/design
Changes during planning and design phases propagate top to bottom
- Changes are formulated as tasks such as requests for creation of design, documentation.
- Outputs are design documents, specifications, project-management docs, execution plans.
- Changes propagate to other developer documentation (docs/dev/), e.g. as updates to sprints.
- Changes propagate to public documentation under `docs/`, including background/, reference/, and recipes/

### Changes introduced during development
Changes during development include task implementation, decisions made, resolved issues, pivots.
They propagate bottom to top: from code changes to outer documents.
- Changes are formulated as tasks/followups/refactorings/requests during development.
- Changes introducing adjustments to business logic/external behavior should be reflected in the documents
- Recording ad-hoc changes in dev documentation: adjustments in decisions records, executive plans, design, and
  specification documents.
- Synchronizing derived docs, such as annotated source in close/annotated/.
- Synchronizing public documentation under `docs/`, including documents in background/, reference/, and recipes/.
- For public docs, synchronize only information relevant to end users; for dev docs, synchronize only information
  relevant to developers.

### Notes for synchronizing
- Avoid placing new content at top or at random place of the document. Think of best place for the top-to-bottom reader,
who is trying to build understanding.
- In code examples, use wildcard imports to save lines.
