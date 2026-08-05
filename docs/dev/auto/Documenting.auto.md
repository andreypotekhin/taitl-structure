# Automation Documenting

# Documenting - Main
- Main: [Documenting.md](../Documenting.md)
- 'Documenting' section in [Style.md](../Style.md)

# Documenting additions for automation

## Annotated code
- Main: [CodeAnnotation.auto.md](../CodeAnnotation.auto.md)
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
- Reference documentation is more formal than the top-level. It attempts to exhaustively cover one topic.
It may leave some details to background documents. Reference documents are superbly organized
for top-to-bottom reading, for the reader who is trying to get familiar with the topic. We should try best
to help them in that.
- Background documents are long-reads thoroughly covering the topic. They are superbly organized/structured
for top-to-bottom reader. Background documents use design, specifications, architectural decisions
and code as source of truth. 
- Each background document focuses on a single topic. While new design/specifications/decisions are often added
to same topic, the set of background documents stay relatively stable.  
- Background docs are more formal than top-level. They are detailed docs which besides reference material include 
design, architecture, system behavior and rationales behind it, based on the docs/dev/design/ and docs/dev/specifications/.
- The reader of background/ docs reader is trying to dig deeper into a topic. 
 - He/she already has some context, but that may contain misunderstandings, so background/ documents must introduce the reader to context first.
 - The reader is past 'scanning' phase of reading as in top-level docs, and should be comfortable with specifics/details. 
- Recipes documents are for hands-on users who are trying to solve an issue or apply the library to a 
specific problem or use case. They can be 'landing pages' where the user comes first, e.g. with web search,
looking for a template solution.

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
- Changes propagate to public documentation (docs/), e.g. by synchronizing to background/, reference/, recipes/    

### Changes introduced during development
Changes during development include task implementation, decisions made, resolved issues, pivots.
They propagate bottom to top: from code changes to outer documents. 
- Changes are formulated as tasks/followups/refactorings/requests during development.
- Changes introducing adjustments to business logic/external behavior should be reflected in the documents
- Recording ad-hoc changes in dev documentation: adjustments in decisions records, executive plans, design, specification documents.
- Synchronizing derived docs, such as annotated source in close/annotated/. 
- Synchronizing public documentation: documentation (docs/), e.g. by synchronizing to documents in background/, reference/, recipes/ 
- For the public docs, only synchronize the info changes that is relevant for end user. For dev docs, only the info relevant to developer. 

### Notes for synchronizing
- Avoid placing new content at top or at random place of the document. Think of best place for the top-to-bottom reader,
who is trying to build understanding.
- In code examples, use wildcard imports to save lines. 