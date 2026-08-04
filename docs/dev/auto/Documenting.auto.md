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
Location: docs/ (top level)
- docs/background/

### End-User reference Documentation 
Location: 
- docs/reference/

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
- Changes propagate to public documentation (docs/), e.g. by synchronizing to documents in background/, reference/, recipes/    

### Changes introduced during development
Changes during development include task implementation, decisions made, resolved issues, pivots.
They propagate bottom to top: from code changes to outer documents. 
- Changes are formulated as tasks/followups/refactorings/requests during development.
- Changes introducing adjustments to business logic/external behavior should be reflected in the documents
- Recording ad-hoc changes in dev documentation: adjustments in decisions records, executive plans, design, specification documents.
- Synchronizing derived docs, such as annotated source in close/annotated/. 
- Synchronizing public documentation: documentation (docs/), e.g. by synchronizing to documents in background/, reference/, recipes/ 
- For the public docs, only synchronize the info changes that is relevant for end user. For dev docs, only the info relevant to developer. 

