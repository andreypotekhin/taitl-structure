## Documenting

Also: Documenting section in [Style.md](Style.md)

## Documenting
### End-User documentation
The end-user documentation consists of [Readme.md](../../Readme.md), [Troubleshooting.md](../../Troubleshooting.md), and the `/docs/` directory.
It is characterized as being concise and all-encompassing, clearly conveying the meaning,
being complete without overwhelming the reader.
Content style:
- [Readme.md](../../Readme.md) is the main entry point for end-users, and is more formal compared to other documents.
- [Troubleshooting.md](../../Troubleshooting.md) tracks common issues and remedies, prioritizes conciseness and clarity.
- Other documents: prioritize focus and practicality.
- Public reference pages in `/docs/reference/` are end-user references, not implementation specifications. Do not keep
  `## Purpose` as the first section heading; put an end-user-oriented introduction directly under the H1 instead.
  Refer to these pages as "reference" pages, not "specifications". Drop implementation details, acceptance criteria,
  test placement, internal checklists, and lowering explanations when the lowering is the same or similar to a PySpark
  concept.

Help a new user to get familiar with the library by making it easy to absorb content for first-time reader. 
- Avoid, if possible, refering to concepts which haven't been introduced, or include a link or a brief definition; 
- Prioritize content from more general/common path to less general/less common use cases.
- Do not overspecify nouns: 
 - Wrong -> Right: 
   - adds deterministic intent labels -> adds intent labels 
   - containing each stable English label name -> containing each English label name
- Be mindful of use of adjectives/adverbs of degree. Remember that these can affect reader's trust:
  - Wrong -> Right: can become tightly coupled -> can become coupled
- Avoid overusing 'online'/'offline' esp. outside online/offline discussions/comparisons.

Typical reading order for first-time reader:
- [Readme.md](../../Readme.md) is the main entry point for end-users
- [QuickRef.md](../QuickRef.md)
- Reference pages (`/docs/reference/`)
- Other docs in `/docs/`.

### Developer documentation
The developer documentation is in the /docs/dev/ directory.
It is characterized as being detailed and comprehensive, rich on technical details,
and focused on the development process and codebase.
Content style:

- Less formal compared to end-user documentation, err on the side of expressiveness and sounding less bureaucratic/official.

### Documenting issues and remedies
Document issues and remedies (fixes) in Troubleshooting.md documents.
Separate end-user troubleshooting items ([Troubleshooting.md](../../Troubleshooting.md)) from development
troubleshooting items ([Troubleshooting.md](Troubleshooting.md)).

### Documenting the design decisions
Add decision items as [action id].[action-title].md file to close/archive/decisions/.
See below sections (Action id, Action format) for namign adn formatting.

### Documenting the suggestions
As you assume team roles as described in 'Team roles' section below, come up with suggestions for improvements.
Add suggestion items as [action id].[action-title].md file to docs/dev/suggestions/.
Suggestions are reviewed by the manager and mastermind role.
The approved suggestions get moved to docs/dev/suggestions/approved.
Implemented suggestions get moved to close/archive/suggestions.

### Documenting issues
Record reproducible code issues in [issues/](issues/Readme.md). Issue records are concise, structured inputs for
maintainers and automation. They capture the runnable report, the observed and expected results, and the pull request
that supplies the fix. Move resolved records to `issues/done/`; do not create a record for an incomplete report.

### Documentation formatting
Because we often read documentation as plain-text Markdown, we want it to look good in plain text editor.
In particular, we maintain line limit of 120 characters per line.

### Documentation hygiene
Do not include sensitive information such as passwords, host names, absolute paths, developer username/home dir
in any documents. Timezone should be specified as abbreviation, rather than city-name-based.

### Documentation pipeline

#### Changes introduced during planning/design
Changes during planning and design phases propagate top to bottom
- Changes are formulated as tasks such as requests for creation of design, documentation.
- Outputs are design documents, specifications, project-management docs, execution plans.
- Changes propagate to other developer documentation (docs/dev/), e.g. as updates to sprints.  
- Changes propagate to public documentation (docs/), e.g. by synchronizing to documents in background/, reference/, recipes/    

#### Changes introduced during development
Changes during development include task implementation, decisions made, resolved issues, pivots.
They propagate bottom to top: from code changes to outer documents. 
- Changes are formulated as tasks/followups/refactorings/requests during development.
- Changes introducing adjustments to business logic/external behavior should be reflected in the documents
- Recording ad-hoc changes in dev documentation: adjustments in decisions records, executive plans, design, specification documents.
- Synchronizing public documentation: documentation (docs/), e.g. by synchronizing to documents in background/, reference/, recipes/ 
- For the public docs, only synchronize the info changes that is relevant for end user. For dev docs, only the info relevant to developer. 


