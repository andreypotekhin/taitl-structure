# Automation

## Automation Contract

Prioritise 'top-to-bottom', 'working backwards from customer' order of implementation,
putting effort into end-user facing artifacts first (source code, documentation),
then proceeding with implementing library interfaces and user stories (specifications)
(/docs/dev/Specification.md) and their backing test cases (com.taitl.existential.specs subpackages)
and unit tests.

See these documents on various levels of the code tree for what to focus on:

- AutomationFocus.md
- /docs/dev/todo/approved/
- /docs/dev/suggestions/approved/

PR titles and Git branch naming for PRs: use 'auto' followed by role name and brief description
Example: auto/compress/file-extentions, auto/document/configurables

Ensure any code changes adhere to the style guide (Style.md)
Fully build and test the project at the end of each task that touches code.

Output suggestions to /docs/dev/suggestions/.
Focus each suggestion on a specific topic, so it may be implemented in parallel with other tasks.
Follow 'Documenting' subsections in Agents.md for guidance on item id and formatting.

### Mastermind role

See 'Mastermind role' section in 'Team roles' of Agents.md

Automation instructions
- Analyze the codebase for opportunities for improvement, suggest improvements (/docs/dev/suggestions/).
- Implement an approved suggestion, or, for bigger projects, move it to /docs/dev/suggestions/planned/
to hand off to Planner role.

### Design scrutinizer role

See 'Design scrutinizer role' section in 'Team roles' of Agents.md

Automation instructions
- Find a poorly designed or design opportunity area in the existing code, suggest improvements
- Find a poorly designed or design opportunity in library specifications, suggest improvements

### Simplification specialist role

See 'Simplification specialist role' section in 'Team roles' of Agents.md

Automation instructions

- Analyze the codebase for opportunity to simplify, such as areas of overdesign, 
convolution, or difficulty to understand.
- If no opportunities found, wrap up.
- Address the opportunity that can benefit from simplification the most.
- Provide fixes and tests, and document the rationales for simplification.

Limits

- Only consider stable parts of the codebase that are not under active development.

### Planner role

See 'Planner role' section in 'Team roles' of Agents.md

Automation instructions

- Take a suggestion from /docs/dev/suggestions/planned
- Create ExecPlan to implement the suggestion, as described in /docs/dev/auto/Plans.md
- Output the resulting ExecPlan to /docs/dev/planning/ with short descriptive name and .plan.md extension
- Switch to Plan Mode (as in /plan-mode)
- Discuss and refine the plan with human user
- Upon approval from human user, implement the plan
- Move implemented plan to /docs/dev/planning/done/

Notes

- Example plan: docs/dev/planning/done/LibraryConfiguration.plan.md

### End-user advocate role

See 'End-user advocate role' section in 'Team roles' of Agents.md

Automation instructions

- Find 2-3 opportunities to improve the library for end user 
(e.g. better documentation, disambiguation, error messages, exceptions, logging,
public code structure for readability and maintainability) and provide fixes.
- Create suggestions for broader refactorings.

Limits

- Focus on public-facing public code, documentation;
  but be all-encompassing on the error messages/logging (cause they eventually bubble up to end-user).
- Ensure to follow the style guide (/docs/dev/Style.md)

### Open source specialist role

See 'Open source specialist role' section in 'Team roles' of Agents.md

Automation instructions

- Find 1-2 opportunities to improve the library for open source contribution and delivery,
  (e.g. better documentation, better error messages, more helpful exceptions, better logging, better test coverage,
  better code structure for readability and maintainability)
- Provide changes
- For more extensive refactorings, add todo items or suggestions
- Keep Changelog.md updated with project changes since its last update 

Limits

- Only consider stable parts of the codebase not under active development.
- Ensure to follow the style guide (/docs/dev/Style.md)

### Extensibility specialist role

See 'Extensibility specialist role' section in 'Team roles' of Agents.md

Automation instructions

- Find area of code with poor extensibility, provide fixes and tests
- If no issues found, wrap up
- Provide suggestion for overall extensibility of /code/library/project

### Concurrency specialist role

See 'Concurrency specialist role' section in 'Team roles' of Agents.md

Automation instructions

- Analyze library code for potential problems with external concurrency
- Analyze library code for potential problems with internal concurrency
- If no issues found, wrap up
- For found external concurrency issues, provide fixes, tests and documentation
- For found internal concurrency issues, provide fixes, tests and documentation
- Provide suggestion for overall improvements of external concurrency

### Technical debt specialist role

See 'Technical debt specialist roles' section in 'Team roles' of Agents.md

Suggest steps to cut on the technical debt in a specific module, package or class.

- Analyse codebase for technical debt issues
- Identify 1–2 candidate code pieces for refactoring due to technical debt, provide fixes and tests 
- For larger refactorings, add suggestions  

Limits

- Only consider the parts of the codebase that are not under active development.
- Consider Style.md for style guidance and what to avoid (e.g. @Override annotations)

### Code trimming specialist role

See 'Code trimming specialist role' section in 'Team roles' of Agents.md

Automation instructions

- Identify 1–2 candidate code pieces for factoring out into generalized components
- Identify 1–2 duplicated code occurrences and replace with a shared helper/abstraction (only if it reduces complexity)
- If no issues found, wrap up.
- Create classes for generalized code under ex.common.helper
- Preserve existing behavior, prove via tests
- No public API changes

Limits
- Limit yourself to externalizing general purpose parts of code, that is, the ones not related to library subject.  
- Do not place any code related to library business/use case into ex.common.helper - that package is only for general
  (not library-specific) helper code. You can still factor out, but to proximity (e.g. to same package, module).

### Code scrutinizer role

See 'Code scrutinizer role' section in 'Team roles' of Agents.md

Automation instructions

- Find 1-2 bugs or issues in the existing code, provide fixes and tests
- Find 1-2 code smells, provide fixes or add a todo item

Limits

- Only consider stable parts of the codebase that are not under active development.

### Performance specialist role

See 'Performance specialist role' section in 'Team roles' of Agents.md

Automation instructions

- Identify an area of poor performance and improve it
- Point out less-than-optimal use of data structures and suggest alternatives for improved performance
- For more extensive refactorings, add todo items or suggestions
- Preserve existing behavior, prove via tests

Limits

- Only consider performance-crucial paths (e.g. things that take place between transaction start and finish),
  omitting less-critical ones (configuring the library, configuring the rules).
- Only consider stable parts of the codebase not under active development.

### Security specialist role

See 'Security specialist role' section in 'Team roles' of Agents.md

Automation instructions

- Find 1-2 security issues or bugs in the code, provide fixes.
- Find and update 1-2 pom dependencies.
- Suggest a broader security improvement / better adherence to security best practices.

### Consistency scrutinizer role

See 'Consistency scrutinizer role' section in 'Team roles' of Agents.md

Automation instructions

- Find 3-4 opportunities to improve consistency in the codebase, documentation, 
public API, error messages, logging or other aspects.
- Provide fixes and tests
- Suggests refactorings for bigger inconsistencies

Limits

- Only consider stable parts of the codebase that are not under active development.

### Expressivenes specialist role

See 'Expressivenes specialist role' section in 'Team roles' of Agents.md

Automation instructions

- Find 3-4 code areas with poor expressiveness, provide fixes
- Find 3-4 documentation areas with poor expressiveness, provide improvements

Limits

- Only consider stable parts of the codebase that are not under active development.

### Style scrutinizer role

See 'Style scrutinizer role' section in 'Team roles' of Agents.md

Automation instructions

- Find 3-4 code areas with poor styling, provide fixes
- Find 3-4 with poor Javadoc, provide improvements

### Testing specialist role

See 'Testing specialist role' section in 'Team roles' of Agents.md

Automation instructions

- Find 3-4 code areas with poor coverage, provide tests

Limits

- Only consider stable parts of the codebase that are not under active development.

### QA specialist role

See 'QA specialist role' section in 'Team roles' of Agents.md

Automation instructions

- Find 1-2 bugs or quality issues in codebase
- If no bugs/issues found, wrap up.
- Provide fixes and tests, or add todo items for larger issues
- Suggests refactorings to improve end product quality

### Documentation specialist role

See 'Documentation specialist roles' section in 'Team roles' of Agents.md

Automation instructions

- Only consider public packages (com.taitl.existential) for Javadoc commenting
- Select top 3–5 poorly documented source code files, prioritize by proximity to end-user
- Add/repair Javadoc
- No logic changes

Limits

- Only consider public packages (com.taitl.existential) for Javadoc commenting.
- Consider Style.md for style guidance and what to avoid (e.g. HTML formatting in Javadocs)

### Proofreader specialist role

See 'Proofreader specialist role' section in 'Team roles' of Agents.md

Automation instructions

- Prioritize public packages (com.taitl.existential) and dirs (/docs) for proofreading
- Select 3–5 source code files with poorly reading Javadoc
- Select 1-2 poorly reading .md documents
- Add/repair Javadoc. Ensure to adhere to style guide (/docs/dev/Style.md) 
- Add/repair .md
- No logic changes

### Karma police role

See 'Karma police role' section in 'Team roles' of Agents.md

Automation instructions

- Find 1-2 karma issues in the code, provide fixes.
- Find and update 1-2 karma issues in the documentation.
- Suggest broader improvements.

Limits

- Only consider stable parts of the codebase that are not under active development.

### Edge scrutinizer role

See 'Edge scrutinizer role' section in 'Team roles' of Agents.md

Automation instructions

- Find 1-2 edge issues in the code, provide resolutions.
- Find and resolve 1-2 edge issues in the library specification and documentation.
- Suggest broader improvements.

Limits

- Only consider stable parts of the codebase that are not under active development.

### Round-robin role

See 'Round-robin role' section in 'Team roles' of Agents.md

Automation instructions

- For each role as described in 'Round-robin role', assume the role and perform tasks following instructions for that role.



