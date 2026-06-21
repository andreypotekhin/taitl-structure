# Style Guide

## Priorities
Coding priorities:
- Convenience of human code reader
- Convenience of human end-user
- Performance where it matters

## Coding

### Coding Style
General rules:
- Follow the surrounding project's style on coding, decomposition, documentation, etc.
- Refactor with minimal code
- Move general (non-specific to project business logic/reusable) code into separate components, 
e.g. by adding to structure.common.helper 
- Less code, less bugs

#### Naming
##### Naming - Identifiers
Avoid abbreviations in identifiers, with an exception for well-known and widely accepted
ones when used in compound identifiers, such as 'Doc' for document. 
Do not use vowel dropping, and also limit the use of numbered identifiers.
Avoid abbreviations in single-word identifiers.
In math-like contexts, e.g. around looping, use single-character identifiers for brevity.

##### Naming - Classes
Use action-oriented name (verb+noun) for logic classes focused on a single action (BuildConfigs instead of ConfigBuilder)

##### Naming - Spank
Prefer 'spankier' names, when applicable, for variables/fields: shorter names that immediately convey purpose.  
Example: In class Tr, the field Set<Transaction> 'transactionSet' is renamed to 'already'.
Rationale: it is 'spankier' than the old one since immediately reflects the rationale, 
and does that with a single word.

##### Naming - Trivialization
For single-field (or near-single-field) classes, it is ok to use a single word trivial name
that matches class purpose. 
Ex: In classes EventKey, RuntimeKey, the essential field is named simply 'key'. 

##### Naming - Loops
Prefer single-word identifiers for the 'for' loop variables.
Prefer single-character identifiers for loop counters and other math-like variables.

##### Naming - Abbreviations
In compound identifiers, do not convert all-capital abbreviations (HTML) to camel-case (Html).

#### Comments
Docstrings
- Avoid HTML formatting in docstrings, but do use triple backticks for multiline code examples. 
- In user-facing code packages, use docstrings comments with Args/Returns/Raises 
- Have docstrings on non-trivial methods
- In implementation (non end-user-facing) packages, avoid Args/Returns/Raises
- In implementation code packages, docstrings are more free-form, used to explain the rationale,
not required on class level, used mostly on essential or non-trivial methods.

We discourage non-docstrings comments: the meaning should stem from code itself.
Example: instead of creating a comment on a method call, we can
create more context by extracting the method into a well-named method or lightweight component 

#### Code Formatting
Code formatting is taken care of automatic build step (with build plugin).
Some parts of code, such as builder chained method calls, tend to be a challenge for automatic fomatter.
We normally surround such sections with # fmt: off / # fmt: off comments.


### OOP
#### Instantiation
- We generally prefer Builder pattern for multi-field classes or where readability is crucial

#### Inheritance
- Avoid deep inheritance chains
- Consider composition over inheritance 

#### Object decomposition
The goal of our way of object decomposition is to maximize 'graspability' - the ability for a reader to quickly understand code logic.
- We divide the classes into 'public', 'orchestration' and 'logic' classes
- The 'public' classes are the end-user facing classes from our 'public' packages (com.taitl.structure)
- The 'orchestration' classes are top-level classes to which the public classes delegate. Example: ConfigBuilder 
- The 'logic' classes implement business logic. Example: BuildConfigs
- The logic classes are characterized by
  - Action-oriented name (verb+noun) (BuildConfigs instead of ConfigBuilder)
  - Focus on a single task
  - Limited or absent state: most methods can be thought of as 'static', even if they formally aren't,
  and the context is normally passed in by method parameters.
  - Belong to 'logic' packages (com.taitl.structure.logic)
  - Rich with implementation details
- The upper-level 'logic' packages are thought to be similar to semi-autonomous 'apps':
  - Define own subpackage. Example: com.taitl.structure.logic.configuration, com.taitl.structure.logic.validation
  - Implement business logic as near-stateless 'actions' (actions subpackage), mappings (maps subpackage),
    data model (data subpackage), business rules (rules subpackage), outputs (output subpackage)
  - Integrate with other 'apps' using their corresponding data model

### Code structure
Main: /docs/Code.md

### Testing
Main: /docs/Testing.md

#### Testing Guidelines
Some rules around testing we adopt:
- It is ok to test protected and private methods
- It is ok to make adjustments to classes to facilitate testability

#### Test Structure
Use modern test frameworks capabilities to the maximum for structuring the tests:
- Use nesting of test cases liberally
- Take advantage of the shared initialization of nested tests
- When nesting, make enclosing test class names descriptive so that child test class/method names could be shortened
- Shorten test method names by creating context with nesting, and using docstring with human description.
- Use user story/specification item text as docstring for nesting test case
- Use docstring if test method name is a multi-word identifier
- Use test parameterization and other advanced techniques to cut down on test code size

#### Test coverage and isolation
- Try to achieve significant (89%) coverage, but do not insist on coverage of units which are in active development
- Test by cohesive sets of units (e.g. class+immediate dependencies) rather than testing each class in isolation
- The above means our unit tests are often also end-to-end tests (that's ok)
- We include all tests (including integration ones) when measuring test coverage 
- Regression tests refer to issue number in test method name and issue title in docstring

#### Feature Testing
See /docs/Testing.md

Directory structure:
- /tests/app/[app]/[subapp]/package/subpackage - tests for app implementation code, mirroring the app package path
- /tests/lib/[lib-name] - tests specific lib

Examples:
- /tests/app/cli/ - CLI app tests
- /tests/app/configuration/ - Configuration app tests
- /tests/lib/helper/ - Helper lib tests

#### Specification Testing
Test cases backing specifications (from /docs/dev/Specification.md) are in tests/specs/.
For each implemented user story from Specification.md, create a test case 
in the corresponding subpackage of tests/specs.

Tests for specifications/ documents, if needed, go to tests/specifications/[specification-doc-slug]

Directory structure:
- /tests/specs/ - tests for Specification.md items user stories
- /tests/specs/[section]/ - tests for specific Specification.md section
- /tests/specifications/[specification-doc-slug]/ - tests for specifications/ documents

#### Concepts Testing
We maintain a list of concepts in /docs/Concepts.md that we want to be covered with tests.
Concept tests are end-to-end tests proving correctness around a specific concept, like 'join'  

Directory structure:
- /tests/concepts/ - tests for concepts in project vocabulary (Concepts.md)
- /tests/concepts/[concept]/ tests for the concepts that have subcontcepts 
(e.g. join with subconcepts left_join, join_one)

While thorough coverage of concepts not required initially, we expect 100% coverage 
towards the end of project.

#### Model Source Code for testing 
Model source code is example Structure-oriented source code that can serve
as input to test cases, as well as pre-composed 'generated/' outputs featuring 
expected results of code generation/compiler output.

Directory structure: 
- Model source: /res/testing/model/v0/orders/, /res/testing/model/v1/orders/, /res/testing/model/v2/orders/
- Model generated source: /res/testing/model/v0/structure_generated/, /res/testing/model/v1/structure_generated/, /res/testing/model/v2/structure_generated/

## Documenting
### End-User documentation
The end-user documentation consists of /Readme.md, /Troubleshooting.md, and /docs/ directory.
It is characterized as being concise and all-encompassing, clearly conveying the meaning, 
being complete without overwhelming the reader.
Content style:
- /Readme.md is the main entry point for end-users, a is more formal compared to other documents.
- /Troubleshooting.md tracks common issues and remedies, prioritizes conciseness and clarity.
- Other documents: prioritize focus and practicality. 

### Developer documentation
The developer documentation is in the /docs/dev/ directory.
It is characterized as being detailed and comprehensive, rich on technical details, 
and focused on the development process and codebase.
Content style:

- Less formal compared to end-user documentation, err on the side of expressiveness and sounding less bureaucratic/official. 

### Documenting issues and remedies
Document issues and remedies (fixes) in Troubleshooting.md documents.
Separate end-user troubleshooting items (/Troubleshooting.md) from development troubleshooting items
(/docs/dev/Troubleshooting.md)

### Documenting the design decisions
Add decision items as [action id].[action-title].md file to docs/dev/design/decisions/.
See below sections (Action id, Action format) for namign adn formatting.

### Documenting the suggestions
As you assume team roles as described in 'Team roles' section below, come up with suggestions for improvements.
Add suggestion items as [action id].[action-title].md file to docs/dev/suggestions/.
Suggestions are reviewed by the manager and mastermind role.
The approved suggestions get moved to docs/dev/suggestions/approved.
Implemented suggestions get moved to docs/dev/suggestions/done.

### Documentation formatting
Because we often read documentation as plain-text Markdown, we want it to look good in plain text editor.
In particular, we maintain line limit of 120 characters per line.

### Action id
Include an action id for each action (suggestion, TODO item, etc.), the form of XMMDDYYNN, where X is action code
(D for decisions, S for suggestions, T for TODO items, M for migrations, P for planning documents), YY is year, MM is month (01-12), DD is day (01-31), NN is a sequence number. For instance, S07142501 is the first suggestion on July 14, 2025.
The action file (md file that conains the action) is named [action id].[action-title].md
and placed into appropriate directory (docs/dev/design/decisions/, docs/dev/suggestions/, docs/dev/todo/ and the like).

### Action format
Inside md file, place each action item (suggestion, TODO item, etc.) under a separate section (H3 heading)
with action id and title.
Inside the section, include one paragraph describing the item.
For bigger items (bigger suggestions, migrations), include a bullet list with the steps for carrying it out.
Insert a blank line between the items if multiple items share a file (rare).

### Making suggestions
Output suggestions into the [action id].[action title].md documents in suggestions dir (docs/dev/suggestions/).
Focus each suggestion on a specific topic, so it may be implemented in parallel with other tasks.

## Various
- Use 'brief' notation for getters and setters (x() instead of getX())

## What to avoid
Being a principled team, we fight a few dogmas.

We generally avoid, unless there is a valid reason:
- non-wildcard imports 
- Optionals
- reflection
- Camel-case abbreviations (e.g. HTML -> Html)
- HTML formatting tags in docstrings, such as <p> and <br>
- non-Docstrings comments (the meaning should stem from code)
- testing a class in total isolation (we test cohesive clusters of classes instead of mocking around) 
