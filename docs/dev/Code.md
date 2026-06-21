# Code

## Coding Standards
Coding guidelines: See Coding section in Style.md

## Code structure 
Our code consist of apps and libraries. Apps are active chunks of the system,
implementing business logic. Libraries are passive chunks, facilitating 
reuse, sharing and preventing app code bloat. 

### Code structure guidelines
- Adhere to Logic Oriented Programming principles to keep classes short and focused
  (see OOP section in Style.md)
- Apps are structured according to Common App Framework (see below)
- Logic packages are structured according to Logic Oriented Programming
- Rule: one-class-per-source-file for the classes
- Always define the value returned by a method in method signature

### App structure
The installable package lives under `src/structure`. Inside that package, project code consists of apps and libraries:

- `src/structure/app` - apps
- `src/structure/lib` - libs

Apps: configuration, cli, discovery, schema, symbolic, ir, compileability, runtime
and other system components (and subcomponents) defined by project architecture.

```text
src/structure/
  app/
    cli/
    configuration/
    discovery/
    runtime/
    symbolic/
  
  lib/
    app/ - Common App Framework (defines classes like Command, Endpoint)
    common/ - common classes, shared constants
    helper/ - shared helpers (no business logic)
```

We refer to apps and libs with slash notation (`app/cli/`, `app/cli`), full slash notation
(`structure/app/cli/`), dot notation (`structure.app.cli`), space notation (app cli, cli app), canonic notation
(CLI app, Helper Library), and sometimes reverse notation (lib common).

### Library package structure
Library package structure: no specific structure, various subpackages as need arises

### Application package structure
Application package structure:

structure/app/[app]/
  - api/ - Endpoints of programmatic API - application entry points
    Endpoints can be simple (single command) or compound - representing groupings of commands (actions). 
      Ex: HelpEndpoint provides helpCompile() method returning new instances of HelpCompile command.  
      Usage: helpEndpoint.helpCompile()  
  - logic/ - 'logic' classes implementing business logic 
  - logic/actions/ - command/action classes serving as business logic entry points
    Ex: Compile command action implementing 'compile' CLI command 
      Usage: cli.commands.compile() creates Compile command 
      compile(args, ...) calls the command
  
API modules are main entry points into an app packages, constituting programmatic API for external and inter-app use. 

Common execution flow within an app:

- API endpoints instantiate and run Commands which delegate to other Logic classes
- On each API request, endpoint creates new Command instance (Command instances are ephemeral). 
- Command classes provide an entry point - __call__ method - with specific (preferably, named) arguments.

Lifecycle: App and Endpoint classes tend to be static/existing for long time. 
Command and other logic class instances are ephemeral and disposed immediately upon use. 

### Logic package structure
Application packages, except, logic/ are flat. 
The logic/ package further splits into deeper package hierarchy:

logic/  
  - actions/ - action-oriented classes causing effect on business
    Action classes are main entry point to logic/
  - data/ - data-oriented classes, mostly method-less
    Data classes can be used in and outside logic/ for data arguments
  - model/ - domain model. 
    - Don't be shy of defining further subpackages for cohesive model classes
    - As usual, one-class-per-source-file for the classes
  - maps/ - mappings between data structures (read-only/no side effect)
  - rules/ - business rules

Action classes are main entry points into logic package. 

### Action-oriented naming
You'll notice that we don't have many actor/-or/-er ending classes (AbcLoader, XyzManager). 
This is replaced by action classes that are typically named verb+noun (e.g. GetProfiles)
Actor classes, if any, tend to be high-level orchestrating classes or the ones that correspond 
to project vocabulary (e.g. generator).

### Recursivity of app/logic package structure 
App and logic packages can consist of other (sub-) application and logic packages.
For instance, we can have runtime/execution/online/ dir structure for Online Execution component app.
In such case, the packages do not follow the above described structure: instead, they are 
simply a set subpackages. In other words, app/logic hierarchy as a whole 
definitely adheres to the above structure on leaf packages, and definitely does not 
in the non-leaf packages.

#### Logic classes instantiation
Logic classes are typically stateless, receiving all data through their method parameters.
More rarely, a logic class may have a state (context) initialized by its owning class, 
with the goal of passing this state down to delegate logic classes, e.g. the ones owned by this 
logic class.

Logic classes are typically instantiated as a static instance, user classes thus can sharing that instance.
The logic classes provide an entry point - __call__ method - with specific (preferably, named) arguments. 

#### Helper Library
Any and all code that is general/not pertaining to immediate business use case must be placed/relocated 
to Helper Library (`structure.lib.helper`)
Rationale: we want to keep business classes code slim and focused, and in the same time facilitate reuse
by creating helpers.
Example: `structure/lib/helper/strings.py` for string helper functions.
Organize the helpers by general concept (e.g. files.py, os_paths.py, strings.py) or even subconcept (file_extentions.py)
