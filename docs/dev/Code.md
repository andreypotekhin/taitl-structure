# Code

## Coding Standards
Coding guidelines: See the Coding section in [Style.md](Style.md).

## Code structure 
Our code consist of apps and libraries. Apps are active chunks of the system,
implementing business logic. Libraries are passive chunks, facilitating 
reuse, sharing and preventing app code bloat. 

### Code structure guidelines
- Adhere to Logic Oriented Programming principles to keep classes short and focused
  (see the OOP section in [Style.md](Style.md))
- Apps are structured according to Common App Framework (see below)
- Logic packages are structured according to Logic Oriented Programming
- Rule: one-class-per-source-file for the classes
- Always define the value returned by a method in method signature

### App structure
The installable package lives under `src/structure`. Inside that package, project code consists of apps and libraries:

- `src/structure/app` - apps
- `src/structure/lib` - libs

Apps: configuration, cli, dsl, compiler, target, runtime
and other system components (and subcomponents) defined by project architecture. Compiler subapps include
frontend, discovery, symbolic_execution, ir, compileability, diagnostics, and traceability. Target subapps include
capabilities and concrete targets such as pyspark.

```text
src/structure/
  app/
    cli/
    compiler/
      frontend/
      ir/
      symbolic_execution/
      compileability/
    configuration/
    dsl/
    runtime/
      execution/
        online/
        generated/
      session/
      schemas/
    target/
      capabilities/
      pyspark/
  
  lib/
    app/ - Common App Framework vocabulary
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
  - api/ - Programmatic API endpoints - application entry points.
    The main endpoint is an uppercase stateless class, such as `Compiler`, `Runtime`,
    `PySpark`, `Configuration`, `Capabilities`, or `CliApp`.
    Sub-endpoints are class attributes, such as `Compiler.frontend`, `Runtime.schemas`,
    and `PySpark.render`.
    Endpoint methods are static factories returning fresh command instances.
      Usage: `Compiler.frontend.compile()` returns a new `CompileTransform` command.
  - commands/ - action-oriented command classes called from endpoint methods.
    Ex: `CompileTransform` implements frontend compilation.
      Usage: `Compiler.frontend.compile()(TransformClass)`.
  - model/ - public app model exposed by endpoint parameters, return types, or API exports.
  - logic/ - app-private implementation classes used by commands and models.
  
API modules are main entry points into an app packages, constituting programmatic API for external and inter-app use. 

Common execution flow within an app:  
- API endpoint classes instantiate commands which delegate to private logic classes.
- Command classes provide an entry point - __call__ method - with specific (preferably, named) arguments.

Lifecycle: Endpoint classes are stateless and long-lived. Commands and other logic classes are ephemeral and disposed
immediately upon use, spanning maximum a single API request.

### Logic package structure
Application packages, except, logic/ are flat. 
The logic/ package further splits into deeper package hierarchy:

logic/  
  - data/ - data-oriented classes, mostly method-less, for simple transfer of information
    Data classes are normally uses as method arguments to 'package' multiple parameters
  - model/ - app-private domain model. Public model types belong in app-level `model/`.
  - maps/ - mappings between data structures - stateless read-only/no side effect.
  - rules/ - business rules. Normally, stateless boolean callables.

Command classes are the public action entry points. `logic/` is implementation-only.

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
