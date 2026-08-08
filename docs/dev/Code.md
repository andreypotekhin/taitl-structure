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
The installable package lives under `src/structure`. Inside that package, project code consists of Core and libraries:

- `src/structure/core` - Core implementation
- `src/structure/lib` - libs
- `src/structure/plugin/api` - Plugin API  
- `src/structure/plugin/pyspark` - PySpark plugin (PySpark DSL and compiler implementation)   

Core components: configuration, cli, dsl, compiler, target, runtime
and other system components (and subcomponents) defined by project architecture. Compiler subapps include
frontend, discovery, symbolic_execution, ir, compileability, diagnostics, and traceability. Target subapps include
capabilities and concrete targets such as pyspark.

```text
src/structure/
  core/
    cli/
    compiler/
      api/
      frontend/
      ir/
      symbolic_execution/
      compileability/
    configuration/
    dsl/
    runtime/
      api/
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

We refer to apps and libraries with slash notation (`core/cli/`, `core/cli`), full slash notation
(`structure/core/cli/`), dot notation (`structure.core.cli`), space notation (Core CLI, CLI Core), canonic notation
(CLI Core, Helper Library), and sometimes reverse notation (lib common).

### Library package structure
Library package structure: no specific structure, various subpackages as need arises

### Application package structure
Application package structure:

structure/core/[component]/
  - api/ - Programmatic API endpoints - application entry points.
    The main endpoint is an uppercase stateless class, such as `Compiler`, `Runtime`,
    `PySpark`, `Configuration`, `Capabilities`, or `CliApp`.
    Sub-endpoints are class attributes, such as `Compiler.frontend`, `Runtime.schemas`,
    and `PySpark.render`.
    Endpoint methods are static factories returning fresh command instances.
      Usage: `Compiler.frontend.analyze()` returns a new structural-plan command; `Compiler.frontend.compile()` returns
      a new Core-to-plugin compilation command. `Compiler.frontend.author()` is the bundled-documentation endpoint
      that attaches a selected plugin body without lowering it.
  - commands/ - action-oriented command classes called from endpoint methods.
    Ex: `AnalyzeTransform` implements frontend analysis; `CompileTransform` is the transitional authoring runner;
    `CompilePluginTransform` dispatches plugin compilation.
    Commands are created/invoked only through api facade endpoints - not directly.
      Usage: `Compiler.frontend.analyze()(TransformClass)` or `Compiler.frontend.compile()(TransformClass)`.
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
For instance, we can have a `runtime/execution/online/` directory for the direct execution component app.
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

#### Inter-app invocations
Invocations between top-level apps should go through their api endpoints. We have a number of cases
when apps call other apps' commands directly, but we want to eliminate that pattern.

### Plugin package structure
- `src/structure/plugin/api` - Plugin API: defines Structure Plugin API for plugin developers.
- `src/structure/plugin/pyspark` - PySpark plugin: PySpark DSL and compiler implementation - Structure's sole bundled plugin.

PySpark plugin package is divided into smaller 'applications' similarly to Core.
Applications typically consist of api/, commands/, logic/ subpackages, with api/ endpoints used for interapp calls.

#### Helper Library
Any and all code that is general/not pertaining to immediate business use case must be placed/relocated
to Helper Library (`structure.lib.helper`)
Rationale: we want to keep business classes code slim and focused, and in the same time facilitate reuse
by creating helpers.
Example: `structure/lib/helper/strings.py` for string helper functions.
Organize the helpers by general concept (e.g. files.py, os_paths.py, strings.py) or even subconcept (file_extentions.py)

###  High traffic areas
There are parts of this open source library that are frequently visited by end users, who browse library code
in to clarify its behavior or to troubleshoot an issue. We call certain areas of Structure library code
'high traffic' because of that, because they constitute the public surface end-user sees first, e.g. when 
navigating to a public-facing class or DSL method definition in an IDE. These areas should be kept helpful 
to end-user with comprehensive doc comments and references to PySpark parity functions/methods.  

High-traffic areas:
- `src/structure/plugin/api` - Plugin API
- `src/structure/plugin/pyspark/dsl` - PySpark DSL
- `src/structure/core/dsl` - Core DSL

Of these areas, the top (outmost) package is most critical for commenting, whereas 
subpackages need more balanced approach. No need to add comments to small classes such
as constant definitiosn (JoinHint) whose purpose is evident from context.

Implementation classes (under logic/) do not need to change from scarce commenting.

We should also try keeping implementation details in subpackages rather than on the top (outmost) package.  

Comments should describe purpose/behavior to end-user, not jsut give implementation-level detail.

Adopt Args/Returns/Example comments to the main member of each function family. 
Ok for one-liner comments on other members of the family.
In examples, avoid extra lines like "```python" or other backtick lines, '...' lines.
Change 'Examples:' to singular 'Example:' if only showing one example. 

Mention PySpark DSL counterpart, if non-obvious/non-exact.

### Example apps structure
We provide several example apps - Search, Security, School - under examples/ dir, as aid to end-user 
and as input to integration and golden tests.

These applications almost entirely consist of schemas and transformers - there is no attempt to make them 
ready to deploy - end-user needs to provide data, Spark instance etc. in order of the code to run.

```text
examples/
  fixtures/ - Per-app data for the tests
  plugin/ - Example minimal plugin to aid plugin developers
  school/ - Example app
  search/ - Example app
  security/ - Example app
  ...
```
Example app dir is divided into schemas/ and transforms/:
```text
examples/
  search/
    schemas/
    transforms/
  ...
```

Inside `schemas/` and `transforms/`, the app is divided into cohesive modules. Example: evaluation, 
experiments, indexing, scoring modules in the Search app.

- Inside a module, further submodules can be introduced as needed.
- Schema dir structure follows module breakdown of transforms/ dir.
- Schema dir keeps intermediate (e.g. lane-specific) schemas in a designated file: workflow.py

#### Example apps file names
A module/submodule often implements an ordered sequence of transforms - a pipeline 
with stages. 

##### 'Alpha+workflow' naming 
To allow code reader understand the order of pipeline stages, we name the files in the
way that convey the processing order from top to bottom. For instance, the pipeline inside 
searching/search_docs/ starts with admit.py and proceeds through rerank.py; the last
file - workflow.py - defines the overall pipeline. Classes inside such stage-named files 
can differ in name with file name, although most of them reflect file name.
```text

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
Note: we are currently switching from CamelCase to underscored_lower in example app file naming
(not in the main codebase). Please use it from this point on in example apps.  

#### Example apps code guidance

- Use .project()/.base() liberally
- Prefer result = outputs(...) output assignments
- Use wildcard imports, for brevity
