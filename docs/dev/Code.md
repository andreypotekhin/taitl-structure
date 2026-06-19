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

### App structure
Project code consists of apps and libraries:
- /src/app - apps
- /src/lib - libs

Apps: configuration, cli, discovery, schema, symbolic, ir, compileability, runtime
and other high-level system components defined by project architecture.

app/
	cli/
    configuration/
	discovery/
	runtime/
    symbolic/
  
lib/
	app/ - Common App Framework (defines App, Feature, Command, Endpoint classes)
	common/ - common classes, shared constants 
	helper/ - shared helpers (no business logic)

We refer to apps and libs with slash notation (app/cli/, app/cli), dot notation (app.cli), 
space notation (app cli, cli app), canonic notation (CLI app, Helper Library) 
and sometimes reverse notation (lib common).

### Importing
Ex: 'from structure import Structure' should work even if the Structure class is defined in a subpackage (e.g. dsl)

### Library package structure
Library package structure: no specific structure, various subpackages as need arises

### Application package structure
Application package structure:

apps/[app]/
  api/ - Endpoints for programmatic API - application entry points
  app/ - Application-specific subclasses to Common App Framework: [App]App, [App]Feature, [App]Endpoint. Ex: CliFeature
  features/ - Feature classes. Ex: CompileFeature(CliFeature). 
    Usage: CliApp.instance.compileFeature.compile() returning CompileCommand  
  commands/ - Command classes. Ex: CompileCommand(CliCommand)
  logic/ - 'logic' classes implementing business logic
  
Endpoint classes are main entry points into an app package, constituting programmatic API 
into the app for external and inter-app use. 

Common execution flow within an app:
- API endpoints instantiate and run Commands which delegate to Logic classes
- To instantiate a Command, endpoint uses a Feature class to create a Command instance. 
- To access Feature class instances, the endpoint uses static instance of an App class.
- Command classes provide an entry point - __call__ method - with specific (preferably, named) arguments.

Lifecycle: App and Feature classes tend to be static/existing for long time; 
Command and Logic class instances are ephemeral and disposed immediately upon use. 

Unlike Feature and Command classes, the Logic classes do not have a common parent superclass.

### Logic package structure
Application packages, except, logic/ are flat. 
The logic/ package further splits into deeper package hierarchy:

logic/ 
  actions/ - action-oriented classes causing effect on business
    Action classes are main entry point to logic/
  data/ - data-oriented classes, mostly method-less
    Data classes can be used in and outside logic/ for data arguments
  model/ - domain model
  maps/ - mappings between data structures (read-only/no side effect)
  rules/ - business rules

Action classes are main entry points into logic package. 

### Action-oriented naming
You'll notice that we don't have many actor/-or/-er ending classes (AbcLoader, XyzManager). 
This is replaced by action classes that are typically named verb+noun (e.g. GetProfiles)
Actor classes, if any, tend to be high-level orchestrating classes or the ones that correspond 
to project vocabulary (e.g. generator).

### Recursivity of app/logic package structure 
App and logic packages can consist of other (sub-) application and logic packages.
For instance, we can have runtime/execution/online/ dir structure for Online 
Execution component.
In such cases, packages do not follow the above described structure: instead, they are 
simply a set of subpackages. In other words, the whole app/logic hierarchy 
definetely adheres to the above structure in leaf packages, and definetely does not 
in non-leaf packages.

#### Logic classes instantiation
Logic classes are typically stateless, receiving all data through their method parameters.
More rarely, a logic class may have a state (context) initialized by its owning class, 
with the goal of passing this state to other logic classes, e.g. the ones owned by this 
logic class.

Logic classes are typically instantiated as a static field in the class that intends to use the logic (owner class).
Several classes can be using same logic class, in this case they instantiate them separately (we may
use to optimize e.g. to singleton in some scenarios).
Logic fields inside the owner class are usually marked with @logic decorator.
The logic classes provide an entry point - __call__ method - with specific (preferably, named) arguments. 

#### Helper Library
Any and all code that is general/not pertaining to immediate business use case must be placed/relocated 
to Helper Library (lib.helper)
Rationale: we want to keep business classes code slim and focused, and in the same time facilitate reuse
by creating helpers.
Example: lib/helper/strings.py for string helper functions. 
Organize the helpers by general concept (e.g. files.py, os_paths.py, strings.py) or even subconcept (file_extentions.py)
