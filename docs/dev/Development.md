# Development

## Project overview
TBD

## User stories and use cases
See Specification.md for detailed description of library external behavior.

## Terminology
See Concepts.md for terminology.

## Architecture
Main: Architecture.md
Docs: /docs/dev/design
Specifications: /docs/specifications

## Coding
Main: Code.md

### Code structure
Package structure:
- com.taitl.existential: public code (classes, interfaces) for use by end-user
- com.taitl.ex and subpackages: private code/implementation
  -  com.taitl.ex.common: common/ubiquitous classes (Creator, Args, State)
  -  com.taitl.ex.cross: cross-cut concepts (caching, logging)
  -  com.taitl.ex.concrete: concrete implementations (e.g. ConcreteExists) for the classes the end-user creates
     with 'new'
  -  com.taitl.ex.core: core classes, such as ExistentialConfigs, immediately used by public code
  -  com.taitl.ex.logic: business logic implementation
  -  com.taitl.ex.configuration: configuration logic (e.g. BuildContexts)
  -  com.taitl.ex.events: event processing logic (e.g. ReceiveEvent)
  -  com.taitl.ex.library: dealing with library as a whole
  -  com.taitl.ex.transactions: transaction logic (e.g. BeginTransaction, RollbackTransaction)
  -  com.taitl.ex.validation: validation logic (e.g. ValidateTransaction)

## Setup
See /docs/dev/Setup.md for setup and prerequisites.

## Building
We use Poetry as build tool. 

    commands TBD


## Testing
Main: Testing.md
Testing guidelines: Style.md 

## Troubleshooting
Refer to /docs/dev/Troubleshooting.md.
