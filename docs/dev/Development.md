# Development

## Project overview
[Overview.md](../Overview.md)

## User stories and use cases
See [UserStories.md](specifications/UserStories.md) for detailed description of library external behavior.

## Terminology
See [Terminology.md](Terminology.md) for project language.

See [Concepts.md](Concepts.md) for the
concept-test coverage map.

## Architecture
Main: [Architecture.md](Architecture.md)

Design docs: **/docs/dev/design**

Specifications: **/docs/dev/specifications**

## Coding
Code structure: [Code.md](Code.md)

Coding style: [Style.md](Style.md)

## Setup
See [Setup.md](Setup.md) for project setup and prerequisites.

## Building
### Prerequisites

    Python 3.12+
    Poetry (prefer Pipx installation)
    make

### Build project

    cd [project]
    make help
    make install
    make build

## Testing
Main: [Testing.md](Testing.md)

Testing guidelines: [Style.md](Style.md)

## Support and Contributions

We use a contributor-led support model suited to a developer audience. A code-related issue must include all of the
following:

- A minimal runnable code example, including the dependency and runtime versions needed to run it.
- The complete observed output, including an error and traceback when present.
- The expected output or behavior.
- A pull request with the proposed fix and a regression test derived from the example.

The issue establishes a reproducible contract; the pull request makes the remedy reviewable and testable. Requests
that cannot be reproduced or do not include a proposed fix remain incomplete. Non-code questions, documentation
corrections, and feature proposals do not need a runnable reproduction, but should state their use case precisely.

For work tracked inside this repository, create an [issue record](issues/Readme.md). The record is an automation-ready
copy of the report and points to its pull request. Do not put credentials, customer data, or other sensitive material
in either artifact.

## Troubleshooting
[Troubleshooting.md](Troubleshooting.md)
