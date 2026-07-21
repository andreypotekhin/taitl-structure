# PySpark Capabilities App

## Purpose
Resolves PySpark profile and variant requests into the capability decisions used by Core compilation and execution.

## Dependency Exchanges
The app consumes target profile, Spark Connect variant, and capability requirements. It returns immutable capability
decisions through `PySpark.capabilities`; peer apps use that endpoint rather than its commands or logic.

## Inner Workings
The resolver selects the supported PySpark feature matrix and reports unsupported requirements with the target-specific
diagnostic contract.
