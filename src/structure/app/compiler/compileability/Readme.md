# Compileability App

## Purpose
The compileability app hosts checks that decide whether already-compiled plans satisfy a narrower execution contract.
It is intentionally separate from source compilation so feature-specific readiness checks can evolve without bloating
the compiler frontend.

## Dependency Exchanges
The app consumes lowered target recipes or backend-neutral plans, depending on the check, and returns focused reports
that callers can show in CLI output, tests, or future diagnostics. It depends on compiler IR and target recipe models
only where a check genuinely needs those shapes.

## Inner Workings
Today this app is a container for the nested `streaming_compatibility` app. Future checks should follow the same
pattern: a small `api/` export, action classes under `logic/actions/`, and report or finding records under
`logic/model/`.
