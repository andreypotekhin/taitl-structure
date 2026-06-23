# Streaming Compatibility App

## Purpose
The streaming compatibility app classifies whether a lowered transform can run safely in a streaming context under
the current v1 contract. It is a compileability check, not the future streaming orchestration runtime.

## Dependency Exchanges
The app consumes `PySparkExecutionPlan` recipes plus join, hook, and expression metadata, then returns a
`StreamingReport` made of `StreamingFinding` values and a highest `StreamingSupport` level. CLI explain output and
specification tests consume the report to make streaming limits visible before runtime.

## Inner Workings
`ClassifyStreamingCompatibility` walks the PySpark recipes and flags constructs such as unsupported joins or unsafe
hooks. `StreamingSupport` expresses the classification vocabulary, while `StreamingFinding` carries the reason and
diagnostic detail needed to explain the decision without importing Spark.
