# Examples

These small applications demonstrate Structure in distinct data-processing domains. Each guide explains the model,
transform boundaries, and caller-owned responsibilities alongside focused usage snippets.

| Example | Focus | Execution boundary |
| --- | --- | --- |
| [School](school/Readme.md) | Algebra, vectors, and matrices | Streaming rows and batch matrices. |
| [Search](search/Readme.md) | Corpus, search, similarity, and feedback | Batch corpus and streaming feedback facts. |
| [Security](security/Readme.md) | Posture, event audit, reports, and quality | Streaming audit and batch reports. |
| [Stocks](stocks/Readme.md) | Daily-bar technical-analysis indicators | Batch-only historical calculation. |
| [Streams](streams/Readme.md) | White-water kayaking timing, progress, and penalties | Spark Structured Streaming. |

Examples are designed as reference applications, not production services. Structure transforms data; callers provide
sources, persistence, execution lifecycle, and the business decisions made from the resulting datasets.
