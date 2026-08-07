# Troubleshooting

## Disk-less Source Transform Is Unavailable or Ambiguous

When calling `session.run(transform="package.module:Transform", ...)`, Structure reports that no source transform is
compiled or that the selected transform is ambiguous.

Compile the source tree into that session first with `session.compile(sources)`. If two source trees expose the same
module and class name, give each variant a distinct Python package root. See
[disk-less source compilation](docs/dev/specifications/DisklessSourceCompilation.spec.md).

## Input DataFrame Column Is Not a Python Identifier

Use a Python-safe field name and point `alias` at the real Spark column:

```python
promotion_code = string(nullable=True, alias="promo-code")
```

Transform code uses `promotion_code`. Spark schemas, validation, expression reads, and projection output use
`promo-code`. Aliases are schema-local unless inherited. Structure passes alias strings through to Spark, so
choose Spark-compatible physical column names or normalize the DataFrame before calling Structure.

## Nested Struct Assignment Fails

For a field declared as `struct(Address, ...)`, assign either a compatible whole struct expression or construct
the nested schema explicitly:

```python
shipping=Address(
    city=trim(order.shipping.city),
    postal_code=order.shipping.postal_code,
)
```

Structure checks the nested schema identity, so another schema with the same fields is not enough. If you only need to
change one child field, construct the full nested value for now; partial nested updates are planned but not part of the
current supported surface.

## Streaming Lifecycle Code Appears in a Transform or Hook

If streaming diagnostics mention `STREAM-E0801` or `STREAM-W0801` around lifecycle code, keep `readStream`,
`writeStream`, checkpoint locations, triggers, output modes, query names, `start()`, `stop()`, `awaitTermination()`,
`foreach`, and `foreachBatch` outside Structure transforms.

Structure transforms should return DataFrame plans. Put lifecycle calls in caller-owned PySpark code such as
`examples/streams/adoption.py`, then pass the streaming DataFrame into online or generated Structure execution. See
[Streaming API](docs/api/Streaming.api.md) and
[Spark Streaming](docs/dev/specifications/SparkStreaming.spec.md).
