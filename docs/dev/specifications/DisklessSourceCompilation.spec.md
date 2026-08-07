# Disk-less Source Compilation

Structure can compile schemas and transforms supplied as Python text rather than discovered from a project directory.
Use this for notebooks, services that retrieve trusted source from another store, and tests that need isolated source
trees. Source text is imported as Python, so it must follow the normal import-safe source rule and must be trusted by
the caller.

```python
sources = StructureSources.files(
    {
        "orders/schemas.py": schema_text,
        "orders/transforms.py": transform_text,
    }
)
session = StructureSession(spark=spark, config=StructureConfig.create())

artifacts = session.compile(sources)
result = session.run(transform="orders.transforms:EnrichOrders", orders=orders_df)
```

`session.compile(sources)` discovers and validates every concrete transform in the source tree. Its results accumulate
in the session artifact pool alongside ordinary class-based compilations. The session retains the source tree, so
`session.run(...)` needs only the transform address and declared input values. A transform address is its Python module
name followed by `:` and its class name, for example `orders.transforms:EnrichOrders`.

`StructureSources.from_directory(path)` snapshots a real directory into the same source-tree model. Loading is
driver-side Python work; it does not require or use PySpark. The source root is represented by relative POSIX `.py`
paths, which are used for provenance and diagnostics. Python module names remain the execution identifier.

`StructureConfig.create(...)` constructs a configuration from defaults and explicit keyword arguments without reading
`structure.toml` or `pyproject.toml`. It has no source roots by default because `StructureSources` supplies them
explicitly. Existing `StructureConfig.resolve(...)` retains filesystem-project resolution.

If two compiled source trees expose the same transform address, transform-only execution fails as ambiguous. Use
distinct package roots when the session must retain both variants. If no compiled source exposes an address, compile its
`StructureSources` before calling `session.run(transform=...)`.
