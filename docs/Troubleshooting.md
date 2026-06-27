# Troubleshooting

### Problem (online execution): generated transform is not importable in generated mode

When: Running a transform with `execution_mode = "generated"`.
Error: "Generated transform is not importable."
Cause: Structure is configured to run checked-in generated PySpark, but the generated module is missing, stale, or not
on the Python import path.
Fix: Run `structure compile`, ensure the generated source root is importable, or set `execution_mode = "online"`. See
`docs/specifications/OnlineExecution.md`.

### Problem (online execution): unknown transform constructor input

When: Constructing a transform invocation such as `EnrichOrders(orders=df, typo=df)`.
Error: "Unknown transform input."
Cause: Transform constructors accept only names declared with `input(...)` on the transform class.
Fix: Rename the argument to a declared input name or add a declared `input(...)` to the transform. Runtime context
belongs in `StructureSession(ctx=...)`.

### Problem (compatibility): configured PySpark target does not support a generated feature

When: Running `structure check` or `structure compile`.
Error: "Feature requires PySpark [version], but target_pyspark is [range]."
Cause: The transform uses a DSL feature whose generated PySpark requires an API outside the configured target range.
Fix: Either raise `target_pyspark` in project configuration or rewrite the transform using APIs supported by the
configured runtime. See `docs/Compatibility.md`.

### Problem (tools): schema generation CLI cannot start Spark

When: Running `structure tools schemas generate --from-path ...` or `structure tools schemas generate --from-table ...`.
Error: "Schema generation from paths or tables requires a Spark-available CLI runtime" or "Spark could not start for
schema generation."
Cause: The CLI runs in its own Python process. That process must have PySpark installed and enough Spark configuration
to read the requested path or table. Delta paths also need the user's normal Delta-capable Spark setup.
Fix: Run the command from a Spark-capable shell, or use the Python API inside an existing Spark notebook/job:
`StructureTools.schemas.generate(..., session=StructureSession(spark=spark), to="OrderRaw")`.

### Problem (context): `message` during [when]

When: [describe when problem manifests]
Error: [error message]
Cause: [root cause]
Fix: [steps to fix]

### Problem (PMD): 'Double-brace initialization should be avoided' error
When: Running PMD checks as part of the build process.
Error: "[INFO] PMD Failure: [class] :22 Rule:DoubleBraceInitialization Priority:3
Double-brace initialization should be avoided."
Cause: Default PMD rules flag double-brace initialization.
Reference: https://pmd.github.io/pmd/pmd_rules_java_bestpractices.html#doublebraceinitialization
Causing code:

```
public void configure()
{
  Ex.configure()
      .context(new Context("/api/cats") {{
          invariant(new Invariant<Cat>() {{
              create(c -> "Black".equals(c.color), "Cats are born black");
          }});
          ...
```

Workaround 1: Adjust PMD rules.
```
  pmd-ruleset.xml:
    <rule ref="category/java/bestpractices.xml">
        <exclude name="DoubleBraceInitialization" />
```

Workaround 2: Use configure-with-builders style.
```
  Ex.configure()
    .context("/api/cats")
       .invariant(Cat.class)
         .create(c -> "Black".equals(c.color), "Cats are born black")
```
Details: Double-brace initialization creates an anonymous subclass, which is in
line with the code above. It is often overkill for collections, so PMD flags it
by default.
