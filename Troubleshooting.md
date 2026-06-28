# Troubleshooting

## Input DataFrame Column Is Not a Python Identifier

Use a Python-safe field name and set `alias` to the real Spark column name:

```python
promotion_code = field(String(), nullable=True, alias="promo-code")
```

Transform code uses `promotion_code`. Spark schemas, validation, expression reads, and projection output use
`promo-code`. Aliases are schema-local unless the field definition is inherited. Structure passes alias strings through
to Spark, so choose Spark-compatible physical column names or normalize the DataFrame before calling Structure.
