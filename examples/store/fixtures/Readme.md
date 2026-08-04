# Store fixtures

These small CSVs are source data for the Store example. They intentionally cover a few meaningful paths instead of
trying to model a production-sized catalog:

- two tenants and tenant-scoped lookups;
- a multi-line order, including repeated products on separate lines;
- allocated, partially allocated, and backordered demand;
- inbound inventory, safety stock, substitutions, and service targets;
- active, inactive, and blocked products;
- taxonomy, recommendation, session, impression, and click inputs; and
- nullable promotion, customer, shipment, and order attributes.

CSV columns flatten nested fields. For example, `tenant_id`, `source_system`, and `ingested_at` become the `tenant` and
`audit` structs, while `tags` uses semicolons and `attributes` uses `key=value` pairs. The integration tests convert the
files to the typed Store schemas before executing transforms.

The data is deliberately small and deterministic. Tests should compare representative rows or selected columns; they do
not need to assert every row in every fixture.
