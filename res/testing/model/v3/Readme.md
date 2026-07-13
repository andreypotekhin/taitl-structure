# v3 Orders Testing Model

This fixture is the release-level happy-path model for Structure v3. It retains the v2 orders pipeline so that every
implemented v3 capability composes with the established schema, join, analytics, and generated-code contracts.

The v3 additions are deliberately concentrated in a few transforms:

- `orders.transforms.v3.V3OrderFeatures` demonstrates the v3 Column API, SQL helpers, variadic `where(...)`, explicit
  `@step(...)`, and null-aware ordering in a reusable order projection.
- `orders.transforms.rowset_join.RowsetJoinExamples` demonstrates using-key compatible rowset joins, full/right
  diagnostics, explicit Cartesian acknowledgement, join strategy directives, and forward-safe join behavior.
- `orders.transforms.adv_analytics.AdvancedOrderAnalytics` demonstrates grouping sets-compatible aggregation
  primitives, aggregate windows, collection helpers, and their typed outputs.
- `orders.transforms.order.EnrichOrders` demonstrates source-ordered `@raw` boundaries alongside explicit steps and
  streaming-safe transform metadata.

The fixture is intentionally Spark-free to import and compile. Generated output under
`res/testing/model/v3/structure_generated/orders/` is the corresponding golden compiler output. It uses the PySpark
`>=4.0,<4.1` profile because the scalar showcase includes the profile-gated `try_cast(...)` helper.
