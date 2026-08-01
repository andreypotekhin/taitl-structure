# Orders Testing Model

This is the consolidated compiler and integration-test model. It combines the v1 through v4 order fixtures into one
stable package under `testing.model.orders`.

The model includes the order enrichment pipeline, rowset-join examples, daily and advanced analytics, v3 scalar
coverage, and v4 scalar coverage. Its checked-in generated compiler artifacts live under
`res/testing/model/structure_generated/orders/`.

The Store example application remains a separate end-user fixture. Store tests and Store-generated goldens continue to
use `examples.store` and `examples/structure_generated/store`.
