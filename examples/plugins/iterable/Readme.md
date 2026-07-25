# Structure Plugin Example: Iterable

This minimal Structure plugin example can serve the starting point for plugin authors. It owns a small
row-processing target: authoring, lowering, execution, serialization, and code (Python) generation.
It imports only the public Structure Plugin API to do the job.

## Authoring a transform

Declare normal Structure fields and use `@step`. The plugin supplies symbolic schema rows while Structure compiles the
method. Return a target schema instance; the Iterable plugin captures its field reads and literals as a projection.

    from structure import Schema, Transform, input, output, step, transform
    from structure_iterable import left_join

    class Order(Schema):
        id: int
        customer_id: int

    class Customer(Schema):
        id: int
        name: str

    class EnrichedOrder(Schema):
        id: int
        customer_name: str

    @transform(target="iterable")
    class EnrichOrders(Transform):
        orders = input(Order)
        customers = input(Customer)
        enriched = output(EnrichedOrder)

        @step(input=[orders, customers], output=enriched)
        def enrich(self, order: Order, customer: Customer) -> EnrichedOrder:
            left_join(customer, on=customer.id == order.customer_id)
            return EnrichedOrder(id=order.id, customer_name=customer.name)

The first input drives order. Secondary inputs are usable through explicit keyed `inner_join(...)` or
`left_join(...)`. Inner join drops unmatched driving rows; left join supplies `None` for absent right fields.
Step methods can accept and return multiple schemas.

## Generated Python

`EnrichOrders.generate(...)` asks the plugin to emit a target-owned module such as
`structure_generated.iterable.transforms.orders`. Its `EnrichOrdersGenerated.run(orders=..., customers=...)` method
returns a list of mapping rows for one final output, or a mapping from output name to lists for multiple outputs.
Generated source uses only the standard library and exposes the algorithm as ordinary loops and indexes; it never calls
back into the plugin executor.

The example intentionally does not allow ordinary Python in transform methods. A plugin author instead defines a compact
symbolic vocabulary - joins, captures that vocabulary during a step invocation, and lowers it to an opaque,
serializable recipe.
