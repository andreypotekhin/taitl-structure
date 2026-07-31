from examples.store.schemas.analytics import CustomerDailyTotal, CustomerEventRank, ProductDailySummary
from examples.store.schemas.order import OrderFulfillment
from examples.store.transforms.analytics.orders.customer_daily_totals import CustomerDailyTotals
from examples.store.transforms.analytics.orders.customer_event_ranks import CustomerEventRanks
from examples.store.transforms.analytics.orders.product_daily_summaries import ProductDailySummaries
from structure import Transform, input, output, stage


class OrderAnalytics(Transform):
    fulfilled = input(OrderFulfillment)
    customer_totals = output(CustomerDailyTotal)
    product_summary = output(ProductDailySummary)
    customer_event_rank = output(CustomerEventRank)

    customer = stage(CustomerDailyTotals(fulfilled=fulfilled))
    product = stage(ProductDailySummaries(fulfilled=fulfilled))
    ranks = stage(CustomerEventRanks(fulfilled=fulfilled))

    result = output(
        customer_totals=customer.customer_totals,
        product_summary=product.product_summary,
        customer_event_rank=ranks.customer_event_rank,
    )
