from examples.store.schemas.analytics import CustomerDailyTotal, CustomerEventRank, ProductDailySummary
from examples.store.schemas.order import OrderFulfillment
from examples.store.transforms.order_analytics.customer_daily_totals import CustomerDailyTotals
from examples.store.transforms.order_analytics.customer_event_ranks import CustomerEventRanks
from examples.store.transforms.order_analytics.product_daily_summaries import ProductDailySummaries
from structure import Transform, input, output, stage


class OrderAnalytics(Transform):
    fulfilled = input(OrderFulfillment)

    customer = stage(CustomerDailyTotals(fulfilled=fulfilled))
    product = stage(ProductDailySummaries(fulfilled=fulfilled))
    ranks = stage(CustomerEventRanks(fulfilled=fulfilled))

    customer_totals = output(CustomerDailyTotal, customer.customer_totals)
    product_summary = output(ProductDailySummary, product.product_summary)
    customer_event_rank = output(CustomerEventRank, ranks.customer_event_rank)
