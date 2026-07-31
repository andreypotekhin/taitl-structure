from examples.store.schemas.analytics import CustomerDailyTotal, CustomerEventRank, ProductDailySummary
from examples.store.schemas.order import OrderFulfillment
from examples.store.transforms.order_analytics.CustomerDailyTotals import CustomerDailyTotals
from examples.store.transforms.order_analytics.CustomerEventRanks import CustomerEventRanks
from examples.store.transforms.order_analytics.ProductDailySummaries import ProductDailySummaries
from structure import Transform, input, output, stage


class OrderAnalytics(Transform):
    fulfilled = input(OrderFulfillment)

    customer = stage(CustomerDailyTotals(fulfilled=fulfilled))
    product = stage(ProductDailySummaries(fulfilled=fulfilled))
    ranks = stage(CustomerEventRanks(fulfilled=fulfilled))

    customer_totals = output(CustomerDailyTotal, customer.customer_totals)
    product_summary = output(ProductDailySummary, product.product_summary)
    customer_event_rank = output(CustomerEventRank, ranks.customer_event_rank)
