from examples.store.schemas.analytics import CustomerDailyTotal, ProductDailySummary
from examples.store.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
from examples.store.schemas.customer import Customer
from examples.store.schemas.fulfillment import (
    DailyFulfillmentSummary,
    FulfillmentAllocation,
    FulfillmentBackorder,
    FulfillmentOption,
    FulfillmentPlan,
    FulfillmentPreferredOption,
    InboundInventory,
    InboundInventoryAvailability,
    InventoryPosition,
    Order,
    FulfillmentReconciliation,
    ReplenishmentSuggestion,
    Warehouse,
    WarehouseLoadSummary,
)
from examples.store.schemas.merchandising import (
    CatalogAvailability,
    CatalogProduct,
    DailyRecommendationBehavior,
    DailyRecommendationClicks,
    DailyRecommendationImpressions,
    MerchandisingBoost,
    MerchandisingPolicy,
    MerchandisingSuppression,
    ProductRecommendationSignal,
    RecommendationCandidate,
    RecommendationClick,
    RecommendationEvaluationBatch,
    RecommendationImpression,
    RecommendationRequest,
    RecommendationRequestBehavior,
    RecommendationRun,
    RecommendedProduct,
)
from examples.store.schemas.order import (
    OrderFulfillment,
    OrderNormalized,
    OrderPublication,
    OrderPublished,
    OrderRaw,
    OrderWithCustomer,
    OrderWithProduct,
    OrderWithPromotion,
    PublicationFlags,
)
from examples.store.schemas.product import BlockedProduct, Product, ProductBase
from examples.store.schemas.promotion import Promotion
from examples.store.schemas.shipment import Shipment
