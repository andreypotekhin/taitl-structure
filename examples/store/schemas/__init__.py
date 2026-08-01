from examples.store.schemas.analytics import CustomerDailyTotal, ProductDailySummary
from examples.store.schemas.catalog import CatalogAvailability, CatalogProduct
from examples.store.schemas.common import Address, AuditStamp, BusinessDate, TenantKey
from examples.store.schemas.customer import Customer
from examples.store.schemas.fulfillment import (
    DailyFulfillmentSummary,
    DailyFulfillmentServiceSummary,
    DemandWindow,
    FulfillmentAllocation,
    FulfillmentBackorder,
    FulfillmentException,
    FulfillmentOption,
    FulfillmentPlan,
    FulfillmentPreferredOption,
    InboundInventory,
    InboundInventoryAvailability,
    InventoryPosition,
    InventoryProjection,
    FulfillmentServiceEvaluation,
    FulfillmentServiceTotals,
    FulfillmentShortage,
    FulfillmentSubstitutionOption,
    LeadTime,
    Order,
    FulfillmentReconciliation,
    ReplenishmentSuggestion,
    ServiceRiskTarget,
    SubstitutionRule,
    Warehouse,
    WarehouseLoadSummary,
)
from examples.store.schemas.merchandising import (
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
    RecommendationPurchase,
    RecommendationRequest,
    RecommendationRequestBehavior,
    RecommendationRun,
    RecommendedProduct,
    SessionEvent,
    SessionFeature,
)
from examples.store.schemas.taxonomy import ExpandedProductTaxonomy, ProductTaxonomy, TaxonomyAncestor, TaxonomyNode
from examples.store.schemas.experiment import RecommendationAssignment, RecommendationExperiment, RecommendationExposure
from examples.store.schemas.evaluation import (
    EvaluationBatch,
    RecommendationBehavior,
    RecommendationVariantMetric,
    RecommendationVariantMetricTotals,
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
