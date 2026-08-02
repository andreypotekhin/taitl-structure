from examples.store.schemas.catalog import *
from examples.store.schemas.merchandising import *
from examples.store.schemas.order import *
from examples.store.schemas.personalization import *
from examples.store.transforms.personalization.features import *
from examples.store.transforms.personalization.history import *
from examples.store.transforms.personalization.score import *
from structure import *


class BuildPersonalizedRecommendations(Transform):
    """Build request-scoped personal recommendations from catalog and user signals."""

    algorithm = parameter(PersonalizationAlgorithm())

    requests = input(RecommendationRequest, streaming=True)
    catalog = input(CatalogProduct)
    preferences = input(UserFeaturePreference)
    session_events = input(SessionEvent, streaming=True)
    fulfilled_orders = input(OrderFulfillment, streaming=True)
    recommendations = output(PersonalizedRecommendation)

    featured = stage(BuildProductFeatures(catalog=catalog))
    history = stage(
        BuildPersonalizationHistory(
            session_events=session_events,
            fulfilled_orders=fulfilled_orders,
        )
    )
    scored = stage(
        ScorePersonalizedRecommendations(
            algorithm=algorithm,
            requests=requests,
            catalog=featured.featured,
            preferences=preferences,
            history=history.history,
        )
    )
    result = output(recommendations=scored.recommendations)
