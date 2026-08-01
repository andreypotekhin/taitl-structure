from examples.store.schemas.merchandising import RecommendationClick, RecommendationImpression
from examples.store.schemas.merchandising.evaluation import RecommendationRequestBehavior
from examples.store.schemas.merchandising.intermediate import RecommendationBehaviorImpression
from structure import *
from structure.plugin.pyspark import *


class MeasureRecommendationImpressions(Transform):
    selected_requests = input(RecommendationRequestBehavior)
    impressions = input(RecommendationImpression)
    clicks = input(RecommendationClick)
    displayed = lane(RecommendationBehaviorImpression)
    clicked = lane(RecommendationBehaviorImpression)
    measured = output(RecommendationBehaviorImpression)

    @step(input=[selected_requests, impressions], output=displayed)
    def select_impressions(
        self, request: RecommendationRequestBehavior, impression: RecommendationImpression
    ) -> RecommendationBehaviorImpression:
        inner_join(
            impression,
            on=(impression.tenant.tenant_id == request.tenant.tenant_id)
            & (impression.request_id == request.request_id),
        )
        return RecommendationBehaviorImpression.project(impression)(
            window=request.window,
            impression_id=impression.id,
            click_count=0,
        )

    @step(input=[displayed, clicks], output=clicked)
    def attribute_clicks(
        self, impression: RecommendationBehaviorImpression, click: RecommendationClick
    ) -> RecommendationBehaviorImpression:
        inner_join(
            click,
            on=(click.impression_id == impression.impression_id)
            & event_time_between(impression.shown_at, click.occurred_at, upper="24 hours"),
        )
        group_by(
            window=impression.window,
            tenant_id=impression.tenant.tenant_id,
            request_id=impression.request_id,
            strategy_id=impression.strategy_id,
            policy_version=impression.policy_version,
            impression_id=impression.impression_id,
            shown_at=impression.shown_at,
            product_id=impression.product_id,
            rank=impression.rank,
            examination_propensity=impression.examination_propensity,
        )
        return RecommendationBehaviorImpression.project(impression)(
            click_count=count(),
        )

    @step(input=[displayed, clicked], output=measured)
    def measure_impressions(
        self, displayed: RecommendationBehaviorImpression, clicked: RecommendationBehaviorImpression
    ) -> RecommendationBehaviorImpression:
        left_join(clicked, on=clicked.impression_id == displayed.impression_id)
        return RecommendationBehaviorImpression.project(displayed)(
            click_count=coalesce(clicked.click_count, 0),
        )
