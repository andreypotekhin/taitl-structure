from __future__ import annotations

from typing import Any, cast

from helpers.example_projects import render_store_example

from examples.store.transforms.personalization.score import PersonalizationAlgorithm, ScorePersonalizedRecommendations
from structure.core.compiler.api import Compiler
from structure.plugin.pyspark import literal
from structure.plugin.pyspark.compiler.model.PySparkExecutionPlan import PySparkExecutionPlan


def test_store_generated_personalization_is_separate_and_tenant_aware() -> None:
    generated = render_store_example()
    workflow = generated[
        "examples/structure_generated/store/pyspark/transforms/examples/store/transforms/"
        "personalization/workflow.py"
    ]
    recommender = generated[
        "examples/structure_generated/store/pyspark/transforms/examples/store/transforms/"
        "recommender/workflow.py"
    ]

    assert "class BuildProductFeaturesGenerated:" in workflow
    assert "class BuildPersonalizationHistoryGenerated:" in workflow
    assert "class ScorePersonalizedRecommendationsGenerated:" in workflow
    assert 'F.col("recommendation_request.tenant.tenant_id")' in workflow
    assert 'F.col("recommendation_request.customer_id")' in workflow
    assert 'F.col("recommendation_request.session_id")' in workflow
    assert 'history__history = frames["history__session_history"]' in workflow
    assert "history__history = history__history.union(frames[\"history__purchase_history\"])" in workflow
    assert "personalized__scored__requests" in recommender


def test_personalization_algorithm_is_replaceable_like_ranker() -> None:
    assert getattr(PersonalizationAlgorithm, "_structure_special_type", None) is None

    class ConstantPersonalizationAlgorithm(PersonalizationAlgorithm):
        def factorization_score(self, request: Any, product: Any) -> Any:
            return literal(0.42)

    class ConstantPersonalizedRecommendations(ScorePersonalizedRecommendations):
        algorithm = ConstantPersonalizationAlgorithm()

    plan = cast(
        PySparkExecutionPlan,
        Compiler.frontend.compile()(
            ConstantPersonalizedRecommendations,
            materialize_schemas=False,
            target_profile=None,
        ).lowered,
    )
    assert any(
        projection.field.name == "factorization_score"
        and projection.expression.kind == "call"
        and projection.expression.data["function"] == "coalesce"
        and projection.expression.args[0].kind == "literal"
        and projection.expression.args[0].data["value"] == 0.42
        for step in plan.steps
        for projection in step.projection
    )
