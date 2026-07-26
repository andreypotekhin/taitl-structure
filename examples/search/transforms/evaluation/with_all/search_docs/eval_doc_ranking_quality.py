"""Combined label-and-user-band judged document-ranking evaluation."""

from examples.search.schemas.clicks import SearchRequest
from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.judged_quality import EvaluationQuery
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.search import SearchQuery
from examples.search.schemas.user import CohortLineage, CohortMembership, UserBand
from examples.search.transforms.evaluation.with_labels.search_docs.eval_doc_ranking_quality import (
    EvaluateDocumentRankingQuality as LabelSelection,
)
from examples.search.transforms.evaluation.with_users.search_docs.eval_doc_ranking_quality import (
    EvaluateDocumentRankingQuality as UserSelection,
)
from structure import step
from structure.plugin.pyspark import cross_join, group_by, inner_join, where


class EvaluateDocumentRankingQuality(UserSelection):
    """Evaluate rankings selected by both caller query labels and one user band."""

    @step(output=UserSelection.evaluated_queries)
    def select_queries(
        self,
        query: SearchQuery,
        request: SearchRequest,
        membership: CohortMembership,
        lineage: CohortLineage,
        user_band: UserBand,
        batch: EvaluationBatch,
        params: EvaluationParams,
    ) -> EvaluationQuery:
        """Apply the label predicate to the user-selected query/context population."""

        cross_join(batch, allow_cartesian=True)
        cross_join(params, allow_cartesian=True)
        inner_join(on=request.query_id == query.id)
        inner_join(on=membership.user_id == request.user_id)
        inner_join(on=lineage.cohort_id == membership.cohort_id)
        inner_join(on=user_band.user_id == request.user_id)
        where(
            params.user_band.is_not_null(),
            lineage.ancestor_cohort_id == params.user_band.id,
            LabelSelection._matches(query, params),
        )
        group_by(
            window=batch.window,
            params=params,
            experiment_id="",
            band_id=user_band.band_id,
            search_query_id=query.id,
        )
        return EvaluationQuery(
            window=batch.window,
            params=EvaluationParams(labels=params.labels, user_band=params.user_band),
            experiment_id="",
            band_id=user_band.band_id,
            search_query_id=query.id,
        )
