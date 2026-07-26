"""Combined label-and-user-band judged document-ranking evaluation."""

from examples.search.schemas.evaluation.batch import EvaluationBatch
from examples.search.schemas.evaluation.judged_quality import EvaluationQuery
from examples.search.schemas.evaluation.params import EvaluationParams
from examples.search.schemas.search import DocumentSearchResult, SearchQuery
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
        result: DocumentSearchResult,
        batch: EvaluationBatch,
        params: EvaluationParams,
    ) -> EvaluationQuery:
        """Apply the label predicate to the user-selected query/context population."""

        cross_join(batch, allow_cartesian=True)
        cross_join(params, allow_cartesian=True)
        inner_join(on=result.search_query_id == query.id)
        where(
            params.band_id.is_not_null(),
            result.band_id == params.band_id,
            LabelSelection._matches(query, params),
        )
        group_by(
            window=batch.window,
            params=params,
            experiment_id="",
            band_id=result.band_id,
            search_query_id=query.id,
        )
        return EvaluationQuery(
            window=batch.window,
            params=EvaluationParams(labels=params.labels, band_id=params.band_id),
            experiment_id="",
            band_id=result.band_id,
            search_query_id=query.id,
        )
