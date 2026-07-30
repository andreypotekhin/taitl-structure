"""Apply one promoted ranking artifact to lexical document candidates."""

from examples.search.schemas.features import DocumentFeatures, QueryFeatures
from examples.search.schemas.search import DocumentSearchCandidate
from examples.search.schemas.training import RankingArtifact
from structure import Transform, input, output, step
from structure.plugin.pyspark import coalesce, cross_join, element_at, exactly_one, inner_join, require_all, types


class RankDocumentCandidates(Transform):
    """Score candidates through the portable standardized-linear artifact contract.

    Callers use this transform only after manually promoting one artifact.  The
    existing ``SearchDocuments`` path remains the no-model fallback because
    Structure transform inputs are intentionally required relations.
    """

    candidates = input(DocumentSearchCandidate)
    artifacts = input(RankingArtifact)
    document_features = input(DocumentFeatures)
    query_features = input(QueryFeatures)
    ranked_candidates = output(DocumentSearchCandidate)

    @step(input=[candidates, artifacts, document_features, query_features], output=ranked_candidates)
    def rank(
        self,
        candidate: DocumentSearchCandidate,
        artifact: RankingArtifact,
        document: DocumentFeatures,
        query: QueryFeatures,
    ) -> DocumentSearchCandidate:
        exactly_one(artifact)
        artifact = cross_join(artifact, allow_cartesian=True)
        inner_join(document, on=document.document_id == candidate.document_id)
        inner_join(query, on=query.query_id == candidate.search_query_id)
        required = (
            element_at(artifact.weights, "lexical_score").is_not_null()
            & element_at(artifact.weights, "query_token_count").is_not_null()
            & element_at(artifact.weights, "query_distinct_token_count").is_not_null()
            & element_at(artifact.weights, "document_content_length").is_not_null()
            & element_at(artifact.weights, "document_url_is_https").is_not_null()
        )
        require_all(required)
        return DocumentSearchCandidate.project(candidate)(
            experiment_id=artifact.model_id,
            score_rank=self._score(candidate, query, document, artifact),
        )

    def _score(self, candidate, query, document, artifact):
        values = {
            "lexical_score": candidate.score,
            "query_token_count": query.token_count.cast(types.double()),
            "query_distinct_token_count": query.distinct_token_count.cast(types.double()),
            "document_content_length": document.content_length.cast(types.double()),
            "document_url_is_https": coalesce(document.url_is_https, False).cast(types.double()),
        }
        return artifact.intercept + sum(
            coalesce(element_at(artifact.weights, name), 0.0)
            * (value - coalesce(element_at(artifact.means, name), 0.0))
            / coalesce(element_at(artifact.scales, name), 1.0)
            for name, value in values.items()
        )
