"""Build explicit judged rows for offline ranking training."""

from examples.search.schemas.evaluation.judged_quality import DocumentRelevanceJudgment
from examples.search.schemas.features import DocumentFeatures, QueryFeatures
from examples.search.schemas.search import DocumentScore
from examples.search.schemas.training.data import DocumentTrainingData
from structure import Transform, input, output, step
from structure.plugin.pyspark import inner_join


class BuildTrainingData(Transform):
    """Join lexical candidates to caller-supplied relevance judgments for offline use."""

    document_scores = input(DocumentScore)
    judgments = input(DocumentRelevanceJudgment)
    document_features = input(DocumentFeatures)
    query_features = input(QueryFeatures)
    training_data = output(DocumentTrainingData)

    @step(input=[document_scores, judgments, document_features, query_features], output=training_data)
    def build(
        self,
        score: DocumentScore,
        judgment: DocumentRelevanceJudgment,
        document: DocumentFeatures,
        query: QueryFeatures,
    ) -> DocumentTrainingData:
        inner_join(on=(judgment.search_query_id == score.query_id) & (judgment.document_id == score.document_id))
        inner_join(document, on=document.document_id == score.document_id)
        inner_join(query, on=query.query_id == score.query_id)
        return DocumentTrainingData(
            search_query_id=judgment.search_query_id,
            document_id=judgment.document_id,
            relevance_grade=judgment.relevance_grade,
            lexical_score=score.score,
            query_token_count=query.token_count,
            query_distinct_token_count=query.distinct_token_count,
            document_content_length=document.content_length,
            document_url_is_https=document.url_is_https,
        )
