"""Offline Search training-data pipeline."""

from examples.search.schemas.evaluation.judged_quality import DocumentRelevanceJudgment
from examples.search.schemas.features import DocumentFeatures, QueryFeatures
from examples.search.schemas.search import DocumentScore, SearchQuery
from examples.search.schemas.text import Document
from examples.search.schemas.training import DocumentTrainingData
from examples.search.transforms.features import Features
from examples.search.transforms.training.BuildTrainingData import BuildTrainingData
from structure import Transform, input, output


class Training(Transform):
    """Build reusable features and judged rows for offline ranking training."""

    documents = input(Document)
    queries = input(SearchQuery)
    document_scores = input(DocumentScore)
    judgments = input(DocumentRelevanceJudgment)

    features = Features(documents=documents, queries=queries)

    data = BuildTrainingData(
        document_scores=document_scores,
        judgments=judgments,
        document_features=features.document_features,
        query_features=features.query_features,
    )

    document_features = output(DocumentFeatures, features.document_features)
    query_features = output(QueryFeatures, features.query_features)
    training_data = output(DocumentTrainingData, data.training_data)
