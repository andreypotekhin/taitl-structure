"""Offline Search training-data pipeline."""

from examples.search.schemas.evaluation.judged_quality import DocumentRelevanceJudgment
from examples.search.schemas.features import DocumentFeatures, QueryFeatures
from examples.search.schemas.search import DocumentScore, SearchQuery
from examples.search.schemas.text import Document
from examples.search.schemas.training import DocumentTrainingData
from examples.search.transforms.features import Features
from examples.search.transforms.training.BuildTrainingData import BuildTrainingData
from structure import Transform, input, output, stage


class Training(Transform):
    """Build reusable features and judged rows for offline ranking training."""

    documents = input(Document)
    queries = input(SearchQuery)
    document_scores = input(DocumentScore)
    judgments = input(DocumentRelevanceJudgment)

    features = stage(Features(documents=documents, queries=queries))
    data = stage(
        BuildTrainingData(
            document_scores=document_scores,
            judgments=judgments,
            document_features=features.document_features,
            query_features=features.query_features,
        )
    )

    document_features = output(DocumentFeatures).from_(features.document_features)
    query_features = output(QueryFeatures).from_(features.query_features)
    training_data = output(DocumentTrainingData).from_(data.training_data)
