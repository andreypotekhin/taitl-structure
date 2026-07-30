"""Search feature-engineering composition."""

from examples.search.schemas.features import DocumentFeatures, QueryFeatures
from examples.search.schemas.search import SearchQuery
from examples.search.schemas.text import Document
from examples.search.transforms.features.BuildDocumentFeatures import BuildDocumentFeatures
from examples.search.transforms.features.BuildQueryFeatures import BuildQueryFeatures
from structure import Transform, input, output, stage


class Features(Transform):
    """Build reusable document and query feature relations."""

    documents = input(Document)
    queries = input(SearchQuery)
    document_features = output(DocumentFeatures)
    query_features = output(QueryFeatures)

    documents_built = stage(BuildDocumentFeatures(documents=documents))
    queries_built = stage(BuildQueryFeatures(queries=queries))
