"""Build reusable Search document feature relations."""

from examples.search.schemas.features import DocumentFeatures
from examples.search.schemas.text import Document
from structure import Transform, input, output, step
from structure.plugin.pyspark import length, lower, regexp_replace, trim


class BuildDocumentFeatures(Transform):
    """Build stable document features from caller-owned corpus metadata and text."""

    documents = input(Document)
    document_features = output(DocumentFeatures)

    @step(input=documents, output=document_features)
    def build(self, document: Document) -> DocumentFeatures:
        return DocumentFeatures(
            document_id=document.id,
            collection_id=document.collection_id,
            source=document.source,
            language=document.language,
            normalized_title=lower(trim(document.title)),
            normalized_content=lower(regexp_replace(trim(document.content), pattern=r"\s+", replacement=" ")),
            title_length=length(document.title),
            content_length=length(document.content),
            url_is_https=document.url.startswith("https://"),
        )
