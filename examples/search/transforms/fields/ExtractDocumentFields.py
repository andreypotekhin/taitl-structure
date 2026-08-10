"""Extract searchable fields and flatten them for metadata indexing."""

from examples.search.schemas.fields import *
from examples.search.schemas.fields.intermediate import ExpandedDocumentField
from examples.search.schemas.text import *
from examples.search.transforms.fields.DocumentFields import DocumentFields
from structure import *
from structure.plugin.pyspark import *


class ExtractDocumentFields(Transform):
    """Preserve typed document fields, fill the map, and expose flat field rows."""

    source_documents = input(Document)
    documents = output(Document)
    document_fields = output(DocumentField)

    @step(input=source_documents, output=documents)
    def extract(self, document: Document) -> Document:
        fields = DocumentFields.complete(document)
        return Document.project(document)(
            source=coalesce(element_at(fields, "source"), document.source),
            title=coalesce(element_at(fields, "title"), document.title),
            url=element_at(fields, "url"),
            content_type=coalesce(element_at(fields, "content_type"), document.content_type),
            encoding=coalesce(element_at(fields, "encoding"), document.encoding),
            language=coalesce(element_at(fields, "language"), document.language),
            document_type=element_at(fields, "document_type"),
            category_id=element_at(fields, "category_id"),
            file_type=element_at(fields, "file_type"),
            fields=fields,
        )

    @step(input=source_documents, output=document_fields)
    def flatten(self, document: Document) -> DocumentField:
        field = posexplode_struct(
            map_entries(document.fields),
            as_=ExpandedDocumentField,
            ordinal="ordinal",
            scope="document_field",
        )
        where(trim(field.key) != "")
        return DocumentField(
            document_id=document.id,
            field_name=lower(trim(field.key)),
            field_value=trim(field.value),
            field_kind="text",
            analyzer_policy="metadata_text_v1",
            ordinal=field.ordinal,
        )


__all__ = ["ExtractDocumentFields"]
