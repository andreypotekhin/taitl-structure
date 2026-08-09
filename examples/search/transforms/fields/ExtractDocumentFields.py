"""Extract searchable fields and flatten them for metadata indexing."""

from examples.search.schemas.fields import *
from examples.search.schemas.text import *
from structure import *
from structure.plugin.pyspark import *


class ExpandedDocumentField(Schema):
    """Internal map entry with its source-local ordinal."""

    ordinal = long(nullable=False)
    key = string(nullable=False)
    value = string(nullable=False)


class DocumentFieldEntry(Schema):
    """One string entry used to merge typed fields into the document map."""

    key = string(nullable=False)
    value = string(nullable=False)


class ExtractDocumentFields(Transform):
    """Preserve typed document fields, fill the map, and expose flat field rows."""

    source_documents = input(Document)
    documents = output(Document)
    document_fields = output(DocumentField)

    @step(input=source_documents, output=documents)
    def extract(self, document: Document) -> Document:
        typed_fields = map_filter(
            map_from_entries(
                array(
                    DocumentFieldEntry(key="source", value=document.source),
                    DocumentFieldEntry(key="title", value=document.title),
                    DocumentFieldEntry(key="url", value=coalesce(document.url, "")),
                    DocumentFieldEntry(key="content_type", value=document.content_type),
                    DocumentFieldEntry(key="encoding", value=document.encoding),
                    DocumentFieldEntry(key="language", value=document.language),
                    DocumentFieldEntry(key="document_type", value=coalesce(document.document_type, "")),
                    DocumentFieldEntry(key="category_id", value=coalesce(document.category_id, "")),
                    DocumentFieldEntry(key="file_type", value=coalesce(document.file_type, "")),
                )
            ),
            lambda key, value: (
                array_contains(
                    array("source", "title", "content_type", "encoding", "language"),
                    key,
                )
                | ((key == "url") & document.url.is_not_null())
                | ((key == "document_type") & document.document_type.is_not_null())
                | ((key == "category_id") & document.category_id.is_not_null())
                | ((key == "file_type") & document.file_type.is_not_null())
            ),
        )
        fields = map_concat(typed_fields, document.fields)
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
