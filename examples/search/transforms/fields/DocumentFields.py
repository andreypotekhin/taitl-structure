"""Reusable document-field map construction."""

from examples.search.schemas.fields.intermediate import DocumentFieldEntry
from examples.search.schemas.text import Document
from structure.plugin.pyspark import *


class DocumentFields:
    """Complete the searchable field map from typed and mapped document values."""

    @staticmethod
    def complete(document: Document):
        typed_fields = map_from_entries(
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
        )
        typed_fields = map_filter(
            typed_fields,
            lambda key, value: array_contains(array("source", "title", "content_type", "encoding", "language"), key)
            | ((key == "url") & document.url.is_not_null())
            | ((key == "document_type") & document.document_type.is_not_null())
            | ((key == "category_id") & document.category_id.is_not_null())
            | ((key == "file_type") & document.file_type.is_not_null()),
        )
        return map_concat(typed_fields, document.fields)
