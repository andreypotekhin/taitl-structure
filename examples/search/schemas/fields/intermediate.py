"""Intermediate rows shared by field extraction and indexing."""

from structure import Schema
from structure.plugin.pyspark import *


class DocumentFieldEntry(Schema):
    """One string entry used to merge typed fields into the document map."""

    key = string(nullable=False)
    value = string(nullable=False)


class ExpandedDocumentField(Schema):
    """Internal map entry with its source-local ordinal."""

    ordinal = long(nullable=False)
    key = string(nullable=False)
    value = string(nullable=False)


class FieldText(Schema):
    """Internal field token before expansion."""

    term = string(nullable=False)


class ExpandedFieldText(Schema):
    """Internal field token with its field-local position."""

    position = long(nullable=False)
    term = string(nullable=False)
