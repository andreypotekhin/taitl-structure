"""Evaluation slice parameters."""

from examples.search.schemas.label import Label
from examples.search.schemas.search import SearchQuery
from structure import Schema
from structure.plugin.pyspark import *


class EvaluationParams(Schema):
    """Caller-defined labels and demographic band slice."""

    labels = array(struct(Label), contains_null=False, nullable=False)
    band_id = string(nullable=True)

    def matches_query(self, query: SearchQuery):
        """Query has every requested label value?"""

        return arr_forall(
            self.labels,
            lambda requested: arr_exists(
                self.labels,
                lambda candidate: (candidate.name == requested.name)
                & (element_at(query.labels, candidate.name) == candidate.value),
                argument_name="candidate",
            ),
            argument_name="requested",
        )

    def matches_band(self, band_id):
        """User belongs to the requested band, or global context if band_id is null"""

        return self.band_id.null_safe_eq(band_id)
