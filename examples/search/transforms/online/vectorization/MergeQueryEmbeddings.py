"""Merge cached and newly inferred query embeddings for online search."""

from examples.search.schemas.indexing.vector import SearchQueryVectorEmbedding
from examples.search.schemas.inference import InferencePolicy
from structure import Transform, input, output, step
from structure.plugin.pyspark import drop_duplicates, param_join, union_all, where


class MergeQueryEmbeddings(Transform):
    cached = input(SearchQueryVectorEmbedding, streaming=True)
    inferred = input(SearchQueryVectorEmbedding, streaming=True)
    policy = input(InferencePolicy)
    embeddings = output(SearchQueryVectorEmbedding)

    @step(input=[cached, inferred, policy], output=embeddings)
    def merge(
        self,
        cached_embedding: SearchQueryVectorEmbedding,
        inferred_embedding: SearchQueryVectorEmbedding,
        policy: InferencePolicy,
    ) -> SearchQueryVectorEmbedding:
        merged = union_all(inferred_embedding)
        param_join(policy)
        where(
            (merged.model_id == policy.model_id)
            & (merged.dimension == policy.dimension)
            & (merged.content_revision == policy.content_revision)
            & (merged.experiment_id == policy.experiment_id)
        )
        drop_duplicates(merged.query_id)
        return SearchQueryVectorEmbedding.project(merged)
