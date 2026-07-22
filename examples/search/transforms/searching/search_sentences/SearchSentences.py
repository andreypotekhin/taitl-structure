"""Ranked sentence-search presentation."""

from examples.search.schemas.search import SearchQuery, SentenceSearchResult
from examples.search.schemas.text import Sentence
from structure import Transform, input, output
from structure.plugin.pyspark import inner_join, row_number, where


class SearchSentences(Transform):
    """Rank pre-scored sentence matches for caller-supplied queries."""

    queries = input(SearchQuery)
    scored_sentences = input(Sentence)
    results = output(SentenceSearchResult)

    def rank_sentences(self, sentence: Sentence, query: SearchQuery) -> SentenceSearchResult:
        inner_join(query, on=query.id == sentence.search_query_id)
        where(
            sentence.search_query_id.is_not_null(),
            sentence.score_overlap.is_not_null(),
            sentence.score_bm25.is_not_null(),
        )
        return SentenceSearchResult.base(sentence)(
            search_query_id=sentence.search_query_id,
            rank=row_number(
                partition_by=sentence.search_query_id,
                order_by=(
                    sentence.score_bm25.desc_nulls_last(),
                    sentence.score_overlap.desc_nulls_last(),
                    sentence.document_id.asc_nulls_first(),
                    sentence.id.asc_nulls_first(),
                ),
            ),
            sentence_id=sentence.id,
        )
