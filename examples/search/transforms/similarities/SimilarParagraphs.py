"""Rank corpus paragraphs from indexed lexical-similarity pairs."""

from typing import Final

from examples.search.schemas.similarity import IndexedSimilarParagraph
from examples.search.schemas.similarity import ParagraphSimilarity as ParagraphSimilarityPair
from examples.search.schemas.similarity import SimilarityParagraphQuery
from examples.search.schemas.text import Paragraph
from structure import *
from structure.plugin.pyspark import *


class SimilarParagraphs(Transform):
    """Return the top fixed number of corpus paragraphs similar to one query paragraph."""

    maximum_results: Final = 10

    query = input(SimilarityParagraphQuery)
    paragraphs = input(Paragraph)
    paragraph_similarities = input(ParagraphSimilarityPair)
    ranked_paragraphs = lane(IndexedSimilarParagraph)
    similar_paragraphs = output(IndexedSimilarParagraph)

    def rank(
        self, query: SimilarityParagraphQuery, paragraph: Paragraph, pair: ParagraphSimilarityPair
    ) -> IndexedSimilarParagraph:
        inner_join(on=query.id == pair.left_paragraph_id)
        candidate_id = pair.right_paragraph_id
        score_bm25 = pair.bm25_left_to_right
        inner_join(on=paragraph.id == candidate_id)
        return IndexedSimilarParagraph.base(paragraph)(
            search_query_id=query.id,
            score_overlap=pair.score_overlap,
            score_bm25=score_bm25,
            rank=pair.rank,
        )

    def limit(self, candidate: IndexedSimilarParagraph) -> IndexedSimilarParagraph:
        where(candidate.rank <= self.maximum_results)
        return IndexedSimilarParagraph.project(candidate)
