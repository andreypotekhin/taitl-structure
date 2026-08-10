"""Rank corpus sections from indexed lexical-similarity pairs."""

from typing import Final

from examples.search.schemas.similarity import IndexedSimilarSection
from examples.search.schemas.similarity import SectionSimilarity as SectionSimilarityPair
from examples.search.schemas.text import Section
from structure import *
from structure.plugin.pyspark import *


class SimilarSections(Transform):
    """Return the top fixed number of corpus sections similar to one query section."""

    maximum_results: Final = 10

    query = input(Section)
    sections = input(Section)
    section_similarities = input(SectionSimilarityPair)
    ranked_sections = lane(IndexedSimilarSection)
    similar_sections = output(IndexedSimilarSection)

    def rank(
        self, query: Section, section: Section, pair: SectionSimilarityPair
    ) -> IndexedSimilarSection:
        inner_join(on=query.id == pair.left_section_id)
        candidate_id = pair.right_section_id
        score_bm25 = pair.bm25_left_to_right
        inner_join(on=section.id == candidate_id)
        return IndexedSimilarSection.base(section)(
            search_query_id=query.id,
            score_overlap=pair.score_overlap,
            score_bm25=score_bm25,
            rank=pair.rank,
        )

    def limit(self, candidate: IndexedSimilarSection) -> IndexedSimilarSection:
        where(candidate.rank <= self.maximum_results)
        return IndexedSimilarSection.project(candidate)
