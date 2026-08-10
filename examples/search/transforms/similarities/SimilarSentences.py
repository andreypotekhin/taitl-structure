"""Rank corpus sentences from indexed lexical-similarity pairs."""

from typing import Final

from examples.search.schemas.similarity import IndexedSimilarSentence
from examples.search.schemas.similarity import SentenceSimilarity as SentenceSimilarityPair
from examples.search.schemas.text import Sentence
from structure import *
from structure.plugin.pyspark import *


class SimilarSentences(Transform):
    """Return the top fixed number of corpus sentences similar to one query sentence."""

    maximum_results: Final = 10

    query = input(Sentence)
    sentences = input(Sentence)
    sentence_similarities = input(SentenceSimilarityPair)
    ranked_sentences = lane(IndexedSimilarSentence)
    similar_sentences = output(IndexedSimilarSentence)

    def rank(
        self, query: Sentence, sentence: Sentence, pair: SentenceSimilarityPair
    ) -> IndexedSimilarSentence:
        inner_join(on=query.id == pair.left_sentence_id)
        candidate_id = pair.right_sentence_id
        score_bm25 = pair.bm25_left_to_right
        inner_join(on=sentence.id == candidate_id)
        return IndexedSimilarSentence.base(sentence)(
            search_query_id=query.id,
            score_overlap=pair.score_overlap,
            score_bm25=score_bm25,
            rank=pair.rank,
        )

    def limit(self, candidate: IndexedSimilarSentence) -> IndexedSimilarSentence:
        where(candidate.rank <= self.maximum_results)
        return IndexedSimilarSentence.project(candidate)
