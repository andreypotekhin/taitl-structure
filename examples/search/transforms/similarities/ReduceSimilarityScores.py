"""Reduce directed index scores into reciprocal canonical similarity pairs."""

from typing import Final

from examples.search.schemas.scoring.bm25 import (
    DocumentBm25Score,
    ParagraphBm25Score,
    SectionBm25Score,
    SentenceBm25Score,
)
from examples.search.schemas.scoring.overlap import (
    DocumentOverlapScore,
    ParagraphOverlapScore,
    SectionOverlapScore,
    SentenceOverlapScore,
)
from examples.search.schemas.similarities.intermediate import (
    DocumentSimilarityCandidate,
    DocumentSimilarityPair,
    ParagraphSimilarityCandidate,
    ParagraphSimilarityPair,
    SectionSimilarityCandidate,
    SectionSimilarityPair,
    SentenceSimilarityCandidate,
    SentenceSimilarityPair,
)
from examples.search.schemas.similarity import (
    DocumentSimilarity,
    DocumentSimilarityQuery,
    ParagraphSimilarity,
    ParagraphSimilarityQuery,
    SectionSimilarity,
    SectionSimilarityQuery,
    SentenceSimilarity,
    SentenceSimilarityQuery,
)
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import inner_join, relation_alias, row_number, union_all, when, where


class ReduceSimilarityScores(Transform):
    """Emit the top same-grain candidates for every source target."""

    maximum_results: Final = 10

    document_queries = input(DocumentSimilarityQuery)
    section_queries = input(SectionSimilarityQuery)
    paragraph_queries = input(ParagraphSimilarityQuery)
    sentence_queries = input(SentenceSimilarityQuery)
    document_overlap_scores = input(DocumentOverlapScore)
    section_overlap_scores = input(SectionOverlapScore)
    paragraph_overlap_scores = input(ParagraphOverlapScore)
    sentence_overlap_scores = input(SentenceOverlapScore)
    document_bm25_scores = input(DocumentBm25Score)
    section_bm25_scores = input(SectionBm25Score)
    paragraph_bm25_scores = input(ParagraphBm25Score)
    sentence_bm25_scores = input(SentenceBm25Score)
    document_similarities = output(DocumentSimilarity)
    section_similarities = output(SectionSimilarity)
    paragraph_similarities = output(ParagraphSimilarity)
    sentence_similarities = output(SentenceSimilarity)

    document_candidates = lane(DocumentSimilarityCandidate)
    document_canonical_pairs = lane(DocumentSimilarityPair)
    document_reversed_pairs = lane(DocumentSimilarityPair)
    document_pairs = lane(DocumentSimilarityPair)
    ranked_document_pairs = lane(DocumentSimilarity)
    section_candidates = lane(SectionSimilarityCandidate)
    section_canonical_pairs = lane(SectionSimilarityPair)
    section_reversed_pairs = lane(SectionSimilarityPair)
    section_pairs = lane(SectionSimilarityPair)
    ranked_section_pairs = lane(SectionSimilarity)
    paragraph_candidates = lane(ParagraphSimilarityCandidate)
    paragraph_canonical_pairs = lane(ParagraphSimilarityPair)
    paragraph_reversed_pairs = lane(ParagraphSimilarityPair)
    paragraph_pairs = lane(ParagraphSimilarityPair)
    ranked_paragraph_pairs = lane(ParagraphSimilarity)
    sentence_candidates = lane(SentenceSimilarityCandidate)
    sentence_canonical_pairs = lane(SentenceSimilarityPair)
    sentence_reversed_pairs = lane(SentenceSimilarityPair)
    sentence_pairs = lane(SentenceSimilarityPair)
    ranked_sentence_pairs = lane(SentenceSimilarity)

    @step(
        input=[document_overlap_scores, document_bm25_scores, document_queries],
        output=document_candidates,
    )
    def build_document_candidates(
        self, overlap: DocumentOverlapScore, bm25: DocumentBm25Score, query: DocumentSimilarityQuery
    ) -> DocumentSimilarityCandidate:
        inner_join(on=(bm25.query_id == overlap.query_id) & (bm25.document_id == overlap.document_id))
        inner_join(query, on=query.query_id == overlap.query_id)
        return DocumentSimilarityCandidate(
            left_document_id=query.document_id,
            right_document_id=overlap.document_id,
            score_overlap=overlap.score_overlap,
            bm25_left_to_right=bm25.score_bm25,
        )

    @step(input=document_candidates, output=document_canonical_pairs)
    def canonical_document_pairs(self, pair: DocumentSimilarityCandidate) -> DocumentSimilarityPair:
        reverse = relation_alias(pair, name="reverse_document")
        where(pair.left_document_id < pair.right_document_id)
        inner_join(
            reverse,
            on=(pair.left_document_id == reverse.right_document_id)
            & (pair.right_document_id == reverse.left_document_id),
        )
        return DocumentSimilarityPair(
            left_document_id=pair.left_document_id,
            right_document_id=pair.right_document_id,
            score_overlap=when(pair.score_overlap <= reverse.score_overlap, pair.score_overlap).otherwise(
                reverse.score_overlap
            ),
            bm25_left_to_right=pair.bm25_left_to_right,
            bm25_right_to_left=reverse.bm25_left_to_right,
            bm25_mean=(pair.bm25_left_to_right + reverse.bm25_left_to_right) / 2.0,
        )

    @step(input=document_canonical_pairs, output=document_reversed_pairs)
    def reverse_document_pairs(self, pair: DocumentSimilarityPair) -> DocumentSimilarityPair:
        return DocumentSimilarityPair(
            left_document_id=pair.right_document_id,
            right_document_id=pair.left_document_id,
            score_overlap=pair.score_overlap,
            bm25_left_to_right=pair.bm25_right_to_left,
            bm25_right_to_left=pair.bm25_left_to_right,
            bm25_mean=pair.bm25_mean,
        )

    @step(input=[document_canonical_pairs, document_reversed_pairs], output=document_pairs)
    def merge_document_pairs(self, pair: DocumentSimilarityPair, reversed_pair: DocumentSimilarityPair) -> DocumentSimilarityPair:
        merged = union_all(reversed_pair)
        return DocumentSimilarityPair.project(merged)

    @step(input=document_pairs, output=ranked_document_pairs)
    def rank_document_pairs(self, pair: DocumentSimilarityPair) -> DocumentSimilarity:
        return DocumentSimilarity.project(pair)(
            rank=row_number(
                partition_by=pair.left_document_id,
                order_by=(
                    pair.bm25_left_to_right.desc_nulls_last(),
                    pair.score_overlap.desc_nulls_last(),
                    pair.right_document_id.asc_nulls_first(),
                ),
            )
        )

    @step(input=ranked_document_pairs, output=document_similarities)
    def publish_document_pairs(self, pair: DocumentSimilarity) -> DocumentSimilarity:
        where(pair.rank <= self.maximum_results)
        return DocumentSimilarity.project(pair)

    @step(
        input=[section_overlap_scores, section_bm25_scores, section_queries],
        output=section_candidates,
    )
    def build_section_candidates(
        self, overlap: SectionOverlapScore, bm25: SectionBm25Score, query: SectionSimilarityQuery
    ) -> SectionSimilarityCandidate:
        inner_join(
            on=(bm25.query_id == overlap.query_id)
            & (bm25.document_id == overlap.document_id)
            & (bm25.section_id == overlap.section_id)
        )
        inner_join(query, on=query.query_id == overlap.query_id)
        return SectionSimilarityCandidate(
            left_document_id=query.document_id,
            left_section_id=query.section_id,
            right_document_id=overlap.document_id,
            right_section_id=overlap.section_id,
            score_overlap=overlap.score_overlap,
            bm25_left_to_right=bm25.score_bm25,
        )

    @step(input=section_candidates, output=section_canonical_pairs)
    def canonical_section_pairs(self, pair: SectionSimilarityCandidate) -> SectionSimilarityPair:
        reverse = relation_alias(pair, name="reverse_section")
        where(pair.left_section_id < pair.right_section_id)
        inner_join(
            reverse,
            on=(pair.left_document_id == reverse.right_document_id)
            & (pair.left_section_id == reverse.right_section_id)
            & (pair.right_document_id == reverse.left_document_id)
            & (pair.right_section_id == reverse.left_section_id),
        )
        return SectionSimilarityPair(
            left_document_id=pair.left_document_id,
            left_section_id=pair.left_section_id,
            right_document_id=pair.right_document_id,
            right_section_id=pair.right_section_id,
            score_overlap=when(pair.score_overlap <= reverse.score_overlap, pair.score_overlap).otherwise(
                reverse.score_overlap
            ),
            bm25_left_to_right=pair.bm25_left_to_right,
            bm25_right_to_left=reverse.bm25_left_to_right,
            bm25_mean=(pair.bm25_left_to_right + reverse.bm25_left_to_right) / 2.0,
        )

    @step(input=section_canonical_pairs, output=section_reversed_pairs)
    def reverse_section_pairs(self, pair: SectionSimilarityPair) -> SectionSimilarityPair:
        return SectionSimilarityPair(
            left_document_id=pair.right_document_id,
            left_section_id=pair.right_section_id,
            right_document_id=pair.left_document_id,
            right_section_id=pair.left_section_id,
            score_overlap=pair.score_overlap,
            bm25_left_to_right=pair.bm25_right_to_left,
            bm25_right_to_left=pair.bm25_left_to_right,
            bm25_mean=pair.bm25_mean,
        )

    @step(input=[section_canonical_pairs, section_reversed_pairs], output=section_pairs)
    def merge_section_pairs(self, pair: SectionSimilarityPair, reversed_pair: SectionSimilarityPair) -> SectionSimilarityPair:
        merged = union_all(reversed_pair)
        return SectionSimilarityPair.project(merged)

    @step(input=section_pairs, output=ranked_section_pairs)
    def rank_section_pairs(self, pair: SectionSimilarityPair) -> SectionSimilarity:
        return SectionSimilarity.project(pair)(
            rank=row_number(
                partition_by=(pair.left_document_id, pair.left_section_id),
                order_by=(
                    pair.bm25_left_to_right.desc_nulls_last(),
                    pair.score_overlap.desc_nulls_last(),
                    pair.right_document_id.asc_nulls_first(),
                    pair.right_section_id.asc_nulls_first(),
                ),
            )
        )

    @step(input=ranked_section_pairs, output=section_similarities)
    def publish_section_pairs(self, pair: SectionSimilarity) -> SectionSimilarity:
        where(pair.rank <= self.maximum_results)
        return SectionSimilarity.project(pair)

    @step(
        input=[paragraph_overlap_scores, paragraph_bm25_scores, paragraph_queries],
        output=paragraph_candidates,
    )
    def build_paragraph_candidates(
        self, overlap: ParagraphOverlapScore, bm25: ParagraphBm25Score, query: ParagraphSimilarityQuery
    ) -> ParagraphSimilarityCandidate:
        inner_join(
            on=(bm25.query_id == overlap.query_id)
            & (bm25.document_id == overlap.document_id)
            & (bm25.section_id == overlap.section_id)
            & (bm25.paragraph_id == overlap.paragraph_id)
        )
        inner_join(query, on=query.query_id == overlap.query_id)
        return ParagraphSimilarityCandidate(
            left_document_id=query.document_id,
            left_section_id=query.section_id,
            left_paragraph_id=query.paragraph_id,
            right_document_id=overlap.document_id,
            right_section_id=overlap.section_id,
            right_paragraph_id=overlap.paragraph_id,
            score_overlap=overlap.score_overlap,
            bm25_left_to_right=bm25.score_bm25,
        )

    @step(input=paragraph_candidates, output=paragraph_canonical_pairs)
    def canonical_paragraph_pairs(self, pair: ParagraphSimilarityCandidate) -> ParagraphSimilarityPair:
        reverse = relation_alias(pair, name="reverse_paragraph")
        where(pair.left_paragraph_id < pair.right_paragraph_id)
        inner_join(
            reverse,
            on=(pair.left_document_id == reverse.right_document_id)
            & (pair.left_section_id == reverse.right_section_id)
            & (pair.left_paragraph_id == reverse.right_paragraph_id)
            & (pair.right_document_id == reverse.left_document_id)
            & (pair.right_section_id == reverse.left_section_id)
            & (pair.right_paragraph_id == reverse.left_paragraph_id),
        )
        return ParagraphSimilarityPair(
            left_document_id=pair.left_document_id,
            left_section_id=pair.left_section_id,
            left_paragraph_id=pair.left_paragraph_id,
            right_document_id=pair.right_document_id,
            right_section_id=pair.right_section_id,
            right_paragraph_id=pair.right_paragraph_id,
            score_overlap=when(pair.score_overlap <= reverse.score_overlap, pair.score_overlap).otherwise(
                reverse.score_overlap
            ),
            bm25_left_to_right=pair.bm25_left_to_right,
            bm25_right_to_left=reverse.bm25_left_to_right,
            bm25_mean=(pair.bm25_left_to_right + reverse.bm25_left_to_right) / 2.0,
        )

    @step(input=paragraph_canonical_pairs, output=paragraph_reversed_pairs)
    def reverse_paragraph_pairs(self, pair: ParagraphSimilarityPair) -> ParagraphSimilarityPair:
        return ParagraphSimilarityPair(
            left_document_id=pair.right_document_id,
            left_section_id=pair.right_section_id,
            left_paragraph_id=pair.right_paragraph_id,
            right_document_id=pair.left_document_id,
            right_section_id=pair.left_section_id,
            right_paragraph_id=pair.left_paragraph_id,
            score_overlap=pair.score_overlap,
            bm25_left_to_right=pair.bm25_right_to_left,
            bm25_right_to_left=pair.bm25_left_to_right,
            bm25_mean=pair.bm25_mean,
        )

    @step(input=[paragraph_canonical_pairs, paragraph_reversed_pairs], output=paragraph_pairs)
    def merge_paragraph_pairs(
        self, pair: ParagraphSimilarityPair, reversed_pair: ParagraphSimilarityPair
    ) -> ParagraphSimilarityPair:
        merged = union_all(reversed_pair)
        return ParagraphSimilarityPair.project(merged)

    @step(input=paragraph_pairs, output=ranked_paragraph_pairs)
    def rank_paragraph_pairs(self, pair: ParagraphSimilarityPair) -> ParagraphSimilarity:
        return ParagraphSimilarity.project(pair)(
            rank=row_number(
                partition_by=(pair.left_document_id, pair.left_section_id, pair.left_paragraph_id),
                order_by=(
                    pair.bm25_left_to_right.desc_nulls_last(),
                    pair.score_overlap.desc_nulls_last(),
                    pair.right_document_id.asc_nulls_first(),
                    pair.right_section_id.asc_nulls_first(),
                    pair.right_paragraph_id.asc_nulls_first(),
                ),
            )
        )

    @step(input=ranked_paragraph_pairs, output=paragraph_similarities)
    def publish_paragraph_pairs(self, pair: ParagraphSimilarity) -> ParagraphSimilarity:
        where(pair.rank <= self.maximum_results)
        return ParagraphSimilarity.project(pair)

    @step(
        input=[sentence_overlap_scores, sentence_bm25_scores, sentence_queries],
        output=sentence_candidates,
    )
    def build_sentence_candidates(
        self, overlap: SentenceOverlapScore, bm25: SentenceBm25Score, query: SentenceSimilarityQuery
    ) -> SentenceSimilarityCandidate:
        inner_join(
            on=(bm25.query_id == overlap.query_id)
            & (bm25.document_id == overlap.document_id)
            & (bm25.section_id == overlap.section_id)
            & (bm25.paragraph_id == overlap.paragraph_id)
            & (bm25.sentence_id == overlap.sentence_id)
        )
        inner_join(query, on=query.query_id == overlap.query_id)
        return SentenceSimilarityCandidate(
            left_document_id=query.document_id,
            left_section_id=query.section_id,
            left_paragraph_id=query.paragraph_id,
            left_sentence_id=query.sentence_id,
            right_document_id=overlap.document_id,
            right_section_id=overlap.section_id,
            right_paragraph_id=overlap.paragraph_id,
            right_sentence_id=overlap.sentence_id,
            score_overlap=overlap.score_overlap,
            bm25_left_to_right=bm25.score_bm25,
        )

    @step(input=sentence_candidates, output=sentence_canonical_pairs)
    def canonical_sentence_pairs(self, pair: SentenceSimilarityCandidate) -> SentenceSimilarityPair:
        reverse = relation_alias(pair, name="reverse_sentence")
        where(pair.left_sentence_id < pair.right_sentence_id)
        inner_join(
            reverse,
            on=(pair.left_document_id == reverse.right_document_id)
            & (pair.left_section_id == reverse.right_section_id)
            & (pair.left_paragraph_id == reverse.right_paragraph_id)
            & (pair.left_sentence_id == reverse.right_sentence_id)
            & (pair.right_document_id == reverse.left_document_id)
            & (pair.right_section_id == reverse.left_section_id)
            & (pair.right_paragraph_id == reverse.left_paragraph_id)
            & (pair.right_sentence_id == reverse.left_sentence_id),
        )
        return SentenceSimilarityPair(
            left_document_id=pair.left_document_id,
            left_section_id=pair.left_section_id,
            left_paragraph_id=pair.left_paragraph_id,
            left_sentence_id=pair.left_sentence_id,
            right_document_id=pair.right_document_id,
            right_section_id=pair.right_section_id,
            right_paragraph_id=pair.right_paragraph_id,
            right_sentence_id=pair.right_sentence_id,
            score_overlap=when(pair.score_overlap <= reverse.score_overlap, pair.score_overlap).otherwise(
                reverse.score_overlap
            ),
            bm25_left_to_right=pair.bm25_left_to_right,
            bm25_right_to_left=reverse.bm25_left_to_right,
            bm25_mean=(pair.bm25_left_to_right + reverse.bm25_left_to_right) / 2.0,
        )

    @step(input=sentence_canonical_pairs, output=sentence_reversed_pairs)
    def reverse_sentence_pairs(self, pair: SentenceSimilarityPair) -> SentenceSimilarityPair:
        return SentenceSimilarityPair(
            left_document_id=pair.right_document_id,
            left_section_id=pair.right_section_id,
            left_paragraph_id=pair.right_paragraph_id,
            left_sentence_id=pair.right_sentence_id,
            right_document_id=pair.left_document_id,
            right_section_id=pair.left_section_id,
            right_paragraph_id=pair.left_paragraph_id,
            right_sentence_id=pair.left_sentence_id,
            score_overlap=pair.score_overlap,
            bm25_left_to_right=pair.bm25_right_to_left,
            bm25_right_to_left=pair.bm25_left_to_right,
            bm25_mean=pair.bm25_mean,
        )

    @step(input=[sentence_canonical_pairs, sentence_reversed_pairs], output=sentence_pairs)
    def merge_sentence_pairs(self, pair: SentenceSimilarityPair, reversed_pair: SentenceSimilarityPair) -> SentenceSimilarityPair:
        merged = union_all(reversed_pair)
        return SentenceSimilarityPair.project(merged)

    @step(input=sentence_pairs, output=ranked_sentence_pairs)
    def rank_sentence_pairs(self, pair: SentenceSimilarityPair) -> SentenceSimilarity:
        return SentenceSimilarity.project(pair)(
            rank=row_number(
                partition_by=(
                    pair.left_document_id,
                    pair.left_section_id,
                    pair.left_paragraph_id,
                    pair.left_sentence_id,
                ),
                order_by=(
                    pair.bm25_left_to_right.desc_nulls_last(),
                    pair.score_overlap.desc_nulls_last(),
                    pair.right_document_id.asc_nulls_first(),
                    pair.right_section_id.asc_nulls_first(),
                    pair.right_paragraph_id.asc_nulls_first(),
                    pair.right_sentence_id.asc_nulls_first(),
                ),
            )
        )

    @step(input=ranked_sentence_pairs, output=sentence_similarities)
    def publish_sentence_pairs(self, pair: SentenceSimilarity) -> SentenceSimilarity:
        where(pair.rank <= self.maximum_results)
        return SentenceSimilarity.project(pair)
