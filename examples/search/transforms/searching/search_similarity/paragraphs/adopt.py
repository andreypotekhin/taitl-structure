"""Adopt lexical and vector paragraph similarity candidates."""

from examples.search.algorithms.similarity.adapter import SimilarityCandidateAdapter
from examples.search.schemas.indexing.vector import ParagraphVectorCandidate
from examples.search.schemas.similarities.vector import ParagraphFusedSimilarityCandidate
from examples.search.schemas.similarity import ParagraphSimilarity
from examples.search.schemas.text import Paragraph
from structure import Transform, input, output, parameter, step
from structure.plugin.pyspark import inner_join, require_unique


class AdoptLexicalSimilarity(Transform):
    """Translate lexical paragraph similarity results into retrieval candidates."""

    paragraph_similarities = input(ParagraphSimilarity)
    paragraph_candidates = output(ParagraphFusedSimilarityCandidate)

    @step(input=paragraph_similarities, output=paragraph_candidates)
    def adopt_paragraphs(self, pair: ParagraphSimilarity) -> ParagraphFusedSimilarityCandidate:
        require_unique(
            pair.left_document_id,
            pair.left_section_id,
            pair.left_paragraph_id,
            pair.right_document_id,
            pair.right_section_id,
            pair.right_paragraph_id,
        )
        return ParagraphFusedSimilarityCandidate(
            left_document_id=pair.left_document_id,
            left_section_id=pair.left_section_id,
            left_paragraph_id=pair.left_paragraph_id,
            right_document_id=pair.right_document_id,
            right_section_id=pair.right_section_id,
            right_paragraph_id=pair.right_paragraph_id,
            lexical_rank=pair.rank,
            vector_rank=None,
            score_overlap=pair.score_overlap,
            bm25_left_to_right=pair.bm25_left_to_right,
            bm25_right_to_left=pair.bm25_right_to_left,
            bm25_mean=pair.bm25_mean,
            vector_similarity=None,
            rrf_score=0.0,
            rrf_k=0,
            experiment_id="",
            vector_backend=None,
            vector_model_id=None,
            vector_dimension=None,
            vector_content_revision=None,
        )


class AdoptVectorSimilarity(Transform):
    """Translate exact vector paragraph results into retrieval candidates."""

    query = input(Paragraph)
    paragraphs = input(Paragraph)
    paragraph_candidates = input(ParagraphVectorCandidate)
    adapter = parameter(SimilarityCandidateAdapter())
    adopted_paragraph_candidates = output(ParagraphFusedSimilarityCandidate)

    @step(input=[paragraph_candidates, query, paragraphs], output=adopted_paragraph_candidates)
    def adopt_paragraphs(
        self, candidate: ParagraphVectorCandidate, query: Paragraph, paragraph: Paragraph
    ) -> ParagraphFusedSimilarityCandidate:
        inner_join(
            query,
            on=(query.id == candidate.query_paragraph_id)
            & (query.document_id == candidate.query_document_id)
            & (query.section_id == candidate.query_section_id),
        )
        inner_join(
            paragraph,
            on=(paragraph.id == candidate.paragraph_id)
            & (paragraph.document_id == candidate.document_id)
            & (paragraph.section_id == candidate.section_id),
        )
        require_unique(
            candidate.query_document_id,
            candidate.query_section_id,
            candidate.query_paragraph_id,
            candidate.document_id,
            candidate.section_id,
            candidate.paragraph_id,
        )
        return self.adapter.paragraph(candidate, query, paragraph)
