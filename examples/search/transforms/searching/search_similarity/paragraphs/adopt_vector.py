"""Adopt ranked vector paragraph candidates into the hybrid similarity contract."""

from examples.search.schemas.indexing.vector import ParagraphVectorScore
from examples.search.schemas.similarities.vector import ParagraphFusedSimilarityCandidate
from examples.search.schemas.text import Paragraph
from structure import Transform, input, output, step
from structure.plugin.pyspark import inner_join, require_unique, row_number


class AdoptVectorSimilarity(Transform):
    """Translate exact vector paragraph results into retrieval candidates."""

    query = input(Paragraph)
    paragraphs = input(Paragraph)
    paragraph_scores = input(ParagraphVectorScore)
    adopted_paragraph_candidates = output(ParagraphFusedSimilarityCandidate)

    @step(input=[paragraph_scores, query, paragraphs], output=adopted_paragraph_candidates)
    def adopt_paragraphs(
        self, score: ParagraphVectorScore, query: Paragraph, paragraph: Paragraph
    ) -> ParagraphFusedSimilarityCandidate:
        inner_join(
            query,
            on=(query.id == score.query_paragraph_id)
            & (query.document_id == score.query_document_id)
            & (query.section_id == score.query_section_id),
        )
        inner_join(
            paragraph,
            on=(paragraph.id == score.paragraph_id)
            & (paragraph.document_id == score.document_id)
            & (paragraph.section_id == score.section_id),
        )
        require_unique(
            score.query_document_id,
            score.query_section_id,
            score.query_paragraph_id,
            score.document_id,
            score.section_id,
            score.paragraph_id,
        )
        return ParagraphFusedSimilarityCandidate(
            left_document_id=score.query_document_id,
            left_section_id=score.query_section_id,
            left_paragraph_id=score.query_paragraph_id,
            right_document_id=paragraph.document_id,
            right_section_id=paragraph.section_id,
            right_paragraph_id=paragraph.id,
            lexical_rank=None,
            vector_rank=row_number(
                partition_by=(score.query_document_id, score.query_section_id, score.query_paragraph_id),
                order_by=(
                    score.cosine_similarity.desc_nulls_last(),
                    paragraph.document_id.asc_nulls_first(),
                    paragraph.section_id.asc_nulls_first(),
                    paragraph.id.asc_nulls_first(),
                ),
            ),
            score_overlap=None,
            bm25_left_to_right=None,
            bm25_right_to_left=None,
            bm25_mean=None,
            vector_similarity=score.cosine_similarity,
            rrf_score=0.0,
            rrf_k=0,
            experiment_id="",
            vector_backend=score.vector_backend,
            vector_model_id=score.model_id,
            vector_dimension=score.dimension,
            vector_content_revision=score.content_revision,
        )
