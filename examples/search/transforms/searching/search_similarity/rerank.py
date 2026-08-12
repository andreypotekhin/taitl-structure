"""Rerank fused similarity candidates and present matching documents."""

from examples.search.schemas.similarities.vector import (
    DocumentFusedSimilarityCandidate,
    ParagraphFusedSimilarityCandidate,
)
from examples.search.schemas.similarity import (
    HybridIndexedSimilarDocument,
    HybridIndexedSimilarParagraph,
    SimilarityDocumentQuery,
    SimilarityFusionPolicy,
)
from examples.search.schemas.text import Document, Paragraph
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import inner_join, param_join, row_number, where


class RerankSimilarity(Transform):
    """Join adopted candidates to documents and apply deterministic final ranking."""

    query = input(SimilarityDocumentQuery)
    documents = input(Document)
    document_candidates = input(DocumentFusedSimilarityCandidate)
    policy = input(SimilarityFusionPolicy)
    ranked_documents = lane(HybridIndexedSimilarDocument)
    similar_documents = output(HybridIndexedSimilarDocument)

    @step(input=[document_candidates, query, documents], output=ranked_documents)
    def rank_documents(
        self,
        candidate: DocumentFusedSimilarityCandidate,
        query: SimilarityDocumentQuery,
        document: Document,
    ) -> HybridIndexedSimilarDocument:
        inner_join(query, on=query.id == candidate.left_document_id)
        inner_join(document, on=document.id == candidate.right_document_id)
        return HybridIndexedSimilarDocument.base(document)(
            search_query_id=query.id,
            score_overlap=candidate.score_overlap,
            score_bm25=candidate.bm25_left_to_right,
            lexical_rank=candidate.lexical_rank,
            vector_rank=candidate.vector_rank,
            vector_similarity=candidate.vector_similarity,
            rrf_k=candidate.rrf_k,
            rrf_score=candidate.rrf_score,
            vector_backend=candidate.vector_backend,
            vector_model_id=candidate.vector_model_id,
            vector_dimension=candidate.vector_dimension,
            vector_content_revision=candidate.vector_content_revision,
            experiment_id=candidate.experiment_id,
            rank=row_number(
                partition_by=candidate.left_document_id,
                order_by=(
                    candidate.rrf_score.desc_nulls_last(),
                    candidate.vector_similarity.desc_nulls_last(),
                    candidate.bm25_mean.desc_nulls_last(),
                    candidate.right_document_id.asc_nulls_first(),
                ),
            ),
        )

    @step(input=[ranked_documents, policy], output=similar_documents)
    def limit_documents(
        self, candidate: HybridIndexedSimilarDocument, policy: SimilarityFusionPolicy
    ) -> HybridIndexedSimilarDocument:
        param_join(policy)
        where(candidate.rank <= policy.maximum_results)
        return HybridIndexedSimilarDocument.project(candidate)


class RerankSimilarityParagraphs(Transform):
    """Join fused paragraph candidates to metadata and apply final ranking."""

    query = input(Paragraph)
    paragraphs = input(Paragraph)
    paragraph_candidates = input(ParagraphFusedSimilarityCandidate)
    policy = input(SimilarityFusionPolicy)
    ranked_paragraphs = lane(HybridIndexedSimilarParagraph)
    similar_paragraphs = output(HybridIndexedSimilarParagraph)

    @step(input=[paragraph_candidates, query, paragraphs], output=ranked_paragraphs)
    def rank_paragraphs(
        self,
        candidate: ParagraphFusedSimilarityCandidate,
        query: Paragraph,
        paragraph: Paragraph,
    ) -> HybridIndexedSimilarParagraph:
        inner_join(
            query,
            on=(query.id == candidate.left_paragraph_id)
            & (query.document_id == candidate.left_document_id)
            & (query.section_id == candidate.left_section_id),
        )
        inner_join(
            paragraph,
            on=(paragraph.id == candidate.right_paragraph_id)
            & (paragraph.document_id == candidate.right_document_id)
            & (paragraph.section_id == candidate.right_section_id),
        )
        return HybridIndexedSimilarParagraph.base(paragraph)(
            score_overlap=candidate.score_overlap,
            score_bm25=candidate.bm25_left_to_right,
            lexical_rank=candidate.lexical_rank,
            vector_rank=candidate.vector_rank,
            vector_similarity=candidate.vector_similarity,
            rrf_k=candidate.rrf_k,
            rrf_score=candidate.rrf_score,
            vector_backend=candidate.vector_backend,
            vector_model_id=candidate.vector_model_id,
            vector_dimension=candidate.vector_dimension,
            vector_content_revision=candidate.vector_content_revision,
            experiment_id=candidate.experiment_id,
            rank=row_number(
                partition_by=(
                    candidate.left_document_id,
                    candidate.left_section_id,
                    candidate.left_paragraph_id,
                ),
                order_by=(
                    candidate.rrf_score.desc_nulls_last(),
                    candidate.vector_similarity.desc_nulls_last(),
                    candidate.bm25_mean.desc_nulls_last(),
                    candidate.right_document_id.asc_nulls_first(),
                    candidate.right_section_id.asc_nulls_first(),
                    candidate.right_paragraph_id.asc_nulls_first(),
                ),
            ),
        )

    @step(input=[ranked_paragraphs, policy], output=similar_paragraphs)
    def limit_paragraphs(
        self, paragraph: HybridIndexedSimilarParagraph, policy: SimilarityFusionPolicy
    ) -> HybridIndexedSimilarParagraph:
        param_join(policy)
        where(paragraph.rank <= policy.maximum_results)
        return HybridIndexedSimilarParagraph.project(paragraph)
