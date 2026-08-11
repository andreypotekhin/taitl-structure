"""Search similarity pipeline."""

from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexSummary,
    DocumentTerm,
    ParagraphIndexSummary,
    ParagraphTerm,
    SectionIndexSummary,
    SectionTerm,
    SentenceIndexSummary,
    SentenceTerm,
)
from examples.search.schemas.search import ScorePolicy
from examples.search.schemas.similarity import (
    DocumentSimilarity,
    ParagraphSimilarity,
    SectionSimilarity,
    SentenceSimilarity,
    SimilarityPolicy,
)
from examples.search.transforms.scoring.lexical.ScoreBm25 import ScoreBm25
from examples.search.transforms.scoring.lexical.ScoreOverlap import ScoreOverlap
from examples.search.transforms.similarities.CreateSimilarityQueries import CreateSimilarityQueries
from examples.search.transforms.similarities.ReduceSimilarityScores import ReduceSimilarityScores
from structure import Transform, input, output


class Similarities(Transform):
    """Create same-grain corpus similarity pairs from reusable lexical indexes."""

    policy = input(SimilarityPolicy)
    score_policy = input(ScorePolicy)
    document_terms = input(DocumentTerm)
    document_summary = input(DocumentIndexSummary)
    section_terms = input(SectionTerm)
    section_summary = input(SectionIndexSummary)
    paragraph_terms = input(ParagraphTerm)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_terms = input(SentenceTerm)
    sentence_summary = input(SentenceIndexSummary)
    document_similarities = output(DocumentSimilarity)
    section_similarities = output(SectionSimilarity)
    paragraph_similarities = output(ParagraphSimilarity)
    sentence_similarities = output(SentenceSimilarity)

    queries = CreateSimilarityQueries(
        policy=policy,
        document_terms=document_terms,
        document_summary=document_summary,
        section_terms=section_terms,
        section_summary=section_summary,
        paragraph_terms=paragraph_terms,
        paragraph_summary=paragraph_summary,
        sentence_terms=sentence_terms,
        sentence_summary=sentence_summary,
    )

    overlap = ScoreOverlap(
        queries=queries.queries,
        document_terms=document_terms,
        section_terms=section_terms,
        paragraph_terms=paragraph_terms,
        sentence_terms=sentence_terms,
        document_summary=document_summary,
        section_summary=section_summary,
        paragraph_summary=paragraph_summary,
        sentence_summary=sentence_summary,
        score_policy=score_policy,
    )

    bm25 = ScoreBm25(
        queries=queries.queries,
        document_terms=document_terms,
        document_summary=document_summary,
        section_terms=section_terms,
        section_summary=section_summary,
        paragraph_terms=paragraph_terms,
        paragraph_summary=paragraph_summary,
        sentence_terms=sentence_terms,
        sentence_summary=sentence_summary,
    )

    reduced = ReduceSimilarityScores(
        document_queries=queries.document_queries,
        section_queries=queries.section_queries,
        paragraph_queries=queries.paragraph_queries,
        sentence_queries=queries.sentence_queries,
        document_overlap_scores=overlap.document_overlap_scores,
        section_overlap_scores=overlap.section_overlap_scores,
        paragraph_overlap_scores=overlap.paragraph_overlap_scores,
        sentence_overlap_scores=overlap.sentence_overlap_scores,
        document_bm25_scores=bm25.document_bm25_scores,
        section_bm25_scores=bm25.section_bm25_scores,
        paragraph_bm25_scores=bm25.paragraph_bm25_scores,
        sentence_bm25_scores=bm25.sentence_bm25_scores,
    )
