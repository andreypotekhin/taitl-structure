"""Production lexical and vector scoring composition."""

from examples.search.schemas.indexing.lexical.index import *
from examples.search.schemas.indexing.vector import *
from examples.search.schemas.scoring.bm25 import *
from examples.search.schemas.scoring.overlap import *
from examples.search.schemas.search import *
from examples.search.transforms.scoring.lexical import *
from examples.search.transforms.scoring.vector import *
from structure import *


class Scoring(Transform):
    """Run lexical and vector scoring and expose their inspectable artifacts."""

    queries = input(SearchQuery, streaming=True)
    document_terms = input(DocumentTerm)
    section_terms = input(SectionTerm)
    paragraph_terms = input(ParagraphTerm)
    sentence_terms = input(SentenceTerm)
    document_summary = input(DocumentIndexSummary)
    section_summary = input(SectionIndexSummary)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_summary = input(SentenceIndexSummary)
    score_policy = input(ScorePolicy)
    document_vector_queries = input(DocumentVectorQuery, streaming=True)
    document_vector_index = input(DocumentVectorIndex)
    paragraph_vector_queries = input(ParagraphVectorQuery)
    paragraph_vector_index = input(ParagraphVectorIndex)
    vector_policy = input(VectorIndexPolicy)
    experiment_id = parameter(None)

    overlap = ScoreOverlap(
        queries=queries,
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
        queries=queries,
        document_terms=document_terms,
        section_terms=section_terms,
        paragraph_terms=paragraph_terms,
        sentence_terms=sentence_terms,
        document_summary=document_summary,
        section_summary=section_summary,
        paragraph_summary=paragraph_summary,
        sentence_summary=sentence_summary,
    )

    selected = SelectScores(
        document_overlap_scores=overlap.document_overlap_scores,
        section_overlap_scores=overlap.section_overlap_scores,
        paragraph_overlap_scores=overlap.paragraph_overlap_scores,
        sentence_overlap_scores=overlap.sentence_overlap_scores,
        document_bm25_scores=bm25.document_bm25_scores,
        section_bm25_scores=bm25.section_bm25_scores,
        paragraph_bm25_scores=bm25.paragraph_bm25_scores,
        sentence_bm25_scores=bm25.sentence_bm25_scores,
        score_policy=score_policy,
        experiment_id=experiment_id,
    )

    vector = ScoreVectors(
        policy=vector_policy,
        score_policy=score_policy,
        document_queries=document_vector_queries,
        document_index=document_vector_index,
        paragraph_queries=paragraph_vector_queries,
        paragraph_index=paragraph_vector_index,
    )

    document_scores = output(DocumentScore, selected.document_scores)
    section_scores = output(SectionScore, selected.section_scores)
    paragraph_scores = output(ParagraphScore, selected.paragraph_scores)
    sentence_scores = output(SentenceScore, selected.sentence_scores)
    document_overlap_scores = output(DocumentOverlapScore, overlap.document_overlap_scores)
    section_overlap_scores = output(SectionOverlapScore, overlap.section_overlap_scores)
    paragraph_overlap_scores = output(ParagraphOverlapScore, overlap.paragraph_overlap_scores)
    sentence_overlap_scores = output(SentenceOverlapScore, overlap.sentence_overlap_scores)
    document_bm25_scores = output(DocumentBm25Score, bm25.document_bm25_scores)
    section_bm25_scores = output(SectionBm25Score, bm25.section_bm25_scores)
    paragraph_bm25_scores = output(ParagraphBm25Score, bm25.paragraph_bm25_scores)
    sentence_bm25_scores = output(SentenceBm25Score, bm25.sentence_bm25_scores)
    document_vector_scores = output(DocumentVectorScore, vector.document_scores)
    paragraph_vector_scores = output(ParagraphVectorScore, vector.paragraph_scores)
