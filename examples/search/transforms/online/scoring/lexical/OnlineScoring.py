"""Fill missing or stale document-search scores from reusable indexes."""

from examples.search.schemas.clicks import *
from examples.search.schemas.indexing.lexical.index import *
from examples.search.schemas.indexing.vector import *
from examples.search.schemas.scoring.overlap import *
from examples.search.schemas.search import *
from examples.search.transforms.online.scoring.lexical.SelectGapQueries import *
from examples.search.transforms.scoring import *
from structure import *


class OnlineScoring(Transform):
    """Calculate only score groups missing or stale in caller-supplied scores."""

    queries = input(SearchQuery, streaming=True)
    requests = input(SearchRequest, streaming=True)
    document_scores = input(DocumentScore)
    document_overlap_scores = input(DocumentOverlapScore)
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

    gap = SelectGapQueries(
        queries=queries,
        requests=requests,
        document_scores=document_scores,
        document_overlap_scores=document_overlap_scores,
        score_policy=score_policy,
    )

    scoring = Scoring(
        queries=gap.gap_queries,
        document_terms=document_terms,
        section_terms=section_terms,
        paragraph_terms=paragraph_terms,
        sentence_terms=sentence_terms,
        document_summary=document_summary,
        section_summary=section_summary,
        paragraph_summary=paragraph_summary,
        sentence_summary=sentence_summary,
        score_policy=score_policy,
        document_vector_queries=document_vector_queries,
        document_vector_index=document_vector_index,
        paragraph_vector_queries=paragraph_vector_queries,
        paragraph_vector_index=paragraph_vector_index,
        vector_policy=vector_policy,
    )

    online_document_scores = output(DocumentScore, scoring.document_scores)
    online_streamed_document_scores = output(DocumentScore, scoring.document_scores)
    online_section_scores = output(SectionScore, scoring.section_scores)
    online_paragraph_scores = output(ParagraphScore, scoring.paragraph_scores)
    online_sentence_scores = output(SentenceScore, scoring.sentence_scores)
    online_document_overlap_scores = output(DocumentOverlapScore, scoring.document_overlap_scores)
    online_section_overlap_scores = output(SectionOverlapScore, scoring.section_overlap_scores)
    online_paragraph_overlap_scores = output(ParagraphOverlapScore, scoring.paragraph_overlap_scores)
    online_sentence_overlap_scores = output(SentenceOverlapScore, scoring.sentence_overlap_scores)
    online_document_vector_scores = output(DocumentVectorScore, scoring.document_vector_scores)
    online_paragraph_vector_scores = output(ParagraphVectorScore, scoring.paragraph_vector_scores)
