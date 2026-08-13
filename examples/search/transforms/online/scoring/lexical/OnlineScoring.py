"""Fill missing or stale document-search scores from reusable indexes."""

from examples.search.schemas.clicks import *
from examples.search.schemas.indexing.lexical.index import *
from examples.search.schemas.indexing.vector import *
from examples.search.schemas.scoring.overlap import *
from examples.search.schemas.search import *
from examples.search.transforms.online.scoring.lexical.merge_scores import MergeDocumentScores
from examples.search.transforms.online.scoring.lexical.MergeDocumentVectorScores import MergeDocumentVectorScores
from examples.search.transforms.online.scoring.lexical.MergeParagraphVectorScores import MergeParagraphVectorScores
from examples.search.transforms.online.scoring.lexical.SelectGapQueries import *
from examples.search.transforms.scoring import *
from structure import *


class OnlineScoring(Transform):
    """Calculate only score groups missing or stale in caller-supplied scores."""

    queries = input(SearchQuery, streaming=True)
    requests = input(SearchRequest, streaming=True)
    prefilter_targets = input(DocumentSearchTarget, streaming=True)
    cached_document_scores = input(DocumentScore)
    streamed_document_scores = input(DocumentScore, streaming=True)
    cached_document_overlap_scores = input(DocumentOverlapScore)
    cached_document_vector_scores = input(DocumentVectorScore)
    cached_paragraph_vector_scores = input(ParagraphVectorScore)
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
        document_scores=cached_document_scores,
        document_overlap_scores=cached_document_overlap_scores,
        document_vector_scores=cached_document_vector_scores,
        paragraph_vector_scores=cached_paragraph_vector_scores,
        prefilter_targets=prefilter_targets,
        score_policy=score_policy,
        vector_policy=vector_policy,
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
        targets=prefilter_targets,
    )

    merged = MergeDocumentScores(
        document_scores=cached_document_scores,
        streamed_document_scores=streamed_document_scores,
        online_document_scores=scoring.document_scores,
        requests=requests,
        prefilter_targets=prefilter_targets,
        score_policy=score_policy,
    )

    merged_vectors = MergeDocumentVectorScores(
        document_vector_scores=cached_document_vector_scores,
        online_document_vector_scores=scoring.document_vector_scores,
        invalidated_queries=gap.gap_queries,
        requests=requests,
        prefilter_targets=prefilter_targets,
        score_policy=score_policy,
        vector_policy=vector_policy,
    )

    merged_paragraph_vectors = MergeParagraphVectorScores(
        paragraph_vector_scores=cached_paragraph_vector_scores,
        online_paragraph_vector_scores=scoring.paragraph_vector_scores,
        invalidated_queries=gap.gap_queries,
        requests=requests,
        prefilter_targets=prefilter_targets,
        score_policy=score_policy,
        vector_policy=vector_policy,
    )

    document_scores = output(DocumentScore, merged.scores)
    section_scores = output(SectionScore, scoring.section_scores)
    paragraph_scores = output(ParagraphScore, scoring.paragraph_scores)
    sentence_scores = output(SentenceScore, scoring.sentence_scores)
    document_overlap_scores = output(DocumentOverlapScore, scoring.document_overlap_scores)
    section_overlap_scores = output(SectionOverlapScore, scoring.section_overlap_scores)
    paragraph_overlap_scores = output(ParagraphOverlapScore, scoring.paragraph_overlap_scores)
    sentence_overlap_scores = output(SentenceOverlapScore, scoring.sentence_overlap_scores)
    document_vector_scores = output(DocumentVectorScore, merged_vectors.scores)
    paragraph_vector_scores = output(ParagraphVectorScore, merged_paragraph_vectors.scores)
