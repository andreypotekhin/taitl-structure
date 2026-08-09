"""Complete pre-serving search-artifact build."""

from examples.search.schemas.analytics import (
    CorpusStatistics,
    CorpusVocabulary,
    DocumentProfile,
    DocumentStatistics,
    ParagraphStatistics,
    SectionStatistics,
    SentenceStatistics,
    SimilarDocument,
)
from examples.search.schemas.clicks import DailyClicks, DailyImpressions
from examples.search.schemas.fields import *
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
from examples.search.schemas.label import Intent, IntentPattern, QueryLabel
from examples.search.schemas.relevance import DocumentPopularity, QueryDocumentSignals, RelevancePolicy
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
from examples.search.schemas.search import (
    DocumentScore,
    ParagraphScore,
    ScorePolicy,
    SearchQuery,
    SectionScore,
    SentenceScore,
)
from examples.search.schemas.similarity import (
    DocumentSimilarity,
    ParagraphSimilarity,
    SectionSimilarity,
    SentenceSimilarity,
    SimilarityPolicy,
)
from examples.search.schemas.text import Document, Paragraph, Section, Sentence
from examples.search.schemas.user import Band, BandFallback, BandMembership, User, UserBand, UserBandMembership
from examples.search.transforms.chunking import Chunking
from examples.search.transforms.cohorts import ResolveCohortBands
from examples.search.transforms.fields import ExtractDocumentFields
from examples.search.transforms.indexing import Indexing
from examples.search.transforms.labeling import Labeling
from examples.search.transforms.relevance.BuildRelevanceSignals import BuildRelevanceSignals
from examples.search.transforms.scoring import OfflineScoring
from examples.search.transforms.similarities import Similarities
from examples.search.transforms.stats import AnalyzeText, CorpusText, ProfileDocuments
from structure import *


class All(Transform):
    """Build every corpus, query, cohort, and feedback artifact before result presentation."""

    documents = input(Document)
    queries = input(SearchQuery)
    query_labels = input(QueryLabel)
    intents = input(Intent)
    patterns = input(IntentPattern)
    daily_impressions = input(DailyImpressions)
    daily_clicks = input(DailyClicks)
    users = input(User)
    bands = input(Band)
    policy = input(RelevancePolicy)
    score_policy = input(ScorePolicy)
    similarity_policy = input(SimilarityPolicy)
    field_profiles = input(FieldProfile)
    analyzer_policies = input(AnalyzerPolicy)
    maximum_offline_queries = parameter(1000)

    chunked = Chunking(documents=documents)
    extracted = ExtractDocumentFields(source_documents=documents)
    profiled = ProfileDocuments(documents=extracted.documents)
    indexed = Indexing(
        documents=extracted.documents,
        sentences=chunked.sentences,
        document_fields=extracted.document_fields,
        field_profiles=field_profiles,
        analyzer_policies=analyzer_policies,
    )

    similarities = Similarities(
        policy=similarity_policy,
        document_terms=indexed.document_terms,
        document_summary=indexed.document_summary,
        section_terms=indexed.section_terms,
        section_summary=indexed.section_summary,
        paragraph_terms=indexed.paragraph_terms,
        paragraph_summary=indexed.paragraph_summary,
        sentence_terms=indexed.sentence_terms,
        sentence_summary=indexed.sentence_summary,
        score_policy=score_policy,
    )

    labeled = Labeling(queries=queries, query_labels=query_labels, intents=intents, patterns=patterns)

    scored = OfflineScoring(
        queries=labeled.labeled_queries,
        daily_impressions=daily_impressions,
        document_terms=indexed.document_terms,
        section_terms=indexed.section_terms,
        paragraph_terms=indexed.paragraph_terms,
        sentence_terms=indexed.sentence_terms,
        document_summary=indexed.document_summary,
        section_summary=indexed.section_summary,
        paragraph_summary=indexed.paragraph_summary,
        sentence_summary=indexed.sentence_summary,
        score_policy=score_policy,
        maximum_offline_queries=maximum_offline_queries,
    )

    cohorts = ResolveCohortBands(users=users, bands=bands)

    relevance = BuildRelevanceSignals(
        daily_impressions=daily_impressions,
        daily_clicks=daily_clicks,
        band_memberships=cohorts.band_memberships,
        user_band_memberships=cohorts.user_band_memberships,
        band_fallbacks=cohorts.band_fallbacks,
        policy=policy,
    )

    analyzed = AnalyzeText(
        documents=extracted.documents,
        sentences=chunked.sentences,
        paragraphs=chunked.paragraphs,
        sections=chunked.sections,
        document_terms=indexed.document_terms,
        section_terms=indexed.section_terms,
        paragraph_terms=indexed.paragraph_terms,
        sentence_terms=indexed.sentence_terms,
        comparison_left=profiled.features,
        comparison_right=profiled.features,
    )

    corpus = CorpusText(
        document_statistics=analyzed.document_statistics,
        document_terms=indexed.document_terms,
    )

    sections = output(Section, chunked.sections)
    paragraphs = output(Paragraph, chunked.paragraphs)
    sentences = output(Sentence, chunked.sentences)
    document_fields = output(DocumentField, extracted.document_fields)
    field_terms = output(FieldTerm, indexed.field_terms)
    document_profiles = output(DocumentProfile, profiled.features)
    sentence_statistics = output(SentenceStatistics, analyzed.sentence_statistics)
    paragraph_statistics = output(ParagraphStatistics, analyzed.paragraph_statistics)
    section_statistics = output(SectionStatistics, analyzed.section_statistics)
    document_statistics = output(DocumentStatistics, analyzed.document_statistics)
    similar_documents = output(SimilarDocument, analyzed.similar_documents)
    corpus_statistics = output(CorpusStatistics, corpus.corpus_statistics)
    corpus_vocabulary = output(CorpusVocabulary, corpus.corpus_vocabulary)
    document_terms = output(DocumentTerm, indexed.document_terms)
    document_summary = output(DocumentIndexSummary, indexed.document_summary)
    section_terms = output(SectionTerm, indexed.section_terms)
    section_summary = output(SectionIndexSummary, indexed.section_summary)
    paragraph_terms = output(ParagraphTerm, indexed.paragraph_terms)
    paragraph_summary = output(ParagraphIndexSummary, indexed.paragraph_summary)
    sentence_terms = output(SentenceTerm, indexed.sentence_terms)
    sentence_summary = output(SentenceIndexSummary, indexed.sentence_summary)
    labeled_queries = output(SearchQuery, labeled.labeled_queries)
    document_scores = output(DocumentScore, scored.document_scores)
    section_scores = output(SectionScore, scored.section_scores)
    paragraph_scores = output(ParagraphScore, scored.paragraph_scores)
    sentence_scores = output(SentenceScore, scored.sentence_scores)
    document_overlap_scores = output(DocumentOverlapScore, scored.document_overlap_scores)
    section_overlap_scores = output(SectionOverlapScore, scored.section_overlap_scores)
    paragraph_overlap_scores = output(ParagraphOverlapScore, scored.paragraph_overlap_scores)
    sentence_overlap_scores = output(SentenceOverlapScore, scored.sentence_overlap_scores)
    document_bm25_scores = output(DocumentBm25Score, scored.document_bm25_scores)
    section_bm25_scores = output(SectionBm25Score, scored.section_bm25_scores)
    paragraph_bm25_scores = output(ParagraphBm25Score, scored.paragraph_bm25_scores)
    sentence_bm25_scores = output(SentenceBm25Score, scored.sentence_bm25_scores)
    document_similarities = output(DocumentSimilarity, similarities.document_similarities)
    section_similarities = output(SectionSimilarity, similarities.section_similarities)
    paragraph_similarities = output(ParagraphSimilarity, similarities.paragraph_similarities)
    sentence_similarities = output(SentenceSimilarity, similarities.sentence_similarities)
    band_memberships = output(BandMembership, cohorts.band_memberships)
    user_bands = output(UserBand, cohorts.user_bands)
    user_band_memberships = output(UserBandMembership, cohorts.user_band_memberships)
    band_fallbacks = output(BandFallback, cohorts.band_fallbacks)
    query_document_signals = output(QueryDocumentSignals, relevance.query_document_signals)
    document_popularity = output(DocumentPopularity, relevance.document_popularity)
