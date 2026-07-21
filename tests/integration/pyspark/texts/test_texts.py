from datetime import datetime
from typing import cast

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows, single

from examples.texts.schemas.analytics import (
    CorpusStatistics,
    CorpusVocabulary,
    DocumentFeatures,
    DocumentStatistics,
    ParagraphStatistics,
    SectionStatistics,
    SentenceStatistics,
    SimilarDocument,
)
from examples.texts.schemas.search import (
    DocumentBm25Score,
    DocumentOverlapScore,
    DocumentSearchTarget,
    ParagraphBm25Score,
    ParagraphOverlapScore,
    ParagraphSearchTarget,
    SearchQuery,
    SectionBm25Score,
    SectionOverlapScore,
    SectionSearchTarget,
    SentenceBm25Score,
    SentenceOverlapScore,
    SentenceSearchTarget,
)
from examples.texts.schemas.text import Document, Paragraph, Section, Sentence, Word
from examples.texts.transforms.analyze import AnalyzeText
from examples.texts.transforms.corpus import CorpusText
from examples.texts.transforms.extract import ExtractText
from examples.texts.transforms.profile import ProfileDocuments
from examples.texts.transforms.search.ScoreCorpus import ScoreCorpus

pytestmark = pytest.mark.integration

PACKAGE = "integration_texts_generated"
SCHEMA_MODULES = {
    "examples.texts.schemas.analytics": [
        DocumentFeatures,
        SentenceStatistics,
        ParagraphStatistics,
        SectionStatistics,
        DocumentStatistics,
        CorpusStatistics,
        CorpusVocabulary,
        SimilarDocument,
    ],
    "examples.texts.schemas.search": [
        SearchQuery,
        DocumentSearchTarget,
        SectionSearchTarget,
        ParagraphSearchTarget,
        SentenceSearchTarget,
        DocumentOverlapScore,
        SectionOverlapScore,
        ParagraphOverlapScore,
        SentenceOverlapScore,
        DocumentBm25Score,
        SectionBm25Score,
        ParagraphBm25Score,
        SentenceBm25Score,
    ],
    "examples.texts.schemas.text": [Document, Section, Paragraph, Sentence, Word],
}
TRANSFORMS = (
    (ExtractText, "examples.texts.transforms.extract.ExtractText"),
    (ProfileDocuments, "examples.texts.transforms.profile.ProfileDocuments"),
    (AnalyzeText, "examples.texts.transforms.analyze.AnalyzeText"),
    (CorpusText, "examples.texts.transforms.corpus.CorpusText"),
    (ScoreCorpus, "examples.texts.transforms.search.ScoreCorpus.ScoreCorpus"),
)


def test_text_fixture_runs_online_and_generated(spark, tmp_path) -> None:
    files = {}
    for transform, source in TRANSFORMS:
        files.update(
            render_generated_project(
                transform,
                source_transform=source,
                generated_package=PACKAGE,
                source_schema_modules=SCHEMA_MODULES,
            )
        )

    with generated_project(tmp_path, PACKAGE, files):
        schemas = __import__(f"{PACKAGE}.pyspark.schemas.text", fromlist=["DOCUMENT_SCHEMA"])
        documents = spark.createDataFrame(
            [
                (
                    "d-1",
                    "docs",
                    "structure",
                    "Structure Guide",
                    "https://example.test/guide",
                    "# Introduction\nStructure makes typed Spark transforms readable.\n\n# Analysis\nThis guide measures text clearly.",
                    "text/plain",
                    "utf-8",
                    "en",
                    datetime(2026, 7, 1, 8),
                    datetime(2026, 7, 2, 8),
                    datetime(2026, 7, 10, 9),
                ),
                (
                    "d-2",
                    "docs",
                    "structure",
                    "Structure Guide Revised",
                    "https://example.test/guide-revised",
                    "# Introduction\nStructure makes typed Spark transformations readable.\n\n# Analysis\nThis guide measures documents clearly.",
                    "text/plain",
                    "utf-8",
                    "en",
                    datetime(2026, 7, 3, 8),
                    datetime(2026, 7, 4, 8),
                    datetime(2026, 7, 10, 10),
                ),
                (
                    "d-3",
                    "docs",
                    "reference",
                    "Reference Notes",
                    None,
                    "Plain text has one paragraph. It still becomes a document section.",
                    "text/plain",
                    "utf-8",
                    "en",
                    None,
                    None,
                    datetime(2026, 7, 11, 11),
                ),
            ],
            schemas.DOCUMENT_SCHEMA,
        )
        online_segments = ExtractText(documents=documents).run(session(spark, execution_mode="online"))
        generated_segments = ExtractText(documents=documents).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_segments.words, "id") == rows(generated_segments.words, "id")
        assert len(rows(generated_segments.sections, "id")) == 5
        assert single(generated_segments.sections, lambda row: row["document_id"] == "d-3")["heading"] == "Document"

        online_features = ProfileDocuments(documents=documents).run(session(spark, execution_mode="online")).features
        generated_features = (
            ProfileDocuments(documents=documents)
            .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
            .features
        )
        assert rows(online_features, "document_id") == rows(generated_features, "document_id")
        guide = single(generated_features, lambda row: row["document_id"] == "d-1")
        assert guide["url_is_https"] is True
        assert guide["content_contains_structure"] is True
        assert guide["content_sha2"]

        inputs = dict(
            words=generated_segments.words,
            paragraphs=generated_segments.paragraphs,
            sections=generated_segments.sections,
            comparison_left=generated_features,
            comparison_right=generated_features,
        )
        online_analytics = AnalyzeText(**inputs).run(session(spark, execution_mode="online"))
        generated_analytics = AnalyzeText(**inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_analytics.document_statistics, "document_id") == rows(
            generated_analytics.document_statistics, "document_id"
        )
        assert len(rows(generated_analytics.similar_documents, "left_document_id", "right_document_id")) == 1

        corpus_inputs = dict(documents=generated_analytics.document_statistics, words=generated_segments.words)
        online_corpus = CorpusText(**corpus_inputs).run(session(spark, execution_mode="online"))
        generated_corpus = CorpusText(**corpus_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_corpus.corpus_statistics, "corpus") == rows(generated_corpus.corpus_statistics, "corpus")
        assert rows(online_corpus.corpus_vocabulary, "corpus") == rows(generated_corpus.corpus_vocabulary, "corpus")

        queries = spark.createDataFrame(
            [("q-structure", "Structure transformations"), ("q-reference", "reference text")],
            __import__(f"{PACKAGE}.pyspark.schemas.search", fromlist=["SEARCH_QUERY_SCHEMA"]).SEARCH_QUERY_SCHEMA,
        )
        search_inputs = dict(queries=queries, words=generated_segments.words)
        online_scores = ScoreCorpus(**search_inputs).run(session(spark, execution_mode="online"))
        generated_scores = ScoreCorpus(**search_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_scores.document_overlap_scores, "query_id", "document_id") == rows(
            generated_scores.document_overlap_scores, "query_id", "document_id"
        )
        assert rows(online_scores.section_overlap_scores, "query_id", "section_id") == rows(
            generated_scores.section_overlap_scores, "query_id", "section_id"
        )
        assert rows(online_scores.paragraph_overlap_scores, "query_id", "paragraph_id") == rows(
            generated_scores.paragraph_overlap_scores, "query_id", "paragraph_id"
        )
        assert rows(online_scores.sentence_overlap_scores, "query_id", "sentence_id") == rows(
            generated_scores.sentence_overlap_scores, "query_id", "sentence_id"
        )
        structure_document = single(
            generated_scores.document_overlap_scores,
            lambda row: row["query_id"] == "q-structure" and row["document_id"] == "d-1",
        )
        assert cast(float, structure_document["score_overlap"]) > 0

        assert rows(online_scores.document_bm25_scores, "query_id", "document_id") == rows(
            generated_scores.document_bm25_scores, "query_id", "document_id"
        )
        assert rows(online_scores.section_bm25_scores, "query_id", "section_id") == rows(
            generated_scores.section_bm25_scores, "query_id", "section_id"
        )
        assert rows(online_scores.paragraph_bm25_scores, "query_id", "paragraph_id") == rows(
            generated_scores.paragraph_bm25_scores, "query_id", "paragraph_id"
        )
        assert rows(online_scores.sentence_bm25_scores, "query_id", "sentence_id") == rows(
            generated_scores.sentence_bm25_scores, "query_id", "sentence_id"
        )
        structure_document = single(
            generated_scores.document_bm25_scores,
            lambda row: row["query_id"] == "q-structure" and row["document_id"] == "d-1",
        )
        assert cast(float, structure_document["score_bm25"]) > 0
