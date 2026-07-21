import csv
from datetime import datetime
from pathlib import Path
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
    DocumentIndexSummary,
    DocumentIndexTarget,
    DocumentIndexTerm,
    DocumentOverlapScore,
    DocumentSearchTarget,
    ParagraphBm25Score,
    ParagraphIndexSummary,
    ParagraphIndexTarget,
    ParagraphIndexTerm,
    ParagraphOverlapScore,
    ParagraphSearchTarget,
    SearchQuery,
    SectionBm25Score,
    SectionIndexSummary,
    SectionIndexTarget,
    SectionIndexTerm,
    SectionOverlapScore,
    SectionSearchTarget,
    SentenceBm25Score,
    SentenceIndexSummary,
    SentenceIndexTarget,
    SentenceIndexTerm,
    SentenceOverlapScore,
    SentenceSearchResult,
    SentenceSearchTarget,
)
from examples.texts.schemas.similarity import (
    DocumentSimilarity,
    DocumentSimilarityQuery,
    IndexedSimilarDocument,
    IndexedSimilarParagraph,
    IndexedSimilarSection,
    IndexedSimilarSentence,
    ParagraphSimilarity,
    ParagraphSimilarityQuery,
    SectionSimilarity,
    SectionSimilarityQuery,
    SentenceSimilarity,
    SentenceSimilarityQuery,
    SimilarityDocumentQuery,
    SimilarityParagraphQuery,
    SimilarityPolicy,
    SimilaritySectionQuery,
    SimilaritySentenceQuery,
)
from examples.texts.schemas.text import Document, Paragraph, Section, Sentence, Word
from examples.texts.transforms.analyze import AnalyzeText
from examples.texts.transforms.corpus import CorpusText
from examples.texts.transforms.extract import ExtractText
from examples.texts.transforms.index import CreateIndex
from examples.texts.transforms.profile import ProfileDocuments
from examples.texts.transforms.scoring.AddScores import AddScores
from examples.texts.transforms.scoring.ScoreAll import ScoreAll
from examples.texts.transforms.search import Search
from examples.texts.transforms.similarities.CreateSimilarityQueries import CreateSimilarityQueries
from examples.texts.transforms.similarities.ReduceSimilarityScores import ReduceSimilarityScores
from examples.texts.transforms.similarity import Similarity, SimilarParagraphs, SimilarSections, SimilarSentences

pytestmark = pytest.mark.integration

PACKAGE = "integration_texts_generated"
FIXTURES = Path(__file__).resolve().parents[4] / "examples" / "fixtures" / "texts"
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
        DocumentIndexTarget,
        SectionIndexTarget,
        ParagraphIndexTarget,
        SentenceIndexTarget,
        DocumentIndexTerm,
        DocumentIndexSummary,
        SectionIndexTerm,
        SectionIndexSummary,
        ParagraphIndexTerm,
        ParagraphIndexSummary,
        SentenceIndexTerm,
        SentenceIndexSummary,
        SentenceSearchResult,
        DocumentOverlapScore,
        SectionOverlapScore,
        ParagraphOverlapScore,
        SentenceOverlapScore,
        DocumentBm25Score,
        SectionBm25Score,
        ParagraphBm25Score,
        SentenceBm25Score,
    ],
    "examples.texts.schemas.similarity": [
        SimilarityPolicy,
        SimilarityDocumentQuery,
        SimilaritySectionQuery,
        SimilarityParagraphQuery,
        SimilaritySentenceQuery,
        DocumentSimilarityQuery,
        SectionSimilarityQuery,
        ParagraphSimilarityQuery,
        SentenceSimilarityQuery,
        DocumentSimilarity,
        IndexedSimilarDocument,
        IndexedSimilarSection,
        IndexedSimilarParagraph,
        IndexedSimilarSentence,
        SectionSimilarity,
        ParagraphSimilarity,
        SentenceSimilarity,
    ],
    "examples.texts.schemas.text": [Document, Section, Paragraph, Sentence, Word],
}
TRANSFORMS = (
    (ExtractText, "examples.texts.transforms.extract.ExtractText"),
    (ProfileDocuments, "examples.texts.transforms.profile.ProfileDocuments"),
    (AnalyzeText, "examples.texts.transforms.analyze.AnalyzeText"),
    (CorpusText, "examples.texts.transforms.corpus.CorpusText"),
    (CreateIndex, "examples.texts.transforms.index.CreateIndex"),
    (Search, "examples.texts.transforms.search.Search"),
    (
        CreateSimilarityQueries,
        "examples.texts.transforms.similarities.CreateSimilarityQueries.CreateSimilarityQueries",
    ),
    (ScoreAll, "examples.texts.transforms.scoring.ScoreAll.ScoreAll"),
    (
        ReduceSimilarityScores,
        "examples.texts.transforms.similarities.ReduceSimilarityScores.ReduceSimilarityScores",
    ),
    (Similarity, "examples.texts.transforms.similarity.Similarity"),
    (SimilarSections, "examples.texts.transforms.similarities.SimilarSections.SimilarSections"),
    (SimilarParagraphs, "examples.texts.transforms.similarities.SimilarParagraphs.SimilarParagraphs"),
    (SimilarSentences, "examples.texts.transforms.similarities.SimilarSentences.SimilarSentences"),
    (AddScores, "examples.texts.transforms.scoring.AddScores.AddScores"),
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
                    None,
                    None,
                    None,
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
                    None,
                    None,
                    None,
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
                    None,
                    None,
                    None,
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
        online_index = CreateIndex(words=generated_segments.words).run(session(spark, execution_mode="online"))
        generated_index = CreateIndex(words=generated_segments.words).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_index.document_terms, "document_id", "token") == rows(
            generated_index.document_terms, "document_id", "token"
        )
        document_structure = single(
            generated_index.document_terms,
            lambda row: row["document_id"] == "d-1" and row["token"] == "structure",
        )
        assert document_structure["term_frequency"] == 1
        assert document_structure["document_frequency"] == 2
        assert cast(int, document_structure["target_word_count"]) >= cast(
            int, document_structure["target_distinct_terms"]
        )
        assert single(generated_index.document_summary, lambda row: True)["target_count"] == 3
        assert single(generated_index.sentence_summary, lambda row: True)["target_count"] == len(
            rows(generated_segments.sentences, "id")
        )

        similarity_policy = spark.createDataFrame(
            [(None,)],
            __import__(
                f"{PACKAGE}.pyspark.schemas.similarity", fromlist=["SIMILARITY_POLICY_SCHEMA"]
            ).SIMILARITY_POLICY_SCHEMA,
        )
        similarity_index_inputs = dict(
            policy=similarity_policy,
            document_terms=generated_index.document_terms,
            document_summary=generated_index.document_summary,
            section_terms=generated_index.section_terms,
            section_summary=generated_index.section_summary,
            paragraph_terms=generated_index.paragraph_terms,
            paragraph_summary=generated_index.paragraph_summary,
            sentence_terms=generated_index.sentence_terms,
            sentence_summary=generated_index.sentence_summary,
        )
        online_similarity_queries = CreateSimilarityQueries(**similarity_index_inputs).run(
            session(spark, execution_mode="online")
        )
        generated_similarity_queries = CreateSimilarityQueries(**similarity_index_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_similarity_queries.queries, "id") == rows(generated_similarity_queries.queries, "id")
        assert len(rows(generated_similarity_queries.document_queries, "query_id")) == 3
        assert (
            single(generated_similarity_queries.document_queries, lambda row: row["document_id"] == "d-1")["query_id"]
            == "document:d-1"
        )
        pruned_policy = spark.createDataFrame(
            [(0.5,)],
            __import__(
                f"{PACKAGE}.pyspark.schemas.similarity", fromlist=["SIMILARITY_POLICY_SCHEMA"]
            ).SIMILARITY_POLICY_SCHEMA,
        )
        pruned_queries = CreateSimilarityQueries(**{**similarity_index_inputs, "policy": pruned_policy}).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert (
            "structure"
            not in cast(str, single(pruned_queries.queries, lambda row: row["id"] == "document:d-1")["content"]).split()
        )

        similarity_score_inputs = dict(
            queries=generated_similarity_queries.queries,
            **{name: value for name, value in similarity_index_inputs.items() if name != "policy"},
        )
        online_similarity_scores = ScoreAll(**similarity_score_inputs).run(session(spark, execution_mode="online"))
        generated_similarity_scores = ScoreAll(**similarity_score_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        similarity_reducer_inputs = dict(
            document_queries=generated_similarity_queries.document_queries,
            section_queries=generated_similarity_queries.section_queries,
            paragraph_queries=generated_similarity_queries.paragraph_queries,
            sentence_queries=generated_similarity_queries.sentence_queries,
            **{
                name: getattr(generated_similarity_scores, name)
                for name in (
                    "document_overlap_scores",
                    "section_overlap_scores",
                    "paragraph_overlap_scores",
                    "sentence_overlap_scores",
                    "document_bm25_scores",
                    "section_bm25_scores",
                    "paragraph_bm25_scores",
                    "sentence_bm25_scores",
                )
            },
        )
        online_similarities = ReduceSimilarityScores(
            document_queries=online_similarity_queries.document_queries,
            section_queries=online_similarity_queries.section_queries,
            paragraph_queries=online_similarity_queries.paragraph_queries,
            sentence_queries=online_similarity_queries.sentence_queries,
            **{
                name: getattr(online_similarity_scores, name)
                for name in (
                    "document_overlap_scores",
                    "section_overlap_scores",
                    "paragraph_overlap_scores",
                    "sentence_overlap_scores",
                    "document_bm25_scores",
                    "section_bm25_scores",
                    "paragraph_bm25_scores",
                    "sentence_bm25_scores",
                )
            },
        ).run(session(spark, execution_mode="online"))
        generated_similarities = ReduceSimilarityScores(**similarity_reducer_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_similarities.document_similarities, "left_document_id", "right_document_id") == rows(
            generated_similarities.document_similarities, "left_document_id", "right_document_id"
        )
        similar_guides = single(
            generated_similarities.document_similarities,
            lambda row: row["left_document_id"] == "d-1" and row["right_document_id"] == "d-2",
        )
        assert cast(float, similar_guides["score_overlap"]) > 0
        assert cast(float, similar_guides["bm25_left_to_right"]) > 0
        assert cast(float, similar_guides["bm25_right_to_left"]) > 0

        similar_document_inputs = dict(
            query=documents.where("id = 'd-1'"),
            documents=documents,
            document_similarities=generated_similarities.document_similarities,
        )
        online_similar_documents = (
            Similarity(
                query=similar_document_inputs["query"],
                documents=documents,
                document_similarities=online_similarities.document_similarities,
            )
            .run(session(spark, execution_mode="online"))
            .similar_documents
        )
        generated_similar_documents = (
            Similarity(**similar_document_inputs)
            .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
            .similar_documents
        )
        assert rows(online_similar_documents, "rank", "id") == rows(generated_similar_documents, "rank", "id")
        best_match = single(generated_similar_documents, lambda row: row["rank"] == 1)
        assert best_match["id"] == "d-2"
        assert best_match["search_query_id"] == "d-1"

        similar_sections = (
            SimilarSections(
                query=generated_segments.sections.where("document_id = 'd-1' AND heading = 'Introduction'"),
                sections=generated_segments.sections,
                section_similarities=generated_similarities.section_similarities,
            )
            .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
            .similar_sections
        )
        similar_paragraphs = (
            SimilarParagraphs(
                query=generated_segments.paragraphs.where("document_id = 'd-1'").limit(1),
                paragraphs=generated_segments.paragraphs,
                paragraph_similarities=generated_similarities.paragraph_similarities,
            )
            .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
            .similar_paragraphs
        )
        similar_sentences = (
            SimilarSentences(
                query=generated_segments.sentences.where("document_id = 'd-1'").limit(1),
                sentences=generated_segments.sentences,
                sentence_similarities=generated_similarities.sentence_similarities,
            )
            .run(session(spark, execution_mode="generated", generated_package=PACKAGE))
            .similar_sentences
        )
        assert all(cast(int, row["rank"]) <= SimilarSections.maximum_results for row in rows(similar_sections, "rank"))
        assert all(
            cast(int, row["rank"]) <= SimilarParagraphs.maximum_results for row in rows(similar_paragraphs, "rank")
        )
        assert all(
            cast(int, row["rank"]) <= SimilarSentences.maximum_results for row in rows(similar_sentences, "rank")
        )

        search_inputs = dict(
            queries=queries,
            document_terms=generated_index.document_terms,
            document_summary=generated_index.document_summary,
            section_terms=generated_index.section_terms,
            section_summary=generated_index.section_summary,
            paragraph_terms=generated_index.paragraph_terms,
            paragraph_summary=generated_index.paragraph_summary,
            sentence_terms=generated_index.sentence_terms,
            sentence_summary=generated_index.sentence_summary,
            documents=documents,
            sections=generated_segments.sections,
            paragraphs=generated_segments.paragraphs,
            sentences=generated_segments.sentences,
        )
        online_scores = AddScores(**search_inputs).run(session(spark, execution_mode="online"))
        generated_scores = AddScores(**search_inputs).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert rows(online_scores.scored_documents, "search_query_id", "id") == rows(
            generated_scores.scored_documents, "search_query_id", "id"
        )
        assert rows(online_scores.scored_sections, "search_query_id", "id") == rows(
            generated_scores.scored_sections, "search_query_id", "id"
        )
        structure_document = single(
            generated_scores.scored_documents,
            lambda row: row["search_query_id"] == "q-structure" and row["id"] == "d-1",
        )
        assert cast(float, structure_document["score_overlap"]) > 0
        assert cast(float, structure_document["score_bm25"]) > 0


def test_search_ranks_fixture_sentences_online_and_generated(spark, tmp_path) -> None:
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
        text_schemas = __import__(f"{PACKAGE}.pyspark.schemas.text", fromlist=["DOCUMENT_SCHEMA"])
        search_schemas = __import__(f"{PACKAGE}.pyspark.schemas.search", fromlist=["SEARCH_QUERY_SCHEMA"])
        documents = spark.createDataFrame(_search_documents(), text_schemas.DOCUMENT_SCHEMA)
        query = spark.createDataFrame([("q-aurora", "aurora beacon navigation")], search_schemas.SEARCH_QUERY_SCHEMA)
        segments = ExtractText(documents=documents).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        index = CreateIndex(words=segments.words).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        scores = AddScores(
            queries=query,
            document_terms=index.document_terms,
            document_summary=index.document_summary,
            section_terms=index.section_terms,
            section_summary=index.section_summary,
            paragraph_terms=index.paragraph_terms,
            paragraph_summary=index.paragraph_summary,
            sentence_terms=index.sentence_terms,
            sentence_summary=index.sentence_summary,
            documents=documents,
            sections=segments.sections,
            paragraphs=segments.paragraphs,
            sentences=segments.sentences,
        ).run(session(spark, execution_mode="generated", generated_package=PACKAGE))

        inputs = dict(query=query, scored_sentences=scores.scored_sentences)
        online = Search(**inputs).run(session(spark, execution_mode="online")).results
        generated = Search(**inputs).run(session(spark, execution_mode="generated", generated_package=PACKAGE)).results

        assert generated.columns == [
            "search_query_id",
            "rank",
            "document_id",
            "section_id",
            "paragraph_id",
            "sentence_id",
            "content",
            "score_overlap",
            "score_bm25",
        ]
        assert rows(online, "rank") == rows(generated, "rank")
        results = rows(generated, "rank")
        assert [row["rank"] for row in results] == [1, 2, 3]
        assert [row["sentence_id"] for row in results] == ["d-11#p0#s0", "d-12#p0#s0", "d-13#p0#s0"]
        assert all(row["score_overlap"] is not None and row["score_bm25"] is not None for row in results)
        assert (
            cast(float, results[0]["score_overlap"])
            > cast(float, results[1]["score_overlap"])
            > cast(float, results[2]["score_overlap"])
        )
        assert (
            cast(float, results[0]["score_bm25"])
            > cast(float, results[1]["score_bm25"])
            > cast(float, results[2]["score_bm25"])
        )


def _search_documents() -> list[tuple[object, ...]]:
    with (FIXTURES / "documents.csv").open(newline="", encoding="utf-8") as source:
        return [
            (
                row["id"],
                row["collection_id"],
                row["source"],
                row["title"],
                row["url"] or None,
                row["content"],
                row["content_type"],
                row["encoding"],
                row["language"],
                _timestamp(row["created_at"]),
                _timestamp(row["published_at"]),
                _timestamp(row["harvested_at"]),
                None,
                None,
                None,
            )
            for row in csv.DictReader(source)
            if row["id"] in {"d-11", "d-12", "d-13"}
        ]


def _timestamp(value: str) -> datetime | None:
    return datetime.fromisoformat(value) if value else None
