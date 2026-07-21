"""Rank corpus targets from indexed lexical-similarity pairs."""

from typing import Final

from examples.texts.schemas.similarity import DocumentSimilarity as DocumentSimilarityPair
from examples.texts.schemas.similarity import (
    IndexedSimilarDocument,
    IndexedSimilarParagraph,
    IndexedSimilarSection,
    IndexedSimilarSentence,
)
from examples.texts.schemas.similarity import ParagraphSimilarity as ParagraphSimilarityPair
from examples.texts.schemas.similarity import SectionSimilarity as SectionSimilarityPair
from examples.texts.schemas.similarity import SentenceSimilarity as SentenceSimilarityPair
from examples.texts.schemas.similarity import (
    SimilarityDocumentQuery,
    SimilarityParagraphQuery,
    SimilaritySectionQuery,
    SimilaritySentenceQuery,
)
from examples.texts.schemas.text import Document, Paragraph, Section, Sentence
from structure import *
from structure.plugin.pyspark import *


class Similarity(Transform):
    """Return the top fixed number of corpus documents similar to one query document."""

    maximum_results: Final = 10

    query = input(SimilarityDocumentQuery)
    documents = input(Document)
    document_similarities = input(DocumentSimilarityPair)
    ranked_documents = lane(IndexedSimilarDocument)
    similar_documents = output(IndexedSimilarDocument)

    @step(input=[query, documents, document_similarities], output=ranked_documents)
    def rank(
        self, query: SimilarityDocumentQuery, document: Document, pair: DocumentSimilarityPair
    ) -> IndexedSimilarDocument:
        inner_join(on=(query.id == pair.left_document_id) | (query.id == pair.right_document_id))
        candidate_id = when(query.id == pair.left_document_id, pair.right_document_id).otherwise(pair.left_document_id)
        score_bm25 = when(query.id == pair.left_document_id, pair.bm25_left_to_right).otherwise(pair.bm25_right_to_left)
        inner_join(on=document.id == candidate_id)
        rank = row_number(
            partition_by=query.id,
            order_by=(score_bm25.desc(), pair.score_overlap.desc(), document.id.asc()),
        )
        return IndexedSimilarDocument(
            id=document.id,
            collection_id=document.collection_id,
            source=document.source,
            title=document.title,
            url=document.url,
            content=document.content,
            content_type=document.content_type,
            encoding=document.encoding,
            language=document.language,
            created_at=document.created_at,
            published_at=document.published_at,
            harvested_at=document.harvested_at,
            search_query_id=query.id,
            score_overlap=pair.score_overlap,
            score_bm25=score_bm25,
            rank=rank,
        )

    @step(input=ranked_documents, output=similar_documents)
    def limit(self, candidate: IndexedSimilarDocument) -> IndexedSimilarDocument:
        where(candidate.rank <= self.maximum_results)
        return IndexedSimilarDocument(
            id=candidate.id,
            collection_id=candidate.collection_id,
            source=candidate.source,
            title=candidate.title,
            url=candidate.url,
            content=candidate.content,
            content_type=candidate.content_type,
            encoding=candidate.encoding,
            language=candidate.language,
            created_at=candidate.created_at,
            published_at=candidate.published_at,
            harvested_at=candidate.harvested_at,
            search_query_id=candidate.search_query_id,
            score_overlap=candidate.score_overlap,
            score_bm25=candidate.score_bm25,
            rank=candidate.rank,
        )


class SimilarSections(Transform):
    """Return the top fixed number of corpus sections similar to one query section."""

    maximum_results: Final = 10

    query = input(SimilaritySectionQuery)
    sections = input(Section)
    section_similarities = input(SectionSimilarityPair)
    ranked_sections = lane(IndexedSimilarSection)
    similar_sections = output(IndexedSimilarSection)

    @step(input=[query, sections, section_similarities], output=ranked_sections)
    def rank(
        self, query: SimilaritySectionQuery, section: Section, pair: SectionSimilarityPair
    ) -> IndexedSimilarSection:
        inner_join(on=(query.id == pair.left_section_id) | (query.id == pair.right_section_id))
        candidate_id = when(query.id == pair.left_section_id, pair.right_section_id).otherwise(pair.left_section_id)
        score_bm25 = when(query.id == pair.left_section_id, pair.bm25_left_to_right).otherwise(pair.bm25_right_to_left)
        inner_join(on=section.id == candidate_id)
        rank = row_number(
            partition_by=query.id,
            order_by=(score_bm25.desc(), pair.score_overlap.desc(), section.id.asc()),
        )
        return IndexedSimilarSection(
            id=section.id,
            document_id=section.document_id,
            ordinal=section.ordinal,
            heading=section.heading,
            search_query_id=query.id,
            score_overlap=pair.score_overlap,
            score_bm25=score_bm25,
            rank=rank,
        )

    @step(input=ranked_sections, output=similar_sections)
    def limit(self, candidate: IndexedSimilarSection) -> IndexedSimilarSection:
        where(candidate.rank <= self.maximum_results)
        return IndexedSimilarSection(
            id=candidate.id,
            document_id=candidate.document_id,
            ordinal=candidate.ordinal,
            heading=candidate.heading,
            search_query_id=candidate.search_query_id,
            score_overlap=candidate.score_overlap,
            score_bm25=candidate.score_bm25,
            rank=candidate.rank,
        )


class SimilarParagraphs(Transform):
    """Return the top fixed number of corpus paragraphs similar to one query paragraph."""

    maximum_results: Final = 10

    query = input(SimilarityParagraphQuery)
    paragraphs = input(Paragraph)
    paragraph_similarities = input(ParagraphSimilarityPair)
    ranked_paragraphs = lane(IndexedSimilarParagraph)
    similar_paragraphs = output(IndexedSimilarParagraph)

    @step(input=[query, paragraphs, paragraph_similarities], output=ranked_paragraphs)
    def rank(
        self, query: SimilarityParagraphQuery, paragraph: Paragraph, pair: ParagraphSimilarityPair
    ) -> IndexedSimilarParagraph:
        inner_join(on=(query.id == pair.left_paragraph_id) | (query.id == pair.right_paragraph_id))
        candidate_id = when(query.id == pair.left_paragraph_id, pair.right_paragraph_id).otherwise(
            pair.left_paragraph_id
        )
        score_bm25 = when(query.id == pair.left_paragraph_id, pair.bm25_left_to_right).otherwise(
            pair.bm25_right_to_left
        )
        inner_join(on=paragraph.id == candidate_id)
        rank = row_number(
            partition_by=query.id,
            order_by=(score_bm25.desc(), pair.score_overlap.desc(), paragraph.id.asc()),
        )
        return IndexedSimilarParagraph(
            id=paragraph.id,
            document_id=paragraph.document_id,
            section_id=paragraph.section_id,
            ordinal=paragraph.ordinal,
            content=paragraph.content,
            search_query_id=query.id,
            score_overlap=pair.score_overlap,
            score_bm25=score_bm25,
            rank=rank,
        )

    @step(input=ranked_paragraphs, output=similar_paragraphs)
    def limit(self, candidate: IndexedSimilarParagraph) -> IndexedSimilarParagraph:
        where(candidate.rank <= self.maximum_results)
        return IndexedSimilarParagraph(
            id=candidate.id,
            document_id=candidate.document_id,
            section_id=candidate.section_id,
            ordinal=candidate.ordinal,
            content=candidate.content,
            search_query_id=candidate.search_query_id,
            score_overlap=candidate.score_overlap,
            score_bm25=candidate.score_bm25,
            rank=candidate.rank,
        )


class SimilarSentences(Transform):
    """Return the top fixed number of corpus sentences similar to one query sentence."""

    maximum_results: Final = 10

    query = input(SimilaritySentenceQuery)
    sentences = input(Sentence)
    sentence_similarities = input(SentenceSimilarityPair)
    ranked_sentences = lane(IndexedSimilarSentence)
    similar_sentences = output(IndexedSimilarSentence)

    @step(input=[query, sentences, sentence_similarities], output=ranked_sentences)
    def rank(
        self, query: SimilaritySentenceQuery, sentence: Sentence, pair: SentenceSimilarityPair
    ) -> IndexedSimilarSentence:
        inner_join(on=(query.id == pair.left_sentence_id) | (query.id == pair.right_sentence_id))
        candidate_id = when(query.id == pair.left_sentence_id, pair.right_sentence_id).otherwise(pair.left_sentence_id)
        score_bm25 = when(query.id == pair.left_sentence_id, pair.bm25_left_to_right).otherwise(pair.bm25_right_to_left)
        inner_join(on=sentence.id == candidate_id)
        rank = row_number(
            partition_by=query.id,
            order_by=(score_bm25.desc(), pair.score_overlap.desc(), sentence.id.asc()),
        )
        return IndexedSimilarSentence(
            id=sentence.id,
            document_id=sentence.document_id,
            section_id=sentence.section_id,
            paragraph_id=sentence.paragraph_id,
            paragraph_ordinal=sentence.paragraph_ordinal,
            ordinal=sentence.ordinal,
            content=sentence.content,
            search_query_id=query.id,
            score_overlap=pair.score_overlap,
            score_bm25=score_bm25,
            rank=rank,
        )

    @step(input=ranked_sentences, output=similar_sentences)
    def limit(self, candidate: IndexedSimilarSentence) -> IndexedSimilarSentence:
        where(candidate.rank <= self.maximum_results)
        return IndexedSimilarSentence(
            id=candidate.id,
            document_id=candidate.document_id,
            section_id=candidate.section_id,
            paragraph_id=candidate.paragraph_id,
            paragraph_ordinal=candidate.paragraph_ordinal,
            ordinal=candidate.ordinal,
            content=candidate.content,
            search_query_id=candidate.search_query_id,
            score_overlap=candidate.score_overlap,
            score_bm25=candidate.score_bm25,
            rank=candidate.rank,
        )
