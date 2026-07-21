"""Query-scoped hierarchy enrichment from reusable-index scores."""

from examples.texts.schemas.search import (
    DocumentBm25Score,
    DocumentOverlapScore,
    ParagraphBm25Score,
    ParagraphOverlapScore,
    SectionBm25Score,
    SectionOverlapScore,
    SentenceBm25Score,
    SentenceOverlapScore,
)
from examples.texts.schemas.text import Document, Paragraph, Section, Sentence
from examples.texts.transforms.scoring.ScoreAll import ScoreAll
from structure import input, lane, output, step
from structure.plugin.pyspark import inner_join


class AddScores(ScoreAll):
    """Run index-backed scoring and attach results to matching hierarchy rows."""

    documents = input(Document)
    sections = input(Section)
    paragraphs = input(Paragraph)
    sentences = input(Sentence)
    scored_documents = output(Document)
    scored_sections = output(Section)
    scored_paragraphs = output(Paragraph)
    scored_sentences = output(Sentence)

    @step(
        input=[documents, lane(ScoreAll.document_overlap_scores), lane(ScoreAll.document_bm25_scores)],
        output=scored_documents,
    )
    def score_documents(self, document: Document, overlap: DocumentOverlapScore, bm25: DocumentBm25Score) -> Document:
        overlap = inner_join(overlap, on=overlap.document_id == document.id)
        bm25 = inner_join(bm25, on=(bm25.document_id == document.id) & (bm25.query_id == overlap.query_id))
        return Document.project(document)(
            search_query_id=overlap.query_id, score_overlap=overlap.score_overlap, score_bm25=bm25.score_bm25
        )

    @step(
        input=[sections, lane(ScoreAll.section_overlap_scores), lane(ScoreAll.section_bm25_scores)],
        output=scored_sections,
    )
    def score_sections(self, section: Section, overlap: SectionOverlapScore, bm25: SectionBm25Score) -> Section:
        overlap = inner_join(overlap, on=overlap.section_id == section.id)
        bm25 = inner_join(bm25, on=(bm25.section_id == section.id) & (bm25.query_id == overlap.query_id))
        return Section.project(section)(
            search_query_id=overlap.query_id, score_overlap=overlap.score_overlap, score_bm25=bm25.score_bm25
        )

    @step(
        input=[paragraphs, lane(ScoreAll.paragraph_overlap_scores), lane(ScoreAll.paragraph_bm25_scores)],
        output=scored_paragraphs,
    )
    def score_paragraphs(
        self, paragraph: Paragraph, overlap: ParagraphOverlapScore, bm25: ParagraphBm25Score
    ) -> Paragraph:
        overlap = inner_join(overlap, on=overlap.paragraph_id == paragraph.id)
        bm25 = inner_join(bm25, on=(bm25.paragraph_id == paragraph.id) & (bm25.query_id == overlap.query_id))
        return Paragraph.project(paragraph)(
            search_query_id=overlap.query_id, score_overlap=overlap.score_overlap, score_bm25=bm25.score_bm25
        )

    @step(
        input=[sentences, lane(ScoreAll.sentence_overlap_scores), lane(ScoreAll.sentence_bm25_scores)],
        output=scored_sentences,
    )
    def score_sentences(self, sentence: Sentence, overlap: SentenceOverlapScore, bm25: SentenceBm25Score) -> Sentence:
        overlap = inner_join(overlap, on=overlap.sentence_id == sentence.id)
        bm25 = inner_join(bm25, on=(bm25.sentence_id == sentence.id) & (bm25.query_id == overlap.query_id))
        return Sentence.project(sentence)(
            search_query_id=overlap.query_id, score_overlap=overlap.score_overlap, score_bm25=bm25.score_bm25
        )
