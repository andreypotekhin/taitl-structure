"""BM25 scoring from reusable text-index artifacts."""

from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexSummary,
    DocumentIndexTerm,
    ParagraphIndexSummary,
    ParagraphIndexTerm,
    SectionIndexSummary,
    SectionIndexTerm,
    SentenceIndexSummary,
    SentenceIndexTerm,
)
from examples.search.schemas.scoring.bm25 import (
    DocumentBm25Score,
    ParagraphBm25Score,
    SectionBm25Score,
    SentenceBm25Score,
)
from examples.search.schemas.scoring.intermediate import QueryTerm
from examples.search.transforms.scoring.ScoreBase import ScoreBase
from structure import input, output, parameter, step
from structure.plugin.pyspark import cross_join, group_by, inner_join, log
from structure.plugin.pyspark import sum as sum_


class ScoreBm25(ScoreBase):
    """Score each query with BM25 over four reusable target indexes."""

    document_summary = input(DocumentIndexSummary)
    section_summary = input(SectionIndexSummary)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_summary = input(SentenceIndexSummary)
    document_bm25_scores = output(DocumentBm25Score)
    section_bm25_scores = output(SectionBm25Score)
    paragraph_bm25_scores = output(ParagraphBm25Score)
    sentence_bm25_scores = output(SentenceBm25Score)
    k1 = parameter(1.2)
    b = parameter(0.75)

    @step(input=[ScoreBase.query_terms, ScoreBase.document_terms, document_summary], output=document_bm25_scores)
    def score_document_bm25(
        self, query: QueryTerm, term: DocumentIndexTerm, summary: DocumentIndexSummary
    ) -> DocumentBm25Score:
        inner_join(on=term.token == query.token)
        cross_join(summary, allow_cartesian=True)
        group_by(query_id=query.query_id, document_id=term.document_id)
        return DocumentBm25Score(
            query_id=query.query_id,
            document_id=term.document_id,
            score_bm25=sum_(self._bm25_term(term, summary)),
        )

    @step(input=[ScoreBase.query_terms, ScoreBase.section_terms, section_summary], output=section_bm25_scores)
    def score_section_bm25(
        self, query: QueryTerm, term: SectionIndexTerm, summary: SectionIndexSummary
    ) -> SectionBm25Score:
        inner_join(on=term.token == query.token)
        cross_join(summary, allow_cartesian=True)
        group_by(query_id=query.query_id, document_id=term.document_id, section_id=term.section_id)
        return SectionBm25Score(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            score_bm25=sum_(self._bm25_term(term, summary)),
        )

    @step(input=[ScoreBase.query_terms, ScoreBase.paragraph_terms, paragraph_summary], output=paragraph_bm25_scores)
    def score_paragraph_bm25(
        self, query: QueryTerm, term: ParagraphIndexTerm, summary: ParagraphIndexSummary
    ) -> ParagraphBm25Score:
        inner_join(on=term.token == query.token)
        cross_join(summary, allow_cartesian=True)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
        )
        return ParagraphBm25Score(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            score_bm25=sum_(self._bm25_term(term, summary)),
        )

    @step(input=[ScoreBase.query_terms, ScoreBase.sentence_terms, sentence_summary], output=sentence_bm25_scores)
    def score_sentence_bm25(
        self, query: QueryTerm, term: SentenceIndexTerm, summary: SentenceIndexSummary
    ) -> SentenceBm25Score:
        inner_join(on=term.token == query.token)
        cross_join(summary, allow_cartesian=True)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
        )
        return SentenceBm25Score(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
            score_bm25=sum_(self._bm25_term(term, summary)),
        )

    def _bm25_term(
        self,
        term: DocumentIndexTerm | SectionIndexTerm | ParagraphIndexTerm | SentenceIndexTerm,
        summary: DocumentIndexSummary | SectionIndexSummary | ParagraphIndexSummary | SentenceIndexSummary,
    ) -> object:
        inverse_frequency = log(
            1.0 + (summary.target_count - term.document_frequency + 0.5) / (term.document_frequency + 0.5)
        )
        normalization = term.term_frequency + self.k1 * (
            1.0 - self.b + self.b * term.target_word_count / summary.average_target_length
        )
        return inverse_frequency * term.term_frequency * (self.k1 + 1.0) / normalization
