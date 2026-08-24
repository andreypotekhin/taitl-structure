"""BM25 scoring from reusable text-index artifacts."""

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
from examples.search.schemas.scoring.bm25 import (
    DocumentBm25Score,
    ParagraphBm25Score,
    SectionBm25Score,
    SentenceBm25Score,
)
from examples.search.schemas.scoring.intermediate import QueryTerm
from examples.search.schemas.search import DocumentSearchTarget
from examples.search.transforms.scoring.lexical.ScoreBase import ScoreBase
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

    @step(
        input=[ScoreBase.expanded_query_terms, ScoreBase.document_terms, ScoreBase.targets, document_summary],
        output=document_bm25_scores,
    )
    def score_document_bm25(
        self, query: QueryTerm, term: DocumentTerm, target: DocumentSearchTarget, summary: DocumentIndexSummary
    ) -> DocumentBm25Score:
        inner_join(on=term.term == query.token)
        inner_join(target, on=(target.query_id == query.query_id) & (target.document_id == term.document_id))
        cross_join(summary, allow_cartesian=True)
        group_by(query_id=query.query_id, document_id=term.document_id, scope_id=target.scope_id)
        return DocumentBm25Score.base(target)(
            score_bm25=sum_(self._bm25_term(term, summary)),
        )

    @step(
        input=[ScoreBase.expanded_query_terms, ScoreBase.section_terms, ScoreBase.targets, section_summary],
        output=section_bm25_scores,
    )
    def score_section_bm25(
        self, query: QueryTerm, term: SectionTerm, target: DocumentSearchTarget, summary: SectionIndexSummary
    ) -> SectionBm25Score:
        inner_join(on=term.term == query.token)
        inner_join(target, on=(target.query_id == query.query_id) & (target.document_id == term.document_id))
        cross_join(summary, allow_cartesian=True)
        group_by(query_id=query.query_id, document_id=term.document_id, section_id=term.section_id, scope_id=target.scope_id)
        return SectionBm25Score.project(target)(
            section_id=term.section_id,
            score_bm25=sum_(self._bm25_term(term, summary)),
        )

    @step(
        input=[ScoreBase.expanded_query_terms, ScoreBase.paragraph_terms, ScoreBase.targets, paragraph_summary],
        output=paragraph_bm25_scores,
    )
    def score_paragraph_bm25(
        self, query: QueryTerm, term: ParagraphTerm, target: DocumentSearchTarget, summary: ParagraphIndexSummary
    ) -> ParagraphBm25Score:
        inner_join(on=term.term == query.token)
        inner_join(target, on=(target.query_id == query.query_id) & (target.document_id == term.document_id))
        cross_join(summary, allow_cartesian=True)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            scope_id=target.scope_id,
        )
        return ParagraphBm25Score.project(target)(
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            score_bm25=sum_(self._bm25_term(term, summary)),
        )

    @step(
        input=[ScoreBase.expanded_query_terms, ScoreBase.sentence_terms, ScoreBase.targets, sentence_summary],
        output=sentence_bm25_scores,
    )
    def score_sentence_bm25(
        self, query: QueryTerm, term: SentenceTerm, target: DocumentSearchTarget, summary: SentenceIndexSummary
    ) -> SentenceBm25Score:
        inner_join(on=term.term == query.token)
        inner_join(target, on=(target.query_id == query.query_id) & (target.document_id == term.document_id))
        cross_join(summary, allow_cartesian=True)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
            scope_id=target.scope_id,
        )
        return SentenceBm25Score.project(target)(
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
            score_bm25=sum_(self._bm25_term(term, summary)),
        )

    def _bm25_term(
        self,
        term: DocumentTerm | SectionTerm | ParagraphTerm | SentenceTerm,
        summary: DocumentIndexSummary | SectionIndexSummary | ParagraphIndexSummary | SentenceIndexSummary,
    ) -> object:
        inverse_frequency = log(
            1.0 + (summary.target_count - term.target_frequency + 0.5) / (term.target_frequency + 0.5)
        )
        normalization = term.term_frequency + self.k1 * (
            1.0 - self.b + self.b * term.target_term_count / summary.average_target_length
        )
        return inverse_frequency * term.term_frequency * (self.k1 + 1.0) / normalization
