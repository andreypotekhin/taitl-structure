"""Overlap scoring from reusable text-index artifacts."""

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
from examples.search.schemas.scoring.intermediate import (
    DocumentOverlapMatch,
    ParagraphOverlapMatch,
    QueryIdfTotal,
    QueryTerm,
    QueryTermIdf,
    SectionOverlapMatch,
    SentenceOverlapMatch,
)
from examples.search.schemas.scoring.overlap import (
    DocumentOverlapScore,
    ParagraphOverlapScore,
    SectionOverlapScore,
    SentenceOverlapScore,
)
from examples.search.schemas.search import ScorePolicy
from examples.search.transforms.scoring.lexical.ScoreBase import ScoreBase
from structure import input, lane, output, step
from structure.plugin.pyspark import (
    coalesce,
    cross_join,
    drop_duplicates,
    group_by,
    inner_join,
    left_join,
    log,
    param_join,
)
from structure.plugin.pyspark import sum as sum_
from structure.plugin.pyspark import when


class ScoreOverlap(ScoreBase):
    """Score each query against reusable indexes at four target grains."""

    document_overlap_matches = lane(DocumentOverlapMatch)
    section_overlap_matches = lane(SectionOverlapMatch)
    paragraph_overlap_matches = lane(ParagraphOverlapMatch)
    sentence_overlap_matches = lane(SentenceOverlapMatch)
    document_vocabulary = lane(DocumentTerm)
    section_vocabulary = lane(SectionTerm)
    paragraph_vocabulary = lane(ParagraphTerm)
    sentence_vocabulary = lane(SentenceTerm)
    document_query_idfs = lane(QueryTermIdf)
    section_query_idfs = lane(QueryTermIdf)
    paragraph_query_idfs = lane(QueryTermIdf)
    sentence_query_idfs = lane(QueryTermIdf)
    document_query_totals = lane(QueryIdfTotal)
    section_query_totals = lane(QueryIdfTotal)
    paragraph_query_totals = lane(QueryIdfTotal)
    sentence_query_totals = lane(QueryIdfTotal)
    document_summary = input(DocumentIndexSummary)
    section_summary = input(SectionIndexSummary)
    paragraph_summary = input(ParagraphIndexSummary)
    sentence_summary = input(SentenceIndexSummary)
    score_policy = input(ScorePolicy)
    document_overlap_scores = output(DocumentOverlapScore)
    section_overlap_scores = output(SectionOverlapScore)
    paragraph_overlap_scores = output(ParagraphOverlapScore)
    sentence_overlap_scores = output(SentenceOverlapScore)

    @step(input=ScoreBase.document_terms, output=document_vocabulary)
    def select_document_vocabulary(self, term: DocumentTerm) -> DocumentTerm:
        drop_duplicates(term.term)
        return DocumentTerm.project(term)

    @step(input=ScoreBase.section_terms, output=section_vocabulary)
    def select_section_vocabulary(self, term: SectionTerm) -> SectionTerm:
        drop_duplicates(term.term)
        return SectionTerm.project(term)

    @step(input=ScoreBase.paragraph_terms, output=paragraph_vocabulary)
    def select_paragraph_vocabulary(self, term: ParagraphTerm) -> ParagraphTerm:
        drop_duplicates(term.term)
        return ParagraphTerm.project(term)

    @step(input=ScoreBase.sentence_terms, output=sentence_vocabulary)
    def select_sentence_vocabulary(self, term: SentenceTerm) -> SentenceTerm:
        drop_duplicates(term.term)
        return SentenceTerm.project(term)

    @step(input=[ScoreBase.expanded_query_terms, document_vocabulary, document_summary], output=document_query_idfs)
    def weight_document_query_terms(
        self, query: QueryTerm, term: DocumentTerm, summary: DocumentIndexSummary
    ) -> QueryTermIdf:
        left_join(term, on=term.term == query.token)
        cross_join(summary, allow_cartesian=True)
        group_by(query_id=query.query_id, token=query.token)
        target_frequency = coalesce(term.target_frequency, 0)
        return QueryTermIdf(
            query_id=query.query_id,
            token=query.token,
            idf=log(1.0 + (summary.target_count - target_frequency + 0.5) / (target_frequency + 0.5)),
        )

    @step(input=[ScoreBase.expanded_query_terms, section_vocabulary, section_summary], output=section_query_idfs)
    def weight_section_query_terms(
        self, query: QueryTerm, term: SectionTerm, summary: SectionIndexSummary
    ) -> QueryTermIdf:
        left_join(term, on=term.term == query.token)
        cross_join(summary, allow_cartesian=True)
        group_by(query_id=query.query_id, token=query.token)
        target_frequency = coalesce(term.target_frequency, 0)
        return QueryTermIdf(
            query_id=query.query_id,
            token=query.token,
            idf=log(1.0 + (summary.target_count - target_frequency + 0.5) / (target_frequency + 0.5)),
        )

    @step(input=[ScoreBase.expanded_query_terms, paragraph_vocabulary, paragraph_summary], output=paragraph_query_idfs)
    def weight_paragraph_query_terms(
        self, query: QueryTerm, term: ParagraphTerm, summary: ParagraphIndexSummary
    ) -> QueryTermIdf:
        left_join(term, on=term.term == query.token)
        cross_join(summary, allow_cartesian=True)
        group_by(query_id=query.query_id, token=query.token)
        target_frequency = coalesce(term.target_frequency, 0)
        return QueryTermIdf(
            query_id=query.query_id,
            token=query.token,
            idf=log(1.0 + (summary.target_count - target_frequency + 0.5) / (target_frequency + 0.5)),
        )

    @step(input=[ScoreBase.expanded_query_terms, sentence_vocabulary, sentence_summary], output=sentence_query_idfs)
    def weight_sentence_query_terms(
        self, query: QueryTerm, term: SentenceTerm, summary: SentenceIndexSummary
    ) -> QueryTermIdf:
        left_join(term, on=term.term == query.token)
        cross_join(summary, allow_cartesian=True)
        group_by(query_id=query.query_id, token=query.token)
        target_frequency = coalesce(term.target_frequency, 0)
        return QueryTermIdf(
            query_id=query.query_id,
            token=query.token,
            idf=log(1.0 + (summary.target_count - target_frequency + 0.5) / (target_frequency + 0.5)),
        )

    @step(input=document_query_idfs, output=document_query_totals)
    def total_document_query_idf(self, term: QueryTermIdf) -> QueryIdfTotal:
        group_by(query_id=term.query_id)
        return QueryIdfTotal(query_id=term.query_id, query_idf=sum_(term.idf))

    @step(input=section_query_idfs, output=section_query_totals)
    def total_section_query_idf(self, term: QueryTermIdf) -> QueryIdfTotal:
        group_by(query_id=term.query_id)
        return QueryIdfTotal(query_id=term.query_id, query_idf=sum_(term.idf))

    @step(input=paragraph_query_idfs, output=paragraph_query_totals)
    def total_paragraph_query_idf(self, term: QueryTermIdf) -> QueryIdfTotal:
        group_by(query_id=term.query_id)
        return QueryIdfTotal(query_id=term.query_id, query_idf=sum_(term.idf))

    @step(input=sentence_query_idfs, output=sentence_query_totals)
    def total_sentence_query_idf(self, term: QueryTermIdf) -> QueryIdfTotal:
        group_by(query_id=term.query_id)
        return QueryIdfTotal(query_id=term.query_id, query_idf=sum_(term.idf))

    @step(
        input=[ScoreBase.expanded_query_terms, ScoreBase.document_terms, document_query_idfs, document_query_totals],
        output=document_overlap_matches,
    )
    def match_documents(
        self, query: QueryTerm, term: DocumentTerm, weight: QueryTermIdf, total: QueryIdfTotal
    ) -> DocumentOverlapMatch:
        inner_join(on=term.term == query.token)
        inner_join(on=(weight.query_id == query.query_id) & (weight.token == query.token))
        inner_join(on=total.query_id == query.query_id)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            query_idf=total.query_idf,
        )
        return DocumentOverlapMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            query_idf=total.query_idf,
            matched_idf=sum_(weight.idf),
        )

    @step(
        input=[ScoreBase.expanded_query_terms, ScoreBase.section_terms, section_query_idfs, section_query_totals],
        output=section_overlap_matches,
    )
    def match_sections(
        self, query: QueryTerm, term: SectionTerm, weight: QueryTermIdf, total: QueryIdfTotal
    ) -> SectionOverlapMatch:
        inner_join(on=term.term == query.token)
        inner_join(on=(weight.query_id == query.query_id) & (weight.token == query.token))
        inner_join(on=total.query_id == query.query_id)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            query_idf=total.query_idf,
        )
        return SectionOverlapMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            query_idf=total.query_idf,
            matched_idf=sum_(weight.idf),
        )

    @step(
        input=[ScoreBase.expanded_query_terms, ScoreBase.paragraph_terms, paragraph_query_idfs, paragraph_query_totals],
        output=paragraph_overlap_matches,
    )
    def match_paragraphs(
        self, query: QueryTerm, term: ParagraphTerm, weight: QueryTermIdf, total: QueryIdfTotal
    ) -> ParagraphOverlapMatch:
        inner_join(on=term.term == query.token)
        inner_join(on=(weight.query_id == query.query_id) & (weight.token == query.token))
        inner_join(on=total.query_id == query.query_id)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            query_idf=total.query_idf,
        )
        return ParagraphOverlapMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            query_idf=total.query_idf,
            matched_idf=sum_(weight.idf),
        )

    @step(
        input=[ScoreBase.expanded_query_terms, ScoreBase.sentence_terms, sentence_query_idfs, sentence_query_totals],
        output=sentence_overlap_matches,
    )
    def match_sentences(
        self, query: QueryTerm, term: SentenceTerm, weight: QueryTermIdf, total: QueryIdfTotal
    ) -> SentenceOverlapMatch:
        inner_join(on=term.term == query.token)
        inner_join(on=(weight.query_id == query.query_id) & (weight.token == query.token))
        inner_join(on=total.query_id == query.query_id)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
            query_idf=total.query_idf,
        )
        return SentenceOverlapMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
            query_idf=total.query_idf,
            matched_idf=sum_(weight.idf),
        )

    @step(input=[document_overlap_matches, score_policy], output=document_overlap_scores)
    def publish_document_overlap_scores(self, match: DocumentOverlapMatch, policy: ScorePolicy) -> DocumentOverlapScore:
        param_join(policy)
        return DocumentOverlapScore(
            query_id=match.query_id,
            document_id=match.document_id,
            scored_at=policy.scored_at,
            score_overlap=self._overlap_score(match),
        )

    @step(input=[section_overlap_matches, score_policy], output=section_overlap_scores)
    def publish_section_overlap_scores(self, match: SectionOverlapMatch, policy: ScorePolicy) -> SectionOverlapScore:
        param_join(policy)
        return SectionOverlapScore(
            query_id=match.query_id,
            document_id=match.document_id,
            section_id=match.section_id,
            scored_at=policy.scored_at,
            score_overlap=self._overlap_score(match),
        )

    @step(input=[paragraph_overlap_matches, score_policy], output=paragraph_overlap_scores)
    def publish_paragraph_overlap_scores(
        self, match: ParagraphOverlapMatch, policy: ScorePolicy
    ) -> ParagraphOverlapScore:
        param_join(policy)
        return ParagraphOverlapScore(
            query_id=match.query_id,
            document_id=match.document_id,
            section_id=match.section_id,
            paragraph_id=match.paragraph_id,
            scored_at=policy.scored_at,
            score_overlap=self._overlap_score(match),
        )

    @step(input=[sentence_overlap_matches, score_policy], output=sentence_overlap_scores)
    def publish_sentence_overlap_scores(self, match: SentenceOverlapMatch, policy: ScorePolicy) -> SentenceOverlapScore:
        param_join(policy)
        return SentenceOverlapScore(
            query_id=match.query_id,
            document_id=match.document_id,
            section_id=match.section_id,
            paragraph_id=match.paragraph_id,
            sentence_id=match.sentence_id,
            scored_at=policy.scored_at,
            score_overlap=self._overlap_score(match),
        )

    def _overlap_score(
        self,
        match: DocumentOverlapMatch | SectionOverlapMatch | ParagraphOverlapMatch | SentenceOverlapMatch,
    ) -> object:
        return when(match.query_idf > 0.0, match.matched_idf / match.query_idf).otherwise(0.0)
