"""Overlap scoring from reusable text-index artifacts."""

from examples.search.schemas.indexing.lexical.index import (
    DocumentIndexTerm,
    ParagraphIndexTerm,
    SectionIndexTerm,
    SentenceIndexTerm,
)
from examples.search.schemas.scoring.intermediate import (
    DocumentOverlapMatch,
    ParagraphOverlapMatch,
    QueryTerm,
    QueryTermCount,
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
from examples.search.transforms.scoring.ScoreBase import ScoreBase
from structure import input, lane, output, step
from structure.plugin.pyspark import count_distinct, cross_join, group_by, inner_join, when


class ScoreOverlap(ScoreBase):
    """Score each query against reusable indexes at four target grains."""

    document_overlap_matches = lane(DocumentOverlapMatch)
    section_overlap_matches = lane(SectionOverlapMatch)
    paragraph_overlap_matches = lane(ParagraphOverlapMatch)
    sentence_overlap_matches = lane(SentenceOverlapMatch)
    score_policy = input(ScorePolicy)
    document_overlap_scores = output(DocumentOverlapScore)
    section_overlap_scores = output(SectionOverlapScore)
    paragraph_overlap_scores = output(ParagraphOverlapScore)
    sentence_overlap_scores = output(SentenceOverlapScore)

    @step(input=[ScoreBase.query_terms, ScoreBase.query_sizes, ScoreBase.document_terms], output=document_overlap_matches)
    def match_documents(self, query: QueryTerm, size: QueryTermCount, term: DocumentIndexTerm) -> DocumentOverlapMatch:
        inner_join(on=term.token == query.token)
        inner_join(on=size.query_id == query.query_id)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            query_terms=size.query_terms,
            target_distinct_terms=term.target_distinct_terms,
        )
        return DocumentOverlapMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            query_terms=size.query_terms,
            target_distinct_terms=term.target_distinct_terms,
            matched_terms=count_distinct(query.token),
        )

    @step(input=[ScoreBase.query_terms, ScoreBase.query_sizes, ScoreBase.section_terms], output=section_overlap_matches)
    def match_sections(self, query: QueryTerm, size: QueryTermCount, term: SectionIndexTerm) -> SectionOverlapMatch:
        inner_join(on=term.token == query.token)
        inner_join(on=size.query_id == query.query_id)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            query_terms=size.query_terms,
            target_distinct_terms=term.target_distinct_terms,
        )
        return SectionOverlapMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            query_terms=size.query_terms,
            target_distinct_terms=term.target_distinct_terms,
            matched_terms=count_distinct(query.token),
        )

    @step(input=[ScoreBase.query_terms, ScoreBase.query_sizes, ScoreBase.paragraph_terms], output=paragraph_overlap_matches)
    def match_paragraphs(
        self, query: QueryTerm, size: QueryTermCount, term: ParagraphIndexTerm
    ) -> ParagraphOverlapMatch:
        inner_join(on=term.token == query.token)
        inner_join(on=size.query_id == query.query_id)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            query_terms=size.query_terms,
            target_distinct_terms=term.target_distinct_terms,
        )
        return ParagraphOverlapMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            query_terms=size.query_terms,
            target_distinct_terms=term.target_distinct_terms,
            matched_terms=count_distinct(query.token),
        )

    @step(input=[ScoreBase.query_terms, ScoreBase.query_sizes, ScoreBase.sentence_terms], output=sentence_overlap_matches)
    def match_sentences(
        self, query: QueryTerm, size: QueryTermCount, term: SentenceIndexTerm
    ) -> SentenceOverlapMatch:
        inner_join(on=term.token == query.token)
        inner_join(on=size.query_id == query.query_id)
        group_by(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
            query_terms=size.query_terms,
            target_distinct_terms=term.target_distinct_terms,
        )
        return SentenceOverlapMatch(
            query_id=query.query_id,
            document_id=term.document_id,
            section_id=term.section_id,
            paragraph_id=term.paragraph_id,
            sentence_id=term.sentence_id,
            query_terms=size.query_terms,
            target_distinct_terms=term.target_distinct_terms,
            matched_terms=count_distinct(query.token),
        )

    @step(input=[document_overlap_matches, score_policy], output=document_overlap_scores)
    def publish_document_overlap_scores(
        self, match: DocumentOverlapMatch, policy: ScorePolicy
    ) -> DocumentOverlapScore:
        cross_join(policy, allow_cartesian=True)
        return DocumentOverlapScore(
            query_id=match.query_id,
            document_id=match.document_id,
            scored_at=policy.scored_at,
            score_overlap=self._overlap_score(match),
        )

    @step(input=[section_overlap_matches, score_policy], output=section_overlap_scores)
    def publish_section_overlap_scores(
        self, match: SectionOverlapMatch, policy: ScorePolicy
    ) -> SectionOverlapScore:
        cross_join(policy, allow_cartesian=True)
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
        cross_join(policy, allow_cartesian=True)
        return ParagraphOverlapScore(
            query_id=match.query_id,
            document_id=match.document_id,
            section_id=match.section_id,
            paragraph_id=match.paragraph_id,
            scored_at=policy.scored_at,
            score_overlap=self._overlap_score(match),
        )

    @step(input=[sentence_overlap_matches, score_policy], output=sentence_overlap_scores)
    def publish_sentence_overlap_scores(
        self, match: SentenceOverlapMatch, policy: ScorePolicy
    ) -> SentenceOverlapScore:
        cross_join(policy, allow_cartesian=True)
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
        denominator = when(
            match.query_terms < match.target_distinct_terms,
            match.query_terms,
        ).otherwise(match.target_distinct_terms)
        return match.matched_terms / denominator
