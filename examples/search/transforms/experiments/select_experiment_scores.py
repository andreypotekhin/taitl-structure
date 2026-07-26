"""Select production and current experiment score rows."""

from examples.search.schemas.experiment import Experiment
from examples.search.schemas.search import DocumentScore, ParagraphScore, SectionScore, SentenceScore
from structure import Transform, input, output, step
from structure.plugin.pyspark import left_join, where


class SelectExperimentScores(Transform):
    """Keep implicit production scores and scores for current named experiments."""

    experiments = input(Experiment)
    document_scores = input(DocumentScore)
    section_scores = input(SectionScore)
    paragraph_scores = input(ParagraphScore)
    sentence_scores = input(SentenceScore)
    active_document_scores = output(DocumentScore)
    active_section_scores = output(SectionScore)
    active_paragraph_scores = output(ParagraphScore)
    active_sentence_scores = output(SentenceScore)

    @step(input=[document_scores, experiments], output=active_document_scores)
    def select_documents(self, score: DocumentScore, experiment: Experiment) -> DocumentScore:
        left_join(on=experiment.experiment_id == score.experiment_id)
        where((score.experiment_id == "") | experiment.is_active)
        return DocumentScore.base(score)

    @step(input=[section_scores, experiments], output=active_section_scores)
    def select_sections(self, score: SectionScore, experiment: Experiment) -> SectionScore:
        left_join(on=experiment.experiment_id == score.experiment_id)
        where((score.experiment_id == "") | experiment.is_active)
        return SectionScore.base(score)

    @step(input=[paragraph_scores, experiments], output=active_paragraph_scores)
    def select_paragraphs(self, score: ParagraphScore, experiment: Experiment) -> ParagraphScore:
        left_join(on=experiment.experiment_id == score.experiment_id)
        where((score.experiment_id == "") | experiment.is_active)
        return ParagraphScore.base(score)

    @step(input=[sentence_scores, experiments], output=active_sentence_scores)
    def select_sentences(self, score: SentenceScore, experiment: Experiment) -> SentenceScore:
        left_join(on=experiment.experiment_id == score.experiment_id)
        where((score.experiment_id == "") | experiment.is_active)
        return SentenceScore.base(score)
