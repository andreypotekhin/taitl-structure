from examples.search.schemas.analytics import CorpusStatistics, CorpusVocabulary, DocumentStatistics
from examples.search.schemas.indexing.lexical.index import DocumentTerm
from structure import *
from structure.plugin.pyspark import *


class CorpusText(Transform):
    """Reduce document-level metrics to one corpus profile."""

    document_statistics = input(DocumentStatistics)
    document_terms = input(DocumentTerm)
    corpus_statistics = output(CorpusStatistics)
    corpus_vocabulary = output(CorpusVocabulary)

    @step(input=document_statistics, output=corpus_statistics)
    def corpus_stats(self, row: DocumentStatistics) -> CorpusStatistics:
        group_by(corpus="all similarity")
        return CorpusStatistics(
            corpus="all similarity",
            document_count=count(),
            average_sections_per_document=avg(row.section_count),
            average_paragraphs_per_document=avg(row.paragraph_count),
            average_sentences_per_document=avg(row.sentence_count),
            average_words_per_document=avg(row.word_count),
            average_distinct_words_per_document=avg(row.distinct_words),
            median_document_average_word_length=approx_percentile(row.average_word_length, 0.5, accuracy=100),
            document_average_word_length_skewness=skewness(row.average_word_length),
            document_average_word_length_kurtosis=kurtosis(row.average_word_length),
        )

    @step(input=document_terms, output=corpus_vocabulary)
    def corpus_vocabulary_stats(self, term: DocumentTerm) -> CorpusVocabulary:
        group_by(corpus="all similarity")
        return CorpusVocabulary(
            corpus="all similarity",
            estimated_distinct_words=approx_count_distinct(term.term),
        )
