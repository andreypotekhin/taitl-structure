"""Ranked sentence-search presentation."""

from examples.search.schemas.search import SearchQuery, SentenceScore, SentenceSearchResult
from examples.search.schemas.text import Document, Sentence
from examples.search.transforms.chunking.MaterializeText import _TextMaterializer
from structure import Transform, input, output, step
from structure.plugin.pyspark import inner_join, row_number, where


class SearchSentences(Transform):
    """Rank pre-scored sentence matches for caller-supplied queries."""

    queries = input(SearchQuery)
    documents = input(Document)
    sentences = input(Sentence)
    sentence_scores = input(SentenceScore)
    results = output(SentenceSearchResult)

    @step(input=[sentence_scores, sentences, queries, documents], output=results)
    def rank_sentences(
        self,
        score: SentenceScore,
        sentence: Sentence,
        query: SearchQuery,
        document: Document,
    ) -> SentenceSearchResult:
        inner_join(query, on=query.id == score.query_id)
        inner_join(on=sentence.id == score.sentence_id)
        inner_join(on=document.id == score.document_id)
        where(
            score.score.is_not_null(),
        )
        return SentenceSearchResult.project(score)(
            search_query_id=score.query_id,
            rank=row_number(
                partition_by=(score.query_id, score.experiment_id),
                order_by=(
                    score.score.desc_nulls_last(),
                    score.document_id.asc_nulls_first(),
                    score.sentence_id.asc_nulls_first(),
                ),
            ),
            document_id=score.document_id,
            section_id=score.section_id,
            paragraph_id=score.paragraph_id,
            sentence_id=score.sentence_id,
            content=_TextMaterializer.canonical_span(document.content, sentence.span_start, sentence.span_end),
            score=score.score,
        )
