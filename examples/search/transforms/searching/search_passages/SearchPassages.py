"""Ranked paragraph-search presentation with immediate answer context."""

from examples.search.schemas.search import ParagraphContext, ParagraphScore, PassageSearchResult, SearchQuery
from examples.search.schemas.text import Document, Paragraph, Section
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import inner_join, lag, lead, row_number, where


class SearchPassages(Transform):
    """Rank scored paragraphs and expose one same-section neighbor on either side."""

    queries = input(SearchQuery)
    paragraph_scores = input(ParagraphScore)
    paragraphs = input(Paragraph)
    sections = input(Section)
    documents = input(Document)
    contexts = lane(ParagraphContext)
    results = output(PassageSearchResult)

    @step(input=paragraphs, output=contexts)
    def add_context(self, paragraph: Paragraph) -> ParagraphContext:
        return ParagraphContext.project(paragraph)(
            paragraph_id=paragraph.id,
            preceding_content=lag(
                paragraph.content,
                partition_by=(paragraph.document_id, paragraph.section_id),
                order_by=paragraph.ordinal,
            ),
            following_content=lead(
                paragraph.content,
                partition_by=(paragraph.document_id, paragraph.section_id),
                order_by=paragraph.ordinal,
            ),
        )

    @step(input=[paragraph_scores, queries, contexts, sections, documents], output=results)
    def rank_passages(
        self,
        score: ParagraphScore,
        query: SearchQuery,
        context: ParagraphContext,
        section: Section,
        document: Document,
    ) -> PassageSearchResult:
        inner_join(on=query.id == score.query_id)
        inner_join(on=context.paragraph_id == score.paragraph_id)
        inner_join(on=section.id == score.section_id)
        inner_join(on=document.id == score.document_id)
        where(
            score.score.is_not_null(),
        )
        return PassageSearchResult.project(score, document, section, context)(
            search_query_id=query.id,
            rank=row_number(
                partition_by=(query.id, score.experiment_id),
                order_by=(
                    score.score.desc_nulls_last(),
                    score.document_id.asc_nulls_first(),
                    score.paragraph_id.asc_nulls_first(),
                ),
            ),
            document_id=score.document_id,
            section_id=score.section_id,
            section_heading=section.heading,
            paragraph_id=score.paragraph_id,
            content=context.content,
        )
