"""Ranked paragraph-search presentation with immediate answer context."""

from examples.search.schemas.search import ParagraphContext, PassageSearchResult, SearchQuery
from examples.search.schemas.text import Document, Paragraph, Section
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import inner_join, lag, lead, row_number, where


class SearchPassages(Transform):
    """Rank scored paragraphs and expose one same-section neighbor on either side."""

    queries = input(SearchQuery)
    scored_paragraphs = input(Paragraph)
    paragraphs = input(Paragraph)
    sections = input(Section)
    documents = input(Document)
    contexts = lane(ParagraphContext)
    results = output(PassageSearchResult)

    @step(input=paragraphs, output=contexts)
    def add_context(self, paragraph: Paragraph) -> ParagraphContext:
        return ParagraphContext.base(paragraph)(
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

    @step(input=[scored_paragraphs, queries, contexts, sections, documents], output=results)
    def rank_passages(
        self,
        paragraph: Paragraph,
        query: SearchQuery,
        context: ParagraphContext,
        section: Section,
        document: Document,
    ) -> PassageSearchResult:
        inner_join(on=query.id == paragraph.search_query_id)
        inner_join(on=context.paragraph_id == paragraph.id)
        inner_join(on=section.id == paragraph.section_id)
        inner_join(on=document.id == paragraph.document_id)
        where(
            paragraph.search_query_id.is_not_null(),
            paragraph.score_overlap.is_not_null(),
            paragraph.score_bm25.is_not_null(),
        )
        return PassageSearchResult.base(paragraph, document, section, context)(
            search_query_id=query.id,
            rank=row_number(
                partition_by=query.id,
                order_by=(
                    paragraph.score_bm25.desc_nulls_last(),
                    paragraph.score_overlap.desc_nulls_last(),
                    paragraph.document_id.asc_nulls_first(),
                    paragraph.id.asc_nulls_first(),
                ),
            ),
            section_heading=section.heading,
            paragraph_id=paragraph.id,
        )
