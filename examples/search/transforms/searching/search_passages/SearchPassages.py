"""Ranked paragraph-search presentation with immediate answer context."""

from examples.search.schemas.chunking.intermediate import MaterializedParagraph, MaterializedSection
from examples.search.schemas.search import ParagraphContext, ParagraphScore, PassageSearchResult, SearchQuery
from examples.search.schemas.text import Document, Paragraph, Section
from examples.search.transforms.chunking.MaterializeText import _TextMaterializer
from structure import input, lane, output, step
from structure.plugin.pyspark import coalesce, inner_join, lag, lead, regexp_replace, row_number, when, where


class SearchPassages(_TextMaterializer):
    """Rank scored paragraphs and expose one same-section neighbor on either side."""

    queries = input(SearchQuery)
    paragraph_scores = input(ParagraphScore)
    paragraphs = input(Paragraph)
    sections = input(Section)
    documents = input(Document)
    contexts = lane(ParagraphContext)
    results = output(PassageSearchResult)
    materialized_paragraph = lane(MaterializedParagraph)
    materialized_section = lane(MaterializedSection)

    @step(input=[documents, paragraphs], output=materialized_paragraph)
    def materialize_paragraph(self, document: Document, paragraph: Paragraph) -> MaterializedParagraph:
        inner_join(on=document.id == paragraph.document_id)
        return MaterializedParagraph(
            id=paragraph.id,
            document_id=paragraph.document_id,
            section_id=paragraph.section_id,
            ordinal=paragraph.ordinal,
            span_start=paragraph.span_start,
            span_end=paragraph.span_end,
            content=regexp_replace(
                self.canonical_span(document.content, paragraph.span_start, paragraph.span_end),
                pattern="\n",
                replacement=" ",
            ),
        )

    @step(input=[documents, sections], output=materialized_section)
    def materialize_section(self, document: Document, section: Section) -> MaterializedSection:
        inner_join(on=document.id == section.document_id)
        heading = when(
            section.heading_span_start.is_not_null(),
            self.canonical_span(document.content, section.heading_span_start, section.heading_span_end),
        ).otherwise("Document")
        return MaterializedSection(
            id=section.id,
            document_id=section.document_id,
            ordinal=section.ordinal,
            span_start=section.span_start,
            span_end=section.span_end,
            heading_span_start=section.heading_span_start,
            heading_span_end=section.heading_span_end,
            heading=coalesce(heading, "Document"),
        )

    @step(input=materialized_paragraph, output=contexts)
    def add_context(self, paragraph: MaterializedParagraph) -> ParagraphContext:
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

    @step(
        input=[paragraph_scores, queries, contexts, sections, documents, materialized_section],
        output=results,
    )
    def rank_passages(
        self,
        score: ParagraphScore,
        query: SearchQuery,
        context: ParagraphContext,
        section: Section,
        document: Document,
        materialized_section: MaterializedSection,
    ) -> PassageSearchResult:
        inner_join(on=query.id == score.query_id)
        inner_join(on=context.paragraph_id == score.paragraph_id)
        inner_join(on=section.id == score.section_id)
        inner_join(on=document.id == score.document_id)
        inner_join(on=materialized_section.id == section.id)
        where(
            score.score.is_not_null(),
        )
        return PassageSearchResult.project(score, document, context)(
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
            section_heading=materialized_section.heading,
            paragraph_id=score.paragraph_id,
            content=context.content,
        )
