"""Ranked paragraph-search presentation with immediate answer context."""

from examples.search.schemas.search import ParagraphContext, ParagraphScore, PassageSearchResult, SearchQuery
from examples.search.schemas.text import Document, Paragraph, Section
from examples.search.transforms.chunking.MaterializeText import _TextMaterializer
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import coalesce, inner_join, lag, lead, regexp_replace, row_number, when, where


class SearchPassages(Transform):
    """Rank scored paragraphs and expose one same-section neighbor on either side."""

    queries = input(SearchQuery)
    paragraph_scores = input(ParagraphScore)
    paragraphs = input(Paragraph)
    sections = input(Section)
    documents = input(Document)
    contexts = lane(ParagraphContext)
    results = output(PassageSearchResult)

    @step(input=[documents, paragraphs], output=contexts)
    def add_context(self, document: Document, paragraph: Paragraph) -> ParagraphContext:
        inner_join(on=document.id == paragraph.document_id)
        content = regexp_replace(
            _TextMaterializer.canonical_span(document.content, paragraph.span_start, paragraph.span_end),
            pattern="\n",
            replacement=" ",
        )
        return ParagraphContext.project(paragraph)(
            paragraph_id=paragraph.id,
            document_id=paragraph.document_id,
            section_id=paragraph.section_id,
            content=content,
            preceding_content=lag(
                content,
                partition_by=(paragraph.document_id, paragraph.section_id),
                order_by=paragraph.ordinal,
            ),
            following_content=lead(
                content,
                partition_by=(paragraph.document_id, paragraph.section_id),
                order_by=paragraph.ordinal,
            ),
        )

    @step(
        input=[paragraph_scores, queries, contexts, sections, documents],
        output=results,
    )
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
        heading = when(
            section.heading_span_start.is_not_null(),
            _TextMaterializer.canonical_span(document.content, section.heading_span_start, section.heading_span_end),
        ).otherwise("Document")
        heading = coalesce(heading, "Document")
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
            section_heading=heading,
            paragraph_id=score.paragraph_id,
            content=context.content,
        )
