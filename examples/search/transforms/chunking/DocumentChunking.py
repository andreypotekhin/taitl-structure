"""Chunk caller-provided documents into span-only sections and paragraphs."""

from typing import Any

from examples.search.schemas.chunking.intermediate import (
    DocumentLine,
    ExpandedDocumentLine,
    MarkedDocumentLine,
    ParagraphContent,
    ParagraphDraft,
    ParagraphLine,
    ParagraphLineGroup,
    SectionHeading,
    SectionKey,
)
from examples.search.schemas.text import Document, Paragraph, Section
from structure import Transform, input, lane, output, special, step
from structure.plugin.pyspark import (
    concat_ws,
    current_row,
    group_by,
    left_join,
    max,
    min,
    posexplode_struct,
    row_number,
    rows_between,
    types,
    unbounded_preceding,
    when,
    where,
    window,
    window_sum,
)


class DocumentChunking(Transform):
    """Chunk caller-provided documents into document-local half-open spans."""

    documents = input(Document)
    marked_lines = lane(MarkedDocumentLine)
    paragraph_lines = lane(ParagraphLine)
    section_headings = lane(SectionHeading)
    paragraph_line_groups = lane(ParagraphLineGroup)
    paragraph_content = lane(ParagraphContent)
    paragraph_drafts = lane(ParagraphDraft)
    section_keys = lane(SectionKey)
    sections = output(Section)
    paragraphs = output(Paragraph)

    @special(
        type="udf",
        return_type=types.array(types.struct(DocumentLine), contains_null=False),
        nullable=False,
    )
    def canonical_document_lines(content: Any) -> list[dict[str, object]]:
        """Return canonical lines and code-point spans without reconstructing text later."""
        import re

        canonical = re.sub(r"\r\n?", "\n", content)
        lines: list[dict[str, object]] = []
        offset = 0
        for line in canonical.split("\n"):
            match = re.match(r"^\s*#+\s+(.+?)\s*$", line)
            heading = match.group(1).strip() if match else None
            heading_start = offset + match.start(1) if match else None
            heading_end = offset + match.end(1) if match else None
            lines.append(
                {
                    "line": line,
                    "span_start": offset,
                    "span_end": offset + len(line),
                    "heading": heading,
                    "heading_span_start": heading_start,
                    "heading_span_end": heading_end,
                    "is_blank": line.strip() == "",
                }
            )
            offset += len(line) + 1
        return lines

    @step(input=documents, output=marked_lines)
    def mark_lines(self, document: Document) -> MarkedDocumentLine:
        lines = self.canonical_document_lines(document.content)
        line = posexplode_struct(lines, as_=ExpandedDocumentLine, scope="document_line")
        is_heading = line.heading.is_not_null()
        line_window = window(
            partition_by=document.id,
            order_by=line.ordinal,
            frame=rows_between(unbounded_preceding(), current_row()),
        )
        return MarkedDocumentLine(
            document_id=document.id,
            line_ordinal=line.ordinal,
            line=line.line,
            span_start=line.span_start,
            span_end=line.span_end,
            heading=line.heading,
            heading_span_start=line.heading_span_start,
            heading_span_end=line.heading_span_end,
            is_blank=line.is_blank,
            section_ordinal=window_sum(when(is_heading, 1).otherwise(0), over=line_window),
            paragraph_group=window_sum(when(line.is_blank, 1).otherwise(0), over=line_window),
        )

    @step(input=marked_lines, output=paragraph_lines)
    def select_paragraph_lines(self, line: MarkedDocumentLine) -> ParagraphLine:
        where(~line.is_blank & line.heading.is_null())
        return ParagraphLine(
            document_id=line.document_id,
            section_ordinal=line.section_ordinal,
            paragraph_group=line.paragraph_group,
            line_ordinal=line.line_ordinal,
            line=line.line,
            span_start=line.span_start,
            span_end=line.span_end,
        )

    @step(input=marked_lines, output=section_headings)
    def select_section_headings(self, line: MarkedDocumentLine) -> SectionHeading:
        where(line.heading.is_not_null())
        return SectionHeading(
            document_id=line.document_id,
            section_ordinal=line.section_ordinal,
            heading=line.heading,
            heading_span_start=line.heading_span_start,
            heading_span_end=line.heading_span_end,
        )

    @step(input=paragraph_lines, output=paragraph_line_groups)
    def collect_paragraph_lines(self, line: ParagraphLine) -> ParagraphLineGroup:
        paragraph_id = concat_ws("#p", line.document_id, line.paragraph_group.cast(types.string()))
        section_id = concat_ws("#s", line.document_id, line.section_ordinal.cast(types.string()))
        group_by(
            id=paragraph_id,
            document_id=line.document_id,
            section_id=section_id,
            section_ordinal=line.section_ordinal,
            paragraph_group=line.paragraph_group,
        )
        return ParagraphLineGroup(
            id=paragraph_id,
            document_id=line.document_id,
            section_id=section_id,
            section_ordinal=line.section_ordinal,
            paragraph_group=line.paragraph_group,
            span_start=min(line.span_start),
            span_end=max(line.span_end),
        )

    @step(input=paragraph_line_groups, output=paragraph_content)
    def assemble_paragraph_content(self, group: ParagraphLineGroup) -> ParagraphContent:
        return ParagraphContent(
            id=group.id,
            document_id=group.document_id,
            section_id=group.section_id,
            section_ordinal=group.section_ordinal,
            paragraph_group=group.paragraph_group,
            span_start=group.span_start,
            span_end=group.span_end,
        )

    @step(input=paragraph_content, output=paragraph_drafts)
    def number_paragraphs(self, paragraph: ParagraphContent) -> ParagraphDraft:
        ordinal = row_number(
            partition_by=(paragraph.document_id, paragraph.section_ordinal),
            order_by=paragraph.paragraph_group,
        ).cast(types.integer())
        return ParagraphDraft(
            id=paragraph.id,
            document_id=paragraph.document_id,
            section_id=paragraph.section_id,
            section_ordinal=paragraph.section_ordinal,
            ordinal=ordinal,
            span_start=paragraph.span_start,
            span_end=paragraph.span_end,
        )

    @step(input=paragraph_drafts, output=paragraphs)
    def publish_paragraphs(self, paragraph: ParagraphDraft) -> Paragraph:
        return Paragraph(
            id=paragraph.id,
            document_id=paragraph.document_id,
            section_id=paragraph.section_id,
            ordinal=paragraph.ordinal,
            span_start=paragraph.span_start,
            span_end=paragraph.span_end,
        )

    @step(input=paragraph_drafts, output=section_keys)
    def select_section_keys(self, paragraph: ParagraphDraft) -> SectionKey:
        group_by(
            id=paragraph.section_id,
            document_id=paragraph.document_id,
            section_ordinal=paragraph.section_ordinal,
            ordinal=paragraph.section_ordinal.cast(types.integer()),
        )
        return SectionKey(
            id=paragraph.section_id,
            document_id=paragraph.document_id,
            section_ordinal=paragraph.section_ordinal,
            ordinal=paragraph.section_ordinal.cast(types.integer()),
            span_start=min(paragraph.span_start),
            span_end=max(paragraph.span_end),
        )

    @step(input=[section_keys, section_headings], output=sections)
    def build_sections(self, key: SectionKey, heading: SectionHeading) -> Section:
        left_join(
            heading,
            on=(heading.document_id == key.document_id) & (heading.section_ordinal == key.section_ordinal),
        )
        return Section(
            id=key.id,
            document_id=key.document_id,
            ordinal=key.ordinal,
            span_start=key.span_start,
            span_end=key.span_end,
            heading_span_start=heading.heading_span_start,
            heading_span_end=heading.heading_span_end,
        )
