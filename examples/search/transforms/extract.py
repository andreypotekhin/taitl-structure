"""Expand caller-extracted text into searchable hierarchy rows."""

from examples.search.schemas.extraction.extract import (
    DocumentLine,
    ExpandedDocumentLine,
    ExpandedSentenceText,
    ExpandedWordText,
    MarkedDocumentLine,
    ParagraphContent,
    ParagraphDraft,
    ParagraphLine,
    ParagraphLineGroup,
    SectionHeading,
    SectionKey,
    SentenceText,
    WordText,
)
from examples.search.schemas.text import Document, Paragraph, Section, Sentence, Word
from structure import Transform, input, lane, output, step
from structure.plugin.pyspark import (
    arr_transform,
    coalesce,
    collect_list,
    concat_ws,
    current_row,
    drop_duplicates,
    group_by,
    left_join,
    lower,
    posexplode_struct,
    regexp_extract,
    regexp_replace,
    row_number,
    rows_between,
    split,
    trim,
    types,
    unbounded_preceding,
    when,
    where,
    window,
    window_sum,
)


class ExtractText(Transform):
    """Expand caller-extracted text into sections, paragraphs, sentences, and words."""

    documents = input(Document)
    marked_lines = lane(MarkedDocumentLine)
    paragraph_lines = lane(ParagraphLine)
    section_headings = lane(SectionHeading)
    paragraph_line_groups = lane(ParagraphLineGroup)
    paragraph_content = lane(ParagraphContent)
    paragraph_drafts = lane(ParagraphDraft)
    section_keys = lane(SectionKey)
    sentence_rows = lane(Sentence)
    sections = output(Section)
    paragraphs = output(Paragraph)
    sentences = output(Sentence)
    words = output(Word)

    @step(input=documents, output=marked_lines)
    def mark_lines(self, document: Document) -> MarkedDocumentLine:
        lines = arr_transform(
            split(regexp_replace(document.content, pattern="\\r\\n?", replacement="\n"), pattern="\n"),
            lambda line: DocumentLine(line=line),
        )
        line = posexplode_struct(lines, as_=ExpandedDocumentLine, scope="document_line")
        heading = trim(regexp_extract(line.line, pattern=r"^\s*#+\s+(.+?)\s*$", group=1))
        is_heading = heading != ""
        is_blank = trim(line.line) == ""
        line_window = window(
            partition_by=document.id,
            order_by=line.ordinal,
            frame=rows_between(unbounded_preceding(), current_row()),
        )
        return MarkedDocumentLine(
            document_id=document.id,
            line_ordinal=line.ordinal,
            line=line.line,
            heading=when(is_heading, heading).otherwise(None),
            is_blank=is_blank,
            section_ordinal=window_sum(when(is_heading, 1).otherwise(0), over=line_window),
            paragraph_group=window_sum(when(is_blank, 1).otherwise(0), over=line_window),
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
        )

    @step(input=marked_lines, output=section_headings)
    def select_section_headings(self, line: MarkedDocumentLine) -> SectionHeading:
        where(line.heading.is_not_null())
        return SectionHeading(
            document_id=line.document_id,
            section_ordinal=line.section_ordinal,
            heading=line.heading,
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
            lines=collect_list(line.line, order_by=line.line_ordinal),
        )

    @step(input=paragraph_line_groups, output=paragraph_content)
    def assemble_paragraph_content(self, group: ParagraphLineGroup) -> ParagraphContent:
        return ParagraphContent(
            id=group.id,
            document_id=group.document_id,
            section_id=group.section_id,
            section_ordinal=group.section_ordinal,
            paragraph_group=group.paragraph_group,
            content=concat_ws(" ", group.lines),
        )

    @step(input=paragraph_content, output=paragraph_drafts)
    def rank_paragraphs(self, paragraph: ParagraphContent) -> ParagraphDraft:
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
            content=paragraph.content,
            search_query_id=None,
            score_overlap=None,
            score_bm25=None,
        )

    @step(input=paragraph_drafts, output=paragraphs)
    def publish_paragraphs(self, paragraph: ParagraphDraft) -> Paragraph:
        return Paragraph(
            id=paragraph.id,
            document_id=paragraph.document_id,
            section_id=paragraph.section_id,
            ordinal=paragraph.ordinal,
            content=paragraph.content,
            search_query_id=paragraph.search_query_id,
            score_overlap=paragraph.score_overlap,
            score_bm25=paragraph.score_bm25,
        )

    @step(input=paragraph_drafts, output=section_keys)
    def select_section_keys(self, paragraph: ParagraphDraft) -> SectionKey:
        drop_duplicates(paragraph.document_id, paragraph.section_id)
        return SectionKey(
            id=paragraph.section_id,
            document_id=paragraph.document_id,
            section_ordinal=paragraph.section_ordinal,
            ordinal=paragraph.section_ordinal.cast(types.integer()),
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
            heading=coalesce(heading.heading, "Document"),
            search_query_id=None,
            score_overlap=None,
            score_bm25=None,
        )

    @step(input=paragraph_drafts, output=sentence_rows)
    def build_sentences(self, paragraph: ParagraphDraft) -> Sentence:
        sentence_texts = arr_transform(
            split(paragraph.content, pattern=r"(?<=[.!?])\s+"),
            lambda content: SentenceText(sentence_content=content),
        )
        sentence = posexplode_struct(
            sentence_texts,
            as_=ExpandedSentenceText,
            ordinal="position",
            scope="sentence_text",
        )
        content = trim(sentence.sentence_content)
        where(content != "")
        return Sentence(
            id=concat_ws("#s", paragraph.id, sentence.position.cast(types.string())),
            document_id=paragraph.document_id,
            section_id=paragraph.section_id,
            paragraph_id=paragraph.id,
            paragraph_ordinal=paragraph.ordinal,
            ordinal=(sentence.position + 1).cast(types.integer()),
            content=content,
            search_query_id=None,
            score_overlap=None,
            score_bm25=None,
        )

    @step(input=sentence_rows, output=sentences)
    def publish_sentences(self, sentence: Sentence) -> Sentence:
        return Sentence.project(sentence)

    @step(input=sentence_rows, output=words)
    def build_words(self, sentence: Sentence) -> Word:
        word_texts = arr_transform(
            split(sentence.content, pattern=r"\s+"),
            lambda token: WordText(word_token=token),
        )
        word = posexplode_struct(
            word_texts,
            as_=ExpandedWordText,
            ordinal="position",
            scope="word_text",
        )
        token = lower(regexp_replace(trim(word.word_token), pattern=r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", replacement=""))
        where(token != "")
        return Word(
            id=concat_ws("#w", sentence.id, word.position.cast(types.string())),
            document_id=sentence.document_id,
            section_id=sentence.section_id,
            paragraph_id=sentence.paragraph_id,
            paragraph_ordinal=sentence.paragraph_ordinal,
            sentence_id=sentence.id,
            ordinal=(word.position + 1).cast(types.integer()),
            token=token,
        )
