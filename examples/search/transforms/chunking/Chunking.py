"""Default Search document chunking composition."""

from examples.search.schemas.text import Document, Paragraph, Section, Sentence
from examples.search.transforms.chunking.DocumentChunking import DocumentChunking
from examples.search.transforms.chunking.SentenceChunking import SentenceChunking
from structure import Transform, input, output


class Chunking(Transform):
    """Chunk documents into pieces."""

    documents = input(Document)
    sections = output(Section)
    paragraphs = output(Paragraph)
    sentences = output(Sentence)

    documents_chunked = DocumentChunking(documents=documents)
    sentences_chunked = SentenceChunking(documents=documents, paragraphs=documents_chunked.paragraphs)
