"""Default Search document chunking composition."""

from examples.search.schemas.text import Document, Paragraph, Section, Sentence, Word
from examples.search.transforms.chunking.DocumentChunking import DocumentChunking
from examples.search.transforms.chunking.SentenceChunking import SentenceChunking
from examples.search.transforms.chunking.WordChunking import WordChunking
from structure import Transform, input, output, stage


class Chunking(Transform):
    """Chunk documents into pieces."""

    documents = input(Document)
    sections = output(Section)
    paragraphs = output(Paragraph)
    sentences = output(Sentence)
    words = output(Word)

    documents_chunked = stage(DocumentChunking(documents=documents))
    sentences_chunked = stage(SentenceChunking(paragraphs=documents_chunked.paragraphs))
    words_chunked = stage(WordChunking(sentences=sentences_chunked.sentences))
