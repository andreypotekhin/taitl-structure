"""Turn sentence chunks into normalized Search words."""

from examples.search.algorithms.text import normalized_token
from examples.search.schemas.chunking.chunk import ExpandedWordText, WordText
from examples.search.schemas.text import Sentence, Word
from structure import Transform, input, output, step
from structure.plugin.pyspark import arr_transform, concat_ws, posexplode_struct, split, types, where


class WordChunking(Transform):
    """Expand sentence chunks into normalized word rows."""

    sentences = input(Sentence)
    words = output(Word)

    @step(input=sentences, output=words)
    def chunk(self, sentence: Sentence) -> Word:
        word_texts = arr_transform(
            split(sentence.content, pattern=r"\s+"),
            lambda token: WordText(word_token=token),
        )
        word = posexplode_struct(word_texts, as_=ExpandedWordText, ordinal="position", scope="word_text")
        token = normalized_token(word.word_token)
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
