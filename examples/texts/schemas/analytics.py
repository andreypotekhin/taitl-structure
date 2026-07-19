from structure import Schema
from structure.platform.pyspark import *


class DocumentFeatures(Schema):
    document_id = string(nullable=False)
    collection_id = string(nullable=False)
    source = string(nullable=False)
    language = string(nullable=False)
    title = string(nullable=False)
    url = string(nullable=True)
    normalized_title = string(nullable=True)
    normalized_content = string(nullable=False)
    title_starts_with_the = boolean(nullable=True)
    title_ends_with_guide = boolean(nullable=True)
    url_is_https = boolean(nullable=True)
    content_contains_structure = boolean(nullable=False)
    content_like_guide = boolean(nullable=False)
    content_ilike_structure = boolean(nullable=False)
    content_matches_sentence = boolean(nullable=False)
    title_prefix = string(nullable=False)
    title_words = array(string(), contains_null=False, nullable=False)
    leading_title_words = array(string(), contains_null=False, nullable=False)
    sorted_title_words = array(string(), contains_null=False, nullable=False)
    title_slug = string(nullable=False)
    first_number = string(nullable=False)
    content_length = integer(nullable=False)
    title_case = string(nullable=False)
    reversed_title = string(nullable=False)
    translated_title = string(nullable=False)
    structure_position = integer(nullable=False)
    title_distance_to_guide = integer(nullable=False)
    display_name = string(nullable=False)
    title_hash = integer(nullable=False)
    content_sha2 = string(nullable=False)
    harvest_year = integer(nullable=False)
    age_days = integer(nullable=True)
    content_length_sqrt = double(nullable=False)
    content_length_log = double(nullable=False)
    rounded_content_length = double(nullable=False)
    content_length_sign = double(nullable=False)
    source_recency_rank = long(nullable=False)


class SentenceStatistics(Schema):
    sentence_id = string(nullable=False)
    document_id = string(nullable=False)
    paragraph_id = string(nullable=False)
    section_id = string(nullable=False)
    ordinal = integer(nullable=False)
    word_count = long(nullable=False)
    distinct_words = long(nullable=False)
    average_word_length = double(nullable=True)


class ParagraphStatistics(Schema):
    paragraph_id = string(nullable=False)
    document_id = string(nullable=False)
    section_id = string(nullable=False)
    ordinal = integer(nullable=False)
    word_count = long(nullable=False)
    sentence_count = long(nullable=False)
    average_word_length = double(nullable=True)


class SectionStatistics(Schema):
    section_id = string(nullable=False)
    document_id = string(nullable=False)
    section_ordinal = integer(nullable=False)
    heading = string(nullable=False)
    paragraph_count = long(nullable=False)
    sentence_count = long(nullable=False)
    word_count = long(nullable=False)
    average_word_length = double(nullable=True)


class DocumentStatistics(Schema):
    document_id = string(nullable=False)
    section_count = long(nullable=False)
    paragraph_count = long(nullable=False)
    sentence_count = long(nullable=False)
    word_count = long(nullable=False)
    distinct_words = long(nullable=False)
    average_word_length = double(nullable=True)


class CorpusStatistics(Schema):
    corpus = string(nullable=False)
    document_count = long(nullable=False)
    average_sections_per_document = double(nullable=False)
    average_paragraphs_per_document = double(nullable=False)
    average_sentences_per_document = double(nullable=False)
    average_words_per_document = double(nullable=False)
    average_distinct_words_per_document = double(nullable=True)
    median_document_average_word_length = double(nullable=True)
    document_average_word_length_skewness = double(nullable=True)
    document_average_word_length_kurtosis = double(nullable=True)


class CorpusVocabulary(Schema):
    corpus = string(nullable=False)
    estimated_distinct_words = long(nullable=False)


class SimilarDocument(Schema):
    left_document_id = string(nullable=False)
    right_document_id = string(nullable=False)
    source = string(nullable=False)
    language = string(nullable=False)
    title_prefix = string(nullable=False)
    title_distance = integer(nullable=False)
    content_distance = integer(nullable=False)
