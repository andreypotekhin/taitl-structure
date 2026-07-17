from examples.texts.schemas.analytics import DocumentFeatures
from examples.texts.schemas.text import Document
from structure import *


class ProfileDocuments(Transform):
    """Extract document features."""

    documents = input(Document)
    features = output(DocumentFeatures)

    def profile(self, row: Document) -> DocumentFeatures:
        normalized_title = nullif(trim(row.title), "Untitled")
        normalized_content = lower(regexp_replace(trim(row.content), pattern=r"\s+", replacement=" "))
        title_words = split(row.title, pattern=r"\s+")
        content_length = length(row.content)
        return DocumentFeatures(
            document_id=row.id,
            collection_id=row.collection_id,
            source=row.source,
            language=row.language,
            title=row.title,
            url=row.url,
            normalized_title=normalized_title,
            normalized_content=normalized_content,
            title_starts_with_the=row.title.startswith("The "),
            title_ends_with_guide=row.title.endswith("Guide"),
            url_is_https=row.url.startswith("https://"),
            content_contains_structure=row.content.contains("Structure"),
            content_like_guide=row.content.like("%guide%"),
            content_ilike_structure=row.content.ilike("%STRUCTURE%"),
            content_matches_sentence=row.content.rlike(r".*[.!?].*"),
            title_prefix=substring(row.title, start=1, length=12),
            title_words=title_words,
            leading_title_words=slice(title_words, 1, 2),
            sorted_title_words=arr_sort(title_words),
            title_slug=regexp_replace(row.title, pattern=r"\s+", replacement="-"),
            first_number=regexp_extract(row.title, pattern=r"([0-9]+)", group=1),
            content_length=content_length,
            title_case=initcap(ltrim(rtrim(row.title))),
            reversed_title=reverse(row.title),
            translated_title=translate(row.title, matching="_", replacement="-"),
            structure_position=instr(row.content, substring="Structure"),
            title_distance_to_guide=levenshtein(row.title, "Structure Guide"),
            display_name=concat_ws(" · ", row.source, row.title, row.url),
            title_hash=hash(row.title, row.source),
            content_sha2=sha2(row.content, bits=256),
            harvest_year=year(row.harvested_at),
            age_days=datediff(row.harvested_at, row.created_at),
            content_length_sqrt=sqrt(content_length),
            content_length_log=log(content_length + 1),
            rounded_content_length=bround(content_length, scale=0),
            content_length_sign=signum(content_length),
            source_recency_rank=row_number(
                partition_by=row.source,
                order_by=(row.harvested_at.desc_nulls_last(), row.id.asc_nulls_first()),
            ),
        )
