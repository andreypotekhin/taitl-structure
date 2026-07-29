"""Search's canonical token-normalization expression."""

from structure.plugin.pyspark import lower, regexp_replace, trim


def normalized_token(token):
    """Normalize one token according to the Search lexical contract."""
    return lower(regexp_replace(trim(token), pattern=r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", replacement=""))
