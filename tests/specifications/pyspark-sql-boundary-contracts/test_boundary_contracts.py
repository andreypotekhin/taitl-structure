import pytest

from structure.plugin.pyspark.compiler.logic.maps.MapPySparkExpression import MapPySparkExpression
from structure.plugin.pyspark.dsl.expressions import (
    aes_decrypt,
    aes_encrypt,
    current_date,
    current_timezone,
    hll_sketch_estimate,
    hll_union,
    now,
)
from structure.plugin.pyspark.dsl.operations_api import bitmap_construct_agg, hll_sketch_agg
from structure.plugin.pyspark.dsl.types import BitmapType, HllSketchType, LongType, StringType
from structure.plugin.pyspark.render.logic.expressions.RenderPySparkExpression import RenderPySparkExpression


class _Capabilities:
    def require(self, requirement):
        return None


def _render(expression):
    recipe = MapPySparkExpression().map(expression, capabilities=_Capabilities())
    return RenderPySparkExpression()(recipe)


def test_query_clock_contract_is_typed_and_query_stable():
    assert isinstance(current_date().type, type(current_date().type))
    assert current_date().nullable is False
    assert current_date().data["query_stable"] is True
    assert current_date().data["nondeterministic"] is True
    assert now().type.name == "timestamp"
    assert current_timezone().type == StringType()
    assert _render(now()) == "F.now()"


def test_aes_contract_rejects_literal_keys_and_warns_for_explicit_iv():
    with pytest.raises(TypeError, match="symbolic String or Binary"):
        aes_encrypt("payload", key="literal-secret")
    expression = aes_encrypt("payload", key=current_timezone(), iv=b"123456789012")
    assert expression.type.name == "binary"
    assert expression.data["warnings"] == ("CRYPTO-W0801",)
    assert _render(expression) == "F.aes_encrypt(F.lit('payload'), F.current_timezone(), F.lit(b'123456789012'))"
    assert _render(aes_decrypt("payload", key=current_timezone())) == "F.aes_decrypt(F.lit('payload'), F.current_timezone())"


def test_opaque_sketch_contract_preserves_brands_and_precision_guard():
    first = hll_sketch_agg(1, lg_config_k=12)
    second = hll_sketch_agg(1, lg_config_k=14)
    assert isinstance(first.type, HllSketchType)
    assert isinstance(bitmap_construct_agg(1).type, BitmapType)
    assert isinstance(hll_sketch_estimate(first).type, LongType)
    with pytest.raises(TypeError, match="matching lg_config_k"):
        hll_union(first, second)
    assert hll_union(first, second, allow_different_lg_config_k=True).data["warnings"] == ("SKETCH-W0802",)
