import importlib

import pytest
from integration.pyspark.support.backend_matrix import generated_project, render_generated_project, session
from integration.pyspark.support.rows import rows

import structure.plugin.pyspark as sf
from structure import Schema, Transform, input, output, transform
from structure.lib.testing import assert_online_generated_parity
from structure.plugin.pyspark import binary, double, integer, long, string

pytestmark = pytest.mark.integration

SOURCE_MODULE = "integration.pyspark.v7.test_numeric_function_parity"
PACKAGE = "integration_v7_numeric_function_generated"


class NumericInput(Schema):
    value = double(nullable=False)
    positive = double(nullable=False)
    second = double(nullable=False)
    integral = integer(nullable=False)
    digits = string(nullable=False)
    hexadecimal = string(nullable=False)


class NumericOutput(Schema):
    abs_value = double(nullable=False)
    acos_value = double(nullable=False)
    acosh_value = double(nullable=False)
    asin_value = double(nullable=False)
    asinh_value = double(nullable=False)
    atan_value = double(nullable=False)
    atan2_value = double(nullable=False)
    atanh_value = double(nullable=False)
    bin_value = string(nullable=False)
    bround_value = double(nullable=False)
    cbrt_value = double(nullable=False)
    ceil_value = long(nullable=False)
    conv_value = string(nullable=False)
    cos_value = double(nullable=False)
    cosh_value = double(nullable=False)
    cot_value = double(nullable=False)
    csc_value = double(nullable=False)
    degrees_value = double(nullable=False)
    e_value = double(nullable=False)
    exp_value = double(nullable=False)
    expm1_value = double(nullable=False)
    factorial_value = long(nullable=False)
    floor_value = long(nullable=False)
    greatest_value = double(nullable=False)
    hex_value = string(nullable=False)
    hypot_value = double(nullable=False)
    least_value = double(nullable=False)
    ln_value = double(nullable=False)
    log_value = double(nullable=False)
    log10_value = double(nullable=False)
    log1p_value = double(nullable=False)
    log2_value = double(nullable=False)
    pi_value = double(nullable=False)
    pmod_value = double(nullable=False)
    pow_value = double(nullable=False)
    radians_value = double(nullable=False)
    rint_value = double(nullable=False)
    round_value = double(nullable=False)
    sec_value = double(nullable=False)
    sign_value = double(nullable=False)
    signum_value = double(nullable=False)
    sin_value = double(nullable=False)
    sinh_value = double(nullable=False)
    sqrt_value = double(nullable=False)
    tan_value = double(nullable=False)
    tanh_value = double(nullable=False)
    unhex_value = binary(nullable=False)
    width_bucket_value = long(nullable=False)


@transform
class NumericFunctions(Transform):
    rows = input(NumericInput)
    result = output(NumericOutput)

    def publish(self, row: NumericInput) -> NumericOutput:
        return NumericOutput(
            abs_value=sf.abs(row.value),
            acos_value=sf.acos(row.value),
            acosh_value=sf.acosh(row.positive),
            asin_value=sf.asin(row.value),
            asinh_value=sf.asinh(row.value),
            atan_value=sf.atan(row.value),
            atan2_value=sf.atan2(row.value, row.positive),
            atanh_value=sf.atanh(row.value),
            bin_value=sf.bin(row.integral),
            bround_value=sf.bround(row.value),
            cbrt_value=sf.cbrt(row.positive),
            ceil_value=sf.ceil(row.value),
            conv_value=sf.conv(row.digits, from_base=2, to_base=16),
            cos_value=sf.cos(row.value),
            cosh_value=sf.cosh(row.value),
            cot_value=sf.cot(row.value),
            csc_value=sf.csc(row.value),
            degrees_value=sf.degrees(row.value),
            e_value=sf.e(),
            exp_value=sf.exp(row.value),
            expm1_value=sf.expm1(row.value),
            factorial_value=sf.factorial(row.integral),
            floor_value=sf.floor(row.value),
            greatest_value=sf.greatest(row.value, row.positive),
            hex_value=sf.hex(row.integral),
            hypot_value=sf.hypot(row.value, row.positive),
            least_value=sf.least(row.value, row.positive),
            ln_value=sf.ln(row.positive),
            log_value=sf.log(row.positive, base=10),
            log10_value=sf.log10(row.positive),
            log1p_value=sf.log1p(row.value),
            log2_value=sf.log2(row.positive),
            pi_value=sf.pi(),
            pmod_value=sf.pmod(-row.positive, row.value),
            pow_value=sf.pow(row.value, row.positive),
            radians_value=sf.radians(row.value),
            rint_value=sf.rint(row.positive),
            round_value=sf.round(row.positive),
            sec_value=sf.sec(row.value),
            sign_value=sf.sign(row.value),
            signum_value=sf.signum(row.value),
            sin_value=sf.sin(row.value),
            sinh_value=sf.sinh(row.value),
            sqrt_value=sf.sqrt(row.positive),
            tan_value=sf.tan(row.value),
            tanh_value=sf.tanh(row.value),
            unhex_value=sf.unhex(row.hexadecimal),
            width_bucket_value=sf.width_bucket(row.positive, 0, 10, num_buckets=5),
        )


def test_numeric_functions_match_native_and_generated_execution(spark, tmp_path) -> None:
    files = render_generated_project(
        NumericFunctions,
        source_transform=f"{SOURCE_MODULE}.NumericFunctions",
        generated_package=PACKAGE,
        source_schema_modules={SOURCE_MODULE: [NumericInput, NumericOutput]},
    )
    with generated_project(tmp_path, PACKAGE, files):
        schemas = importlib.import_module(f"{PACKAGE}.pyspark.schemas.test_numeric_function_parity")
        source = spark.createDataFrame([(0.5, 2.0, 2.5, 5, "101", "AF")], schemas.NUMERIC_INPUT_SCHEMA)
        online = NumericFunctions(rows=source).run(session(spark, execution_mode="online"))
        generated = NumericFunctions(rows=source).run(
            session(spark, execution_mode="generated", generated_package=PACKAGE)
        )
        assert_online_generated_parity(lambda: online, lambda: generated)

        from pyspark.sql import functions as F

        native = source.select(
            F.abs("value").alias("abs_value"),
            F.acos("value").alias("acos_value"),
            F.acosh("positive").alias("acosh_value"),
            F.asin("value").alias("asin_value"),
            F.asinh("value").alias("asinh_value"),
            F.atan("value").alias("atan_value"),
            F.atan2("value", "positive").alias("atan2_value"),
            F.atanh("value").alias("atanh_value"),
            F.bin("integral").alias("bin_value"),
            F.bround("value").alias("bround_value"),
            F.cbrt("positive").alias("cbrt_value"),
            F.ceil("value").alias("ceil_value"),
            F.conv("digits", 2, 16).alias("conv_value"),
            F.cos("value").alias("cos_value"),
            F.cosh("value").alias("cosh_value"),
            F.cot("value").alias("cot_value"),
            F.csc("value").alias("csc_value"),
            F.degrees("value").alias("degrees_value"),
            F.e().alias("e_value"),
            F.exp("value").alias("exp_value"),
            F.expm1("value").alias("expm1_value"),
            F.factorial("integral").alias("factorial_value"),
            F.floor("value").alias("floor_value"),
            F.greatest("value", "positive").alias("greatest_value"),
            F.hex("integral").alias("hex_value"),
            F.hypot("value", "positive").alias("hypot_value"),
            F.least("value", "positive").alias("least_value"),
            F.ln("positive").alias("ln_value"),
            F.log(10.0, "positive").alias("log_value"),
            F.log10("positive").alias("log10_value"),
            F.log1p("value").alias("log1p_value"),
            F.log2("positive").alias("log2_value"),
            F.pi().alias("pi_value"),
            F.pmod(-F.col("positive"), F.col("value")).alias("pmod_value"),
            F.pow("value", "positive").alias("pow_value"),
            F.radians("value").alias("radians_value"),
            F.rint("positive").alias("rint_value"),
            F.round("positive").alias("round_value"),
            F.sec("value").alias("sec_value"),
            F.sign("value").alias("sign_value"),
            F.signum("value").alias("signum_value"),
            F.sin("value").alias("sin_value"),
            F.sinh("value").alias("sinh_value"),
            F.sqrt("positive").alias("sqrt_value"),
            F.tan("value").alias("tan_value"),
            F.tanh("value").alias("tanh_value"),
            F.unhex("hexadecimal").alias("unhex_value"),
            F.width_bucket("positive", F.lit(0), F.lit(10), 5).alias("width_bucket_value"),
        )
        assert rows(native) == rows(generated.result)
