import shutil
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from uuid import uuid4

import pytest

from structure import (
    CompilerOptions,
    MemoryStorage,
    PackageImportStorage,
    StructureConfig,
    StructureRuntimeError,
    StructureSession,
)


@contextmanager
def workspace_tmp():
    root = Path(".pytest-workspace-tmp") / uuid4().hex
    root.mkdir(parents=True)
    try:
        yield root.resolve()
    finally:
        shutil.rmtree(root, ignore_errors=True)


@dataclass(frozen=True)
class FakeType:
    name: str
    args: tuple = ()
    options: tuple = ()


class FakeTypes:

    @staticmethod
    def StructType(fields):
        return FakeType("StructType", tuple(fields))

    @staticmethod
    def StructField(name, dataType, nullable):
        return FakeType("StructField", (name, dataType, nullable))

    @staticmethod
    def StringType():
        return FakeType("StringType")

    @staticmethod
    def IntegerType():
        return FakeType("IntegerType")

    @staticmethod
    def LongType():
        return FakeType("LongType")

    @staticmethod
    def FloatType():
        return FakeType("FloatType")

    @staticmethod
    def DoubleType():
        return FakeType("DoubleType")

    @staticmethod
    def BooleanType():
        return FakeType("BooleanType")

    @staticmethod
    def DateType():
        return FakeType("DateType")

    @staticmethod
    def TimestampType():
        return FakeType("TimestampType")

    @staticmethod
    def DecimalType(precision, scale):
        return FakeType("DecimalType", (precision, scale))

    @staticmethod
    def ArrayType(element, *, containsNull):
        return FakeType("ArrayType", (element,), (("containsNull", containsNull),))

    @staticmethod
    def MapType(key, value, *, valueContainsNull):
        return FakeType("MapType", (key, value), (("valueContainsNull", valueContainsNull),))


def test_v1_session_reads_project_config() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            '[tool.structure]\nexecution_mode = "generated"\ngenerated_package = "project_generated"\n',
            encoding="utf-8",
        )

        session = StructureSession(project_root=root, schema_types=FakeTypes)

        assert session.config.execution_mode == "generated"
        assert session.execution_mode == "generated"
        assert session.generated_package == "project_generated"
        assert session.config.source_map["generated_package"] == "structure.toml"


def test_v1_session_convenience_overrides_win_over_project_config() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            '[tool.structure]\nexecution_mode = "generated"\ngenerated_package = "project_generated"\n',
            encoding="utf-8",
        )

        session = StructureSession(
            project_root=root,
            execution_mode="online",
            generated_package="programmatic_generated",
            schema_types=FakeTypes,
        )

        assert session.config.execution_mode == "online"
        assert session.generated_package == "programmatic_generated"
        assert session.config.source_map["execution_mode"] == "programmatic"
        assert session.config.source_map["generated_package"] == "programmatic"


def test_v1_session_uses_supplied_config() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        config = StructureConfig.resolve(project_root=root, execution_mode="generated")

        session = StructureSession(config=config, schema_types=FakeTypes)

        assert session.config is config
        assert session.execution_mode == "generated"


def test_v1_session_compiler_options_match_project_config() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        (root / "structure.toml").write_text(
            '[tool.structure]\ntarget_profile = ">=3.5,<4.1"\ngenerated_package = "project_generated"\n',
            encoding="utf-8",
        )

        options = CompilerOptions.resolve(project_root=root, schema_types=FakeTypes)
        session = StructureSession(project_root=root, schema_types=FakeTypes)

        assert session.compiler_options == options


def test_v1_session_rejects_config_mixed_with_project_or_overrides() -> None:
    with workspace_tmp() as root:
        (root / "src").mkdir()
        config = StructureConfig.resolve(project_root=root)

        with pytest.raises(ValueError) as raised:
            StructureSession(config=config, project_root=root)

        assert "config=StructureConfig.resolve" in str(raised.value)

        with pytest.raises(ValueError) as raised:
            StructureSession(config=config, execution_mode="generated")

        assert "config override fields" in str(raised.value)


def test_v1_online_session_defers_to_runner_and_exposes_schemas_without_pyspark() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    before = {name for name in sys.modules if name.startswith("pyspark")}
    captured = {}

    def executor(**kwargs):
        captured.update(kwargs)
        return "online-result"

    invocation = EnrichOrders(
        orders="orders-df",
        customers="customers-df",
        products="products-df",
        promotions="promotions-df",
    )
    session = StructureSession(spark="spark", ctx="ctx", schema_types=FakeTypes, online_executor=executor)

    result = invocation.run(session)

    after = {name for name in sys.modules if name.startswith("pyspark")}
    assert after == before
    assert result.published == "online-result"
    assert result["published"] == "online-result"
    assert captured["spark"] == "spark"
    assert captured["ctx"] == "ctx"
    assert captured["inputs"]["orders"] == "orders-df"
    assert captured["plan"].transform == "EnrichOrders"
    assert list(result.schema) == ["published"]
    assert result.schema.published.name == "StructType"
    assert result.schema["published"].name == "StructType"


def test_v1_online_session_reuses_class_compiled_artifact(monkeypatch) -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    from structure.app.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform

    EnrichOrders._structure_compiled.clear()
    calls = 0
    original = BuildCompiledTransform.__call__

    def counted(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(BuildCompiledTransform, "__call__", counted)
    session = StructureSession(schema_types=FakeTypes, online_executor=lambda **kwargs: "online-result")
    inputs = {
        "orders": "orders-df",
        "customers": "customers-df",
        "products": "products-df",
        "promotions": "promotions-df",
    }

    EnrichOrders(**inputs).run(session)
    EnrichOrders(**inputs).run(session)

    assert calls == 1


def test_v1_transform_compile_force_rebuilds_class_artifact(monkeypatch) -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    from structure.app.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform

    EnrichOrders._structure_compiled.clear()
    calls = 0
    original = BuildCompiledTransform.__call__

    def counted(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(BuildCompiledTransform, "__call__", counted)

    EnrichOrders.compile(schema_types=FakeTypes)
    EnrichOrders.compile(schema_types=FakeTypes)
    EnrichOrders.compile(schema_types=FakeTypes, force=True)

    assert calls == 2


def test_v1_compile_key_includes_version_and_source_hash() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    from structure.app.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform

    key = BuildCompiledTransform().key(
        EnrichOrders,
        options=CompilerOptions.resolve(schema_types=FakeTypes),
    )

    assert key.structure_version
    assert key.sources[0][3] is not None
    assert len(key.sources[0][3]) == 64


def test_v1_compiled_artifact_does_not_capture_bound_inputs() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    EnrichOrders._structure_compiled.clear()
    artifact = EnrichOrders.compile(schema_types=FakeTypes)
    invocation = EnrichOrders(
        orders="orders-df-sentinel",
        customers="customers-df-sentinel",
        products="products-df-sentinel",
        promotions="promotions-df-sentinel",
    )

    assert "orders-df-sentinel" in repr(invocation._structure_bound_inputs)
    assert "orders-df-sentinel" not in repr(artifact)


def test_v1_pipeline_reuses_shared_compiled_artifact(monkeypatch) -> None:
    from structure import String, Structure, Transform, field, input, output, transform
    from structure.app.compiler.artifacts.commands.BuildCompiledTransform import BuildCompiledTransform
    from structure.app.dsl.model.transforms.TransformPipeline import TransformPipeline

    class Raw(Structure):
        id = field(String(), nullable=False)

    class Normalized(Structure):
        id = field(String(), nullable=False)

    class Published(Structure):
        id = field(String(), nullable=False)

    @transform
    class NormalizeOrders(Transform):
        orders = input(Raw)
        normalized = output(Normalized)

        def normalize(self, order: Raw) -> Normalized:
            return Normalized(id=order.id)

    @transform
    class PublishOrders(Transform):
        normalized = input(Normalized)
        published = output(Published)

        def publish(self, order: Normalized) -> Published:
            return Published(id=order.id)

    TransformPipeline._structure_compiled.clear()
    calls = 0
    original = BuildCompiledTransform.__call__

    def counted(self, *args, **kwargs):
        nonlocal calls
        calls += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(BuildCompiledTransform, "__call__", counted)
    options = CompilerOptions.resolve(schema_types=FakeTypes)

    NormalizeOrders(orders=object()).to(PublishOrders()).compile(options, schema_types=FakeTypes)
    NormalizeOrders(orders=object()).to(PublishOrders()).compile(options, schema_types=FakeTypes)

    assert calls == 1


def test_v1_online_session_reports_missing_declared_inputs() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    session = StructureSession(schema_types=FakeTypes, online_executor=lambda **kwargs: None)

    with pytest.raises(StructureRuntimeError) as raised:
        session.run(EnrichOrders(orders="orders-df"))

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "ONLINE-E1201"
    assert diagnostic.execution_mode == "online"
    assert diagnostic.context["inputs"] == "customers, products, promotions"
    assert "Pass every declared input DataFrame" in diagnostic.use
    assert "docs/Diagnostics.md#online-e1201" in str(raised.value)


def test_v1_generated_session_delegates_to_generated_class() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    module_name = "testing.model.v1.structure_generated.orders.pyspark.transforms.order"
    installed = _install_generated_module(module_name)
    try:
        invocation = EnrichOrders(
            orders="orders-df",
            customers="customers-df",
            products="products-df",
            promotions="promotions-df",
        )
        session = StructureSession(
            spark="spark",
            ctx="ctx",
            execution_mode="generated",
            generated_package="testing.model.v1.structure_generated.orders",
            schema_types=FakeTypes,
        )

        result = session.run(invocation)

        assert result.published == {
            "spark": "spark",
            "ctx": "ctx",
            "orders": "orders-df",
            "customers": "customers-df",
            "products": "products-df",
            "promotions": "promotions-df",
        }
        assert result.schema.published.name == "StructType"
    finally:
        for name in installed:
            sys.modules.pop(name, None)


def test_v1_generated_session_can_import_from_memory_storage() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    storage = MemoryStorage()
    storage.write(
        {
            "memory_generated/__init__.py": "",
            "memory_generated/pyspark/__init__.py": "",
            "memory_generated/pyspark/transforms/__init__.py": "",
            "memory_generated/pyspark/transforms/order.py": """
class EnrichOrdersGenerated:

    def __init__(self, *, spark, ctx=None):
        self.spark = spark
        self.ctx = ctx

    def run(self, *, orders, customers, products, promotions):
        return {
            "spark": self.spark,
            "ctx": self.ctx,
            "orders": orders,
            "customers": customers,
            "products": products,
            "promotions": promotions,
        }
""",
        }
    )
    invocation = EnrichOrders(
        orders="orders-df",
        customers="customers-df",
        products="products-df",
        promotions="promotions-df",
    )
    session = StructureSession(
        spark="spark",
        ctx="ctx",
        execution_mode="generated",
        generated_package="memory_generated",
        schema_types=FakeTypes,
        storage=storage,
    )

    result = session.run(invocation)

    assert result.published == {
        "spark": "spark",
        "ctx": "ctx",
        "orders": "orders-df",
        "customers": "customers-df",
        "products": "products-df",
        "promotions": "promotions-df",
    }


def test_v1_memory_storage_does_not_import_unowned_modules() -> None:
    module = ModuleType("memory_generated.pyspark.transforms.order")
    sys.modules[module.__name__] = module
    try:
        with pytest.raises(ImportError):
            MemoryStorage().import_module(module.__name__)
    finally:
        sys.modules.pop(module.__name__, None)


def test_v1_package_import_storage_rejects_modules_outside_package() -> None:
    storage = PackageImportStorage("structure_generated")

    with pytest.raises(ImportError):
        storage.import_module("other_generated.pyspark.transforms.order")


def test_v1_generated_session_reports_missing_generated_code() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    invocation = EnrichOrders(
        orders="orders-df",
        customers="customers-df",
        products="products-df",
        promotions="promotions-df",
    )
    session = StructureSession(
        execution_mode="generated",
        generated_package="missing_structure_generated",
        schema_types=FakeTypes,
    )

    with pytest.raises(StructureRuntimeError) as raised:
        session.run(invocation)

    diagnostic = raised.value.diagnostic
    assert diagnostic.code == "GEN-E0902"
    assert diagnostic.execution_mode == "generated"
    assert "structure compile" in diagnostic.use
    assert "missing_structure_generated.pyspark.transforms.order" in diagnostic.problem


def test_v1_generated_spark_connect_classic_only_failure_reports_boundary() -> None:
    from testing.model.v1.orders.transforms.order import EnrichOrders

    module_name = "testing.model.v1.structure_generated.orders.pyspark.transforms.order"
    installed = _install_generated_module(
        module_name,
        failure=RuntimeError("Generated hook touched _jvm through Py4J"),
    )
    try:
        invocation = EnrichOrders(
            orders="orders-df",
            customers="customers-df",
            products="products-df",
            promotions="promotions-df",
        )
        session = StructureSession(
            spark="spark",
            execution_mode="generated",
            generated_package="testing.model.v1.structure_generated.orders",
            schema_types=FakeTypes,
            target_variant="spark-connect",
        )

        with pytest.raises(StructureRuntimeError) as raised:
            session.run(invocation)

        diagnostic = raised.value.diagnostic
        assert diagnostic.code == "CONNECT-E2601"
        assert diagnostic.execution_mode == "generated"
        assert diagnostic.context["surface"] == "generated transform or hook code"
        assert "RDD APIs, or Py4J gateway objects" in diagnostic.problem
    finally:
        for name in installed:
            sys.modules.pop(name, None)


def _install_generated_module(name: str, *, failure: Exception | None = None) -> list[str]:
    installed: list[str] = []
    parts = name.split(".")
    for index in range(1, len(parts)):
        package_name = ".".join(parts[:index])
        if package_name not in sys.modules:
            package = ModuleType(package_name)
            package.__path__ = []  # type: ignore[attr-defined]
            sys.modules[package_name] = package
            installed.append(package_name)
        if index > 1:
            parent = sys.modules[".".join(parts[: index - 1])]
            setattr(parent, parts[index - 1], sys.modules[package_name])

    module = ModuleType(name)

    class EnrichOrdersGenerated:

        def __init__(self, *, spark, ctx=None) -> None:
            self.spark = spark
            self.ctx = ctx

        def run(self, *, orders, customers, products, promotions):
            if failure is not None:
                raise failure
            return {
                "spark": self.spark,
                "ctx": self.ctx,
                "orders": orders,
                "customers": customers,
                "products": products,
                "promotions": promotions,
            }

    setattr(module, "EnrichOrdersGenerated", EnrichOrdersGenerated)
    sys.modules[name] = module
    setattr(sys.modules[".".join(parts[:-1])], parts[-1], module)
    installed.append(name)
    return list(reversed(installed))
