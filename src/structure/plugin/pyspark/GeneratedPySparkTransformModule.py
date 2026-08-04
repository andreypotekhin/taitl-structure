from structure.plugin.api.v1.logic import SourceModulePath


def generated_pyspark_transform_module(source_module: str, *, generated_package: str) -> str:
    """Return the collision-free generated module for a source module."""

    source = SourceModulePath.from_module(source_module)
    package = SourceModulePath.from_module(generated_package)
    return f"{package.module}.pyspark.transforms.{source.module}"


def legacy_generated_pyspark_transform_module(source_module: str, *, generated_package: str) -> str:
    """Return the pre-collision-safe basename mapping for compatibility imports."""

    source = SourceModulePath.from_module(source_module)
    package = SourceModulePath.from_module(generated_package)
    return f"{package.module}.pyspark.transforms.{source.parts[-1]}"
