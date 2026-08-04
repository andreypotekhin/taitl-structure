def generated_pyspark_transform_module(source_module: str, *, generated_package: str) -> str:
    """Return the collision-free generated module for a source module."""

    return f"{generated_package}.pyspark.transforms.{source_module}"


def legacy_generated_pyspark_transform_module(source_module: str, *, generated_package: str) -> str:
    """Return the pre-collision-safe basename mapping for compatibility imports."""

    return f"{generated_package}.pyspark.transforms.{source_module.rsplit('.', 1)[-1]}"
