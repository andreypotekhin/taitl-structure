"""Structure's target-neutral public API.

Import PySpark expressions, joins, field factories, and concrete types from
``structure.plugin.pyspark``.  The root package deliberately contains only
Structure lifecycle and artifact concepts.
"""

from structure.core.compiler.artifacts.model.CompiledArtifactPool import CompiledArtifactPool
from structure.core.compiler.artifacts.model.ArtifactCacheReport import ArtifactCacheReport
from structure.core.compiler.artifacts.model.CompileKey import CompileKey
from structure.core.compiler.artifacts.model.CompiledTransform import CompiledTransform
from structure.core.compiler.artifacts.model.CompilerOptions import CompilerOptions
from structure.core.compiler.artifacts.model.GeneratedTransform import GeneratedTransform
from structure.core.compiler.artifacts.storage import DiskStorage, MemoryStorage, PackageImportStorage
from structure.core.compiler.diagnostics.api import StructureCompileError
from structure.core.configuration.api import StructureConfig
from structure.core.dsl.model.schemas.Schema import Schema
from structure.core.dsl.model.transforms.SchemaMode import SchemaMode
from structure.core.dsl.model.transforms.StreamingMode import StreamingMode
from structure.core.dsl.model.transforms.Transform import Transform
from structure.core.dsl.model.transforms.transform_api import input, lane, output, raw, special, step, transform
from structure.core.runtime.api import (
    ResultSchemas,
    StructureRuntimeError,
    StructureSession,
    TransformResult,
    TransformSchemas,
)
from structure.core.sources import CompiledSources, SourceTransformAddress, StructureSources
from structure.core.tools.api import StructureTools

__all__ = [
    "ArtifactCacheReport",
    "CompileKey",
    "CompiledArtifactPool",
    "CompiledSources",
    "CompiledTransform",
    "CompilerOptions",
    "DiskStorage",
    "GeneratedTransform",
    "MemoryStorage",
    "PackageImportStorage",
    "ResultSchemas",
    "Schema",
    "SchemaMode",
    "SourceTransformAddress",
    "StreamingMode",
    "StructureCompileError",
    "StructureConfig",
    "StructureRuntimeError",
    "StructureSession",
    "StructureSources",
    "StructureTools",
    "Transform",
    "TransformResult",
    "TransformSchemas",
    "input",
    "lane",
    "output",
    "raw",
    "special",
    "step",
    "transform",
]
