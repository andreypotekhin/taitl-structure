from __future__ import annotations

from importlib.resources import files

RESOURCE_PACKAGE = "structure.app.target.pyspark.resources"
RUNTIME_MODULE_RESOURCE = "runtime.py.template"


class RenderPySparkRuntimeModule:

    def __call__(self) -> str:
        return files(RESOURCE_PACKAGE).joinpath(RUNTIME_MODULE_RESOURCE).read_text(encoding="utf-8")


render_pyspark_runtime_module = RenderPySparkRuntimeModule()
