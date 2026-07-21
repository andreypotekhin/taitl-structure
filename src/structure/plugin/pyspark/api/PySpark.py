from structure.plugin.pyspark.capabilities.api.Capabilities import Capabilities
from structure.plugin.pyspark.compiler.api.Compiler import Compiler
from structure.plugin.pyspark.execution.api.Execution import Execution
from structure.plugin.pyspark.files.api.Files import Files
from structure.plugin.pyspark.render.api.Render import Render
from structure.plugin.pyspark.schema.api.Schema import Schema
from structure.plugin.pyspark.symbolic_execution.api.SymbolicExecution import SymbolicExecution


class PySpark:

    files = Files()
    compiler = Compiler()
    capabilities = Capabilities()
    execution = Execution()
    render = Render()
    schema = Schema()
    symbolic_execution = SymbolicExecution()
