from structure.platform.pyspark.capabilities.api.Capabilities import Capabilities
from structure.platform.pyspark.compiler.api.Compiler import Compiler
from structure.platform.pyspark.execution.api.Execution import Execution
from structure.platform.pyspark.files.api.Files import Files
from structure.platform.pyspark.render.api.Render import Render
from structure.platform.pyspark.schema.api.Schema import Schema
from structure.platform.pyspark.symbolic_execution.api.SymbolicExecution import SymbolicExecution


class PySpark:

    files = Files()
    compiler = Compiler()
    capabilities = Capabilities()
    execution = Execution()
    render = Render()
    schema = Schema()
    symbolic_execution = SymbolicExecution()
