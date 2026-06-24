from structure.app.compiler.frontend.api.FrontendEndpoint import FrontendEndpoint
from structure.app.compiler.frontend.logic.actions.CompileTransform import CompileTransform

frontend = FrontendEndpoint()

__all__ = [
    "CompileTransform",
    "frontend",
]
