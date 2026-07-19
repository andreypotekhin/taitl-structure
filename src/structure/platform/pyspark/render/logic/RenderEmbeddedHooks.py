from __future__ import annotations

import ast
import builtins
import inspect
import textwrap
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Callable, NoReturn

from structure.lib.cross.errors import Diagnostic, diagnostic_registry, render_diagnostic
from structure.platform.api.v1 import TransformMemberOrigin
from structure.platform.pyspark.compiler.model.PySparkHookRecipe import PySparkHookRecipe


class EmbeddedHookError(ValueError):

    def __init__(self, diagnostic: Diagnostic) -> None:
        self.diagnostic = diagnostic
        super().__init__(render_diagnostic(diagnostic, kind="GeneratedCodeError"))


@dataclass(frozen=True)
class EmbeddedHook:
    origin: TransformMemberOrigin
    lines: tuple[str, ...]

    @property
    def name(self) -> str:
        return self.origin.member_name


class RenderEmbeddedHooks:
    """Extract standalone raw-hook methods for a generated PySpark class."""

    _builtins = frozenset(dir(builtins))

    def __call__(self, hooks: Iterable[PySparkHookRecipe]) -> tuple[EmbeddedHook, ...]:
        rendered: list[EmbeddedHook] = []
        seen: set[tuple[str, str]] = set()
        for hook in hooks:
            origin = hook.origin
            if origin is None or origin.owner is None:
                self._fail(hook.name, "its declaring source method is unavailable")
            key = (origin.import_name, origin.member_name)
            if key in seen:
                continue
            seen.add(key)
            rendered.append(EmbeddedHook(origin=origin, lines=self._render(hook.name, origin)))
        return tuple(rendered)

    def _render(self, hook_name: str, origin: TransformMemberOrigin) -> tuple[str, ...]:
        try:
            function = origin.owner.__dict__[origin.member_name]
        except KeyError:
            self._fail(hook_name, "its declaring source method is unavailable")
        if not inspect.isfunction(function):
            self._fail(hook_name, "only ordinary instance methods can be embedded")
        try:
            module = ast.parse(textwrap.dedent(inspect.getsource(function)))
        except (OSError, TypeError, SyntaxError) as error:
            self._fail(hook_name, f"its source cannot be parsed ({type(error).__name__})")
        if len(module.body) != 1:
            self._fail(hook_name, "only one ordinary synchronous function definition can be embedded")
        function_node = module.body[0]
        if not isinstance(function_node, ast.FunctionDef):
            self._fail(hook_name, "only one ordinary synchronous function definition can be embedded")
        function_node.decorator_list = []
        self._without_docstring(function_node)
        self._validate_dependencies(hook_name, function_node)
        return tuple(f"    {line}" if line else "" for line in ast.unparse(function_node).splitlines())

    def _without_docstring(self, function: ast.FunctionDef) -> None:
        if (
            function.body
            and isinstance(function.body[0], ast.Expr)
            and isinstance(function.body[0].value, ast.Constant)
            and isinstance(function.body[0].value.value, str)
        ):
            function.body.pop(0)
        if not function.body:
            function.body.append(ast.Pass())

    def _validate_dependencies(self, hook_name: str, function: ast.FunctionDef) -> None:
        visitor = _HookDependencies()
        for default in (*function.args.defaults, *function.args.kw_defaults):
            if default is not None:
                visitor.visit(default)
        for argument in (
            *function.args.posonlyargs,
            *function.args.args,
            *function.args.kwonlyargs,
        ):
            if argument.annotation is not None:
                visitor.visit(argument.annotation)
        if function.args.vararg is not None and function.args.vararg.annotation is not None:
            visitor.visit(function.args.vararg.annotation)
        if function.args.kwarg is not None and function.args.kwarg.annotation is not None:
            visitor.visit(function.args.kwarg.annotation)
        if function.returns is not None:
            visitor.visit(function.returns)
        for statement in function.body:
            visitor.visit(statement)
        if visitor.super_call:
            self._fail(hook_name, "super() is not supported in an embedded hook")
        if visitor.invalid_self_attribute is not None:
            self._fail(hook_name, f"self.{visitor.invalid_self_attribute} is not supported")

        parameters = {
            argument.arg
            for argument in (
                *function.args.posonlyargs,
                *function.args.args,
                *function.args.kwonlyargs,
            )
        }
        if function.args.vararg is not None:
            parameters.add(function.args.vararg.arg)
        if function.args.kwarg is not None:
            parameters.add(function.args.kwarg.arg)
        local_names = parameters | visitor.bound_names - visitor.global_names - visitor.nonlocal_names
        unresolved = sorted(visitor.loaded_names - local_names - self._builtins)
        if unresolved:
            self._fail(hook_name, f"{unresolved[0]!r} is not a local import, parameter, or local value")

    def _fail(self, hook_name: str, detail: str) -> NoReturn:
        raise EmbeddedHookError(
            Diagnostic(
                entry=diagnostic_registry["GEN-E0903"],
                problem=(
                    f"Cannot embed hook {hook_name!r}: {detail}. "
                    "embed_hooks requires a standalone hook body with local imports."
                ),
                use=(
                    "Move the dependency into the hook as a local import or value, remove embed_hooks, "
                    "or wait for explicit dependency packaging support."
                ),
                context={"hook": hook_name},
            )
        )


class _HookDependencies(ast.NodeVisitor):

    def __init__(self) -> None:
        self.bound_names: set[str] = set()
        self.loaded_names: set[str] = set()
        self.global_names: set[str] = set()
        self.nonlocal_names: set[str] = set()
        self.invalid_self_attribute: str | None = None
        self.super_call = False

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.loaded_names.add(node.id)
        else:
            self.bound_names.add(node.id)
        self.generic_visit(node)

    def visit_arg(self, node: ast.arg) -> None:
        self.bound_names.add(node.arg)

    def visit_alias(self, node: ast.alias) -> None:
        self.bound_names.add(node.asname or node.name.split(".", 1)[0])

    def visit_Global(self, node: ast.Global) -> None:
        self.global_names.update(node.names)

    def visit_Nonlocal(self, node: ast.Nonlocal) -> None:
        self.nonlocal_names.update(node.names)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if isinstance(node.value, ast.Name) and node.value.id == "self" and node.attr not in {"spark", "ctx"}:
            self.invalid_self_attribute = node.attr
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Name) and node.func.id == "super":
            self.super_call = True
        self.generic_visit(node)
