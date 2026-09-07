"""Read Indexing contracts without importing the application or a Spark runtime."""

import ast
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[5]
SOURCES = {
    "Indexing": "examples/search/transforms/indexing/Indexing.py",
    "LexIndex": "examples/search/transforms/indexing/lexical/LexIndex.py",
    "FieldIndex": "examples/search/transforms/indexing/fields/FieldIndex.py",
}
SCHEMAS = [
    "examples/search/schemas/text.py",
    "examples/search/schemas/chunking/intermediate.py",
    "examples/search/schemas/indexing/lexical/index.py",
    "examples/search/schemas/indexing/lexical/intermediate.py",
    "examples/search/schemas/fields/fields.py",
]


def inventory():
    classes = {}
    for name, filename in SOURCES.items():
        source = (ROOT / filename).read_text(encoding="utf-8")
        lines = source.splitlines()
        node = next(n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == name)
        methods = [n for n in node.body if isinstance(n, ast.FunctionDef)]
        header_end = min(d.lineno for d in methods[0].decorator_list) - 1 if methods else node.end_lineno
        item = {"name": name, "source": filename, "inputs": [], "outputs": [], "stages": [], "methods": [],
                "header": "\n".join(lines[node.lineno - 1:header_end]).rstrip()}
        for member in node.body:
            if isinstance(member, ast.Assign) and isinstance(member.value, ast.Call):
                call = member.value
                function = ast.unparse(call.func)
                target = ast.unparse(member.targets[0])
                if function in ("input", "output"):
                    item[function + "s"].append({"name": target, "schema": ast.unparse(call.args[0])})
                elif function in SOURCES:
                    item["stages"].append({"alias": target, "transform": function,
                                           "bindings": {k.arg: ast.unparse(k.value) for k in call.keywords}})
        for method in methods:
            args = [{"name": a.arg, "schema": ast.unparse(a.annotation)} for a in method.args.args if a.arg != "self"]
            returned = next(n.value for n in ast.walk(method) if isinstance(n, ast.Return))
            projection = any(isinstance(n, ast.Attribute) and n.attr in ("project", "base") for n in ast.walk(returned))
            explicit = [k.arg for k in returned.keywords] if isinstance(returned, ast.Call) else []
            start = min([method.lineno] + [d.lineno for d in method.decorator_list])
            item["methods"].append({"name": method.name, "args": args, "return": ast.unparse(method.returns),
                                    "projection": projection, "explicit": explicit,
                                    "code": "\n".join(lines[start - 1:method.end_lineno]),
                                    "ast": ast.dump(method, include_attributes=False)})
        classes[name] = item
    schema_nodes = {}
    for filename in SCHEMAS:
        for node in ast.parse((ROOT / filename).read_text(encoding="utf-8")).body:
            if isinstance(node, ast.ClassDef):
                schema_nodes[node.name] = node

    def fields(name):
        if name == "Schema":
            return []
        node = schema_nodes[name]
        result = [field for base in node.bases for field in fields(ast.unparse(base))]
        for member in node.body:
            if isinstance(member, ast.Assign):
                for target in member.targets:
                    if isinstance(target, ast.Name) and target.id not in result:
                        result.append(target.id)
        return result

    returns = {m["return"] for c in classes.values() for m in c["methods"]}
    return {"classes": classes, "schemas": {name: fields(name) for name in sorted(returns)}}


if __name__ == "__main__":
    print(json.dumps(inventory(), ensure_ascii=False))
