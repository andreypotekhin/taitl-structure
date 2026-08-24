from pathlib import Path
import re

BS = chr(92)
PATTERN = re.compile(
    r"\\begin\{aligned\}\r?\n&(.*?)\r?\n&\\rightarrow\s*(.*?)\r?\n\\end\{aligned\}",
    re.S,
)


def unsplit(match: re.Match[str]) -> str:
    call = match.group(1).rstrip()
    if call.endswith(BS + BS):
        call = call[:-2].rstrip()
    return call + " " + BS + "rightarrow " + match.group(2).strip()


for path in Path("close/form/search").rglob("*.form.md"):
    text = path.read_text(encoding="utf-8")
    path.write_text(PATTERN.sub(unsplit, text), encoding="utf-8")
