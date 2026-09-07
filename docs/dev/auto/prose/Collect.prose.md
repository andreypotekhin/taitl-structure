## Collect

### Collect process

Assemble the topic's annotated source into a continuous source document under `close/collected/`, preserving its
relative path. This manually invoked process follows the chapter's ownership boundaries and does not expand external
implementations or change application code.

~~~text
Collect(topic, annotated_source, chapter_scope) -> close/collected/<topic-path>/<Topic>.cnd.md
~~~

### Collect operator

Organize source into transform containers and intent-led method groups. Read [Prose.md](../Prose.md), [Definitions.prose.md](Definitions.prose.md), and
[General.style.md](General.style.md), then instantiate [Collect.prose.temp.md](Collect.prose.temp.md).
Source annotation remains governed by [Annotation.prose.md](Annotation.prose.md).

1. Inventory roots, child calls, ownership, classes, and all annotated method sections against source. Include implicit
   steps and public/private helpers, including trailing publishing methods. Resolve missing annotation before collection;
   if repair is outside the request, report it rather than silently producing partial output.
2. Choose the container shape from the inventory:
   - Multiple roots, with or without a designated main: use the topic title and a named container for each independent
     root. Collect each recursively; do not invent a common Workflow or calls between neighboring roots.
   - Sole main composed transform: preserve its title, put its complete parent listing under Workflow, then collect children
     in execution order.
   - Main step transform: use a gerund topic title and its class/method narrative, without Workflow.
   - No main: use the topic title and named root containers, without a common Workflow.
3. Preserve internal class descriptions as plain prose before class/interface listings. Retain transform container
   headings. Replace each low-level method/helper heading with its short italicized intent sentence at the beginning of
   the explanation paragraph. Preserve the original explanation once, without a duplicate paraphrase or long italic span.
4. Keep each listing in source order, unchanged except removing module-level imports; retain method-local imports.
   Do not merge listings. Each method-group listing
   needs its own opening intent and explanation; when annotation has several listings under one heading, derive a short
   source-backed intent for each existing description. If a description is missing, repair annotation first. Do not
   manufacture an extra wrapper paragraph or reinterpret an internal stage description as a method group.
5. Recurse into internal classes, once per root. For each external call, retain the complete assignment in the parent
   and a self-contained external section with short italic intent, source-backed explanation, and that exact
   parameterized assignment. Only the assignment is intentionally repeated; never collect the external implementation
   or repeat the parent class. An external call is one item, not a fabricated method group.
6. Run [QA.prose.md](QA.prose.md)'s shared and Collect checks. Emit one H1, with all containers nested beneath it.
   Collect emits no item numbers; Extend assigns Code numbers to these collected method groups and external calls.

A Workflow or internal class container without descriptive prose needs one source-grounded summary before its listing.
Keep this inside the container, not between Code and its first heading in downstream chapters.
