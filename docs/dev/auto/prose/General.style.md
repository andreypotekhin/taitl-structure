# General narrative style

Write for a technically confident reader who may be new to the domain. Introduce concepts before use; Definitions
collects every essential reusable term, not a predetermined number of entries.
Capitalize the first word of every definition sentence; preserve an exact identifier's spelling by introducing it
with an ordinary capitalized word when needed. In prose references, name the destination as a chapter, for example
"See the Filtering chapter for overlap matching." Bare topic names remain appropriate in Builds on/Used by inventories.
Verify that the destination actually covers the referenced component; package membership alone does not establish
chapter coverage. Do not invent a chapter reference for an otherwise undocumented helper.
Assume no prior knowledge of the topic's integrations or abstractions. Introduce the central activity and fully name
roles on first use (for example, "inference adapter" and what it translates), before using shortened references.
Builds on/Used by name principal chapters such as Online and Offline, not their implementation-stage classes.

Name the actor accurately: use "search user," "caller," or "application" for retrieval requests. Use "reader" only
when discussing someone reading a document or this chapter, not as a blanket synonym for a search user.

Use concrete subjects and active verbs. Format exact class, schema, method, and relation names as inline code in prose,
not in headings or inside math. Keep domain terms in ordinary text. Avoid operator-production language such as
"collected source," "boundary-only subsection," or "this document records"; a direct chapter reference is clearer.

Ground behavioral claims in the actual operations rather than class names or docstrings. Distinguish compatibility
checks, numeric normalization, and row-selection precedence instead of treating them as interchangeable guarantees.
State which returned measure a threshold changes, and distinguish population expansion from selection. Do not infer
deduplication, denominator protection, or fallback choice from the intended use of a relation.
For sequential overlays, trace a shared key through every writer, including zero and null cases, before describing
precedence. Distinguish validation failures from row filtering.

Use "search engine" at most once per document, and omit it when unnecessary. Do not position the system being described
as its own downstream consumer.

Wrap prose near 120 columns, with continuation lines at column zero except actual nested lists. Preserve source-code
whitespace. Use blank lines around headings, paragraphs, lists, and fences; never leave adjacent code fences without
intervening descriptive prose.
