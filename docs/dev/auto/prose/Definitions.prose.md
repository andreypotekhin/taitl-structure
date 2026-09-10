# Chapter model

These terms define the model used by [Prose.md](../Prose.md)'s chapter operators. Build this inventory before writing;
it is working data, not chapter prose. Resolve it from source declarations, annotated source, and explicit topic scope.

## Inventory

~~~text
Chapter(topic, main?, roots[], owned_classes[], schemas[])
Transform(name, source, kind, inputs[], outputs[], groups[], stages[])
Stage(alias, transform, bindings[], output_relations[], ownership)
Group(intent, explanation, listing, public_methods[], private_methods[])
Schema(name, fields[], return_constructions[])
~~~

- **Main transform**: an actual source class designated as the chapter's subject; never inferred from filename order.
- **Root**: each independent transform in the established topic scope, including the main when present. A main may
  coexist with separately invoked topic transforms; Vectorization and its query binders are an example. A called child
  is not also a root merely because it has its own source file. Preserve declared topic order, then source order where
  unspecified; never invent calls connecting independent roots.
- **Step transform**: a transform implemented by methods, including implicit steps and public typed helpers.
- **Composed transform**: a transform implemented by child-stage assignments. A **workflow transform** is a composed main.
- **Stage**: one call occurrence, including its alias and complete argument bindings; not just the called class.
- **Internal stage**: a call whose class belongs to the chapter's implementation scope. Default scope is the selected
  package subtree; include explicitly established topic-owned helpers outside it. Record such exceptions with evidence.
- **External stage**: a call to a class outside that scope. Imports alone do not establish ownership.
- **Method group**: one low-level source section's intent, explanation, and method listing. A listing may contain several
  related methods. A class declaration or stage assignment is not a method group.
- **Public method**: a source-declared public step (decorated or implicit), or public typed helper such as a
  public `@special` or `@raw` method. Private helpers remain in Code, not Implementation.
- **Explanatory item**: a method-group paragraph or external-call description. Implementation and Code have separate
  item streams, even when some of their intents describe the same operation.

If a transform genuinely mixes methods and stages, record both in source order and apply both branches below; do not
drop one to force a binary classification. A transform with stages concludes with Result.

Resolve inherited inputs, methods, and calls as part of each concrete transform's effective contract. A subclass whose
base is documented in another chapter, or already fully documented in this chapter, is a **specialization**: represent
its effective contract as that exact base plus every local replacement/addition. Identify the base by chapter and
module when names collide. This is an explicit reusable definition, not an omitted-method shortcut for ordinary steps.
Expand a topic-owned base once before its specializations; do not collect external base implementations.
Inheritance is not a child-stage call. Each independently invoked specialization is still a numbering root; a called
specialization belongs to its caller's root.

Specialization body = plain base/behavior introduction + local public groups or actual replacement-stage subsections
+ effective contract. The effective contract lists every inherited input/output and every replacement with complete
signatures or call bindings, referencing the named base for unchanged members. A specialized step ends with one
Resulting transform shape; a specialized composition ends with its own Result. Number local method groups and actual
external replacement calls by the ordinary rules, never the inheritance declaration or a parameter-only class intro.
Code contains exact local declarations and groups, with a plain base reference when needed to resolve import aliases.

An external boundary still exposes the complete effective input/output contract, including inherited inputs. Stopping
recursion suppresses methods and child implementations, not the query, index, or target relations passed to the call.

## Rendering algebra

~~~text
Draft.Implementation(chapter) = continuous narrative                 # no subsections or Result

Extend.Implementation(chapter) =
    preamble
    for root in chapter.roots:
        reset implementation_counter
        if chapter.roots has one entry and root == chapter.main and root.kind == composed:
            children(root) + Result(root)                           # siblings under Implementation
        else:
            heading(root) + body(root)                              # independent root owns its subtree

body(internal step) =
    plain_intro + public_groups + "Resulting transform shape:" + step_shape

body(internal composed) =
    plain_intro + members_in_source_order + Result                  # nested inside this transform
    # members = child subsections, plus public groups if source genuinely mixes both

body(external call) =
    circled_plain_description + boundary_notation                    # stop recursion at this boundary

Format.Implementation = preserve(Extend.Implementation) + formula_notation

Collect.containers = transform headings                            # Workflow names a sole composed main
Collect.transform_body = plain_intro + class_listing + groups
Collect.group = italic_intent_and_explanation + method_listing      # never a heading
Extend.Code = rebase_headings(Collect) + independent_decimal_items
Format.Code = exact(Extend.Code)
~~~

`children` emits one subsection per call in execution order, recursively using internal/external rules. Repeated calls
retain distinct aliases and arguments. Class/method code is collected once per root; repeated calls do not justify
duplicating a class listing. Every internal composed transform has its own concluding Result, including composed
internal stages. An external composed class still uses boundary-only treatment; its Result belongs in its own chapter.

Code headings represent transform containers, never method groups. Below a transform's plain class description and
listing, each method/helper group is one numbered, intent-led explanatory paragraph immediately before its listing.
Nested headings are permitted only for actual child transforms or external calls, not for methods or grain passes.
The stage introduction cannot stand in for public groups: an internal step with N collected public groups has N
Implementation items, not one synthetic "Run transform" item followed only by its shape.

The top-level **Stages section** is a concise inventory, not the Implementation subsection tree. With multiple roots,
list those independent transforms, whether a main is designated or not. Otherwise list a step main's public steps or
a composed main's direct calls. Multiple roots each own their container and counter; none is a child of its neighbor.

## Numbering

| Stream | Consumes a number | Does not consume a number | Scope |
|---|---|---|---|
| Implementation | Public method group; external-call item | Internal intro/class/stage, Result, shape | Circled ① onward, reset per root |
| Code | Intent-led method/helper group; external-call item | Internal class/Workflow description; assignments within parent listing | Decimal 1. onward, independently reset per root |

External stages are single-step items because their method groups are not expanded here. In Implementation, their
source-backed description has a circled number but no italicized intent; the heading already identifies the operation.
Code independently retains the collected short italic intent and explanation before the call listing.
Internal class descriptions remain plain and unnumbered, including composed classes without methods.

Code numbering follows collected groups and external calls, never Implementation items. Private helper groups with
intents still belong to Code; their absence from Implementation is one reason the streams differ. Where an intent is
required, keep it and its explanation in the same numbered paragraph immediately before its listing or notation; never add a
separate unnumbered explanation after it.

Use actual circled numerals (①–⑳, ㉑–㉟, ㊱–㊿); do not generate them by incrementing one Unicode code point past ⑳.
If a root exceeds the available circled glyphs, use an explicit circled-number rendering, not parenthesized numerals.

## Notation

- **Text notation**: lossless signatures and named input/method/stage/output blocks in Draft and Extend.
- **Formula notation**: the chapter profile of [Notation.md](Notation.md#chapter-profile) used by Format.
- **Step notation**: one complete signature per public method, including every parallel grain path and concrete return.
- **Resulting shape block**: the step-transform summary, once at the end of that step's subsection.
- **Result**: a composed transform's concluding subsection, containing its explanation and whole-transform notation.
  It is not a synonym for step shape or chapter-wide summary.
