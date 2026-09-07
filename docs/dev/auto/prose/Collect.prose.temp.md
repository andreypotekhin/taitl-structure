# {{Topic title; gerund for a step main}}

<!-- Choose one container layout using Collect.prose.md:
sole main composed: ## Workflow, then child containers;
main step: description/listing/groups directly below H1;
multiple roots (even with a main), or no main: ## RootName per independent transform, with nested child containers.
An internal composed child has its own class listing followed by nested children.
The fragments below are repeatable slots, not mandatory literal headings. -->

## {{Workflow, RootName, or InternalStageName}}

{{Plain source-grounded class description.}}

~~~python
{{Exact class/interface listing, including composed assignments; module-level imports removed.}}
~~~

*{{Short method-group intent.}}* {{Original explanation once, in the same paragraph.}}

~~~python
{{Exact method-group listing.}}
~~~

<!-- Repeat intent + explanation + listing for all method/helper groups, in source order.
Method headings from annotation become intents, not subsections. Container headings identify transforms only. -->

### {{ExternalStageName}}

*{{Short external-call intent.}}* {{Source-grounded explanation.}}

~~~python
{{Exact parameterized assignment also present in the parent listing.}}
~~~
