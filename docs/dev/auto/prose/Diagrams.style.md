# Diagrams style

# Diagrams - General
Diagrams: general style
- Markup: Mermaid by default; named styles below may specify another format.
- Compatibility: GitHub/Typora 
- Colors: Monochrome only

## Artifact location

Store SVG diagrams and their PNG previews under `close/diagrams/`, preserving the chapter's relative topic path.
For example, `close/form/search/transforms/filtering/Filtering.form.md` uses diagrams in
`close/diagrams/search/transforms/filtering/`. Keep diagram assets out of `close/form/`.
Page placement is undecided: do not add diagram embeds, captions, source keys, or diagram links to Form chapters.
Keep diagrams standalone for possible facing-page placement. Use `<Topic>.<style>.svg` and `<Topic>.<style>.png`
to retain distinct explorations.

## Experimental implementation circuits

For the Filtering prototype, temporarily waive the Mermaid requirement and use an SVG with a PNG preview.
Keep GitHub/Typora as the intended reading environments. This experiment does not require converting other diagrams.

Use mixed electrical circuit notation to explain implementation data flow. Label components with actual method names
and connections with actual lane and schema names. Functional ICs represent normalization, counting, ranking, and
projection; comparators represent comparisons, and a controlled analog switch represents row admission. These are
software metaphors, not electrically executable designs. Define each symbol's meaning in a small legend.

Route wires orthogonally, mark connected branches with dots, and show signal direction. Solid connections carry data;
dashed connections carry predicate control. Use dashed enclosures for transform and method ownership. Keep labels clear
of wires. Additional components inside a method explain its expressions; they do not imply additional source methods.

Use analog symbols only when their meaning is explicit. Do not add decorative resistors, capacitors, power supplies,
or grounds. A supplied timestamp is data, not a clock. Preserve the actual input bindings and responsibility boundaries.

Keep implementation circuits standalone, with any Markdown source key beside the diagram in `close/diagrams/`.
Diagrams supplement existing prose, formulas, and listings. Check source accuracy and visually inspect the rendered SVG
before publication; provide a PNG preview when the reading surface cannot display SVG.

## Conceptual Action Blocks

Use this prompt to create a conceptual diagram for a prose chapter. Reference:
[Filtering](../../../../close/diagrams/search/transforms/filtering/Filtering.blocks.svg).

### Prompt

Create a Conceptual Action Blocks diagram for the supplied chapter. Explain how its inputs become useful outputs
through meaningful actions. Write for a conceptual reader, using plain language without code references, method names,
schema names, electrical symbols, or UML notation. Read the chapter and check its underlying behavior before drawing.

**Content**

- Center a short chapter title and a one-line purpose above the diagram.
- Begin with the principal input, then show numbered action blocks, ending with the resulting output and its use.
- Give each action a short verb-led heading and one or two concise lines explaining what it does. Preserve important
  actions such as matching, counting, ranking, selection, and timestamping when they belong to the chapter; do not
  impose Filtering's sequence on other topics.
- Bring additional inputs into the action that actually consumes them. Use small explanatory cards for examples,
  interpretation, or caller responsibilities. Keep examples close to the action they explain.
- Preserve meaningful limits and distinctions. Simplifying the language must not invent guarantees or change behavior.
  Place essential caveats below the output rather than crowding the action blocks.

**Layout and appearance**

- Use a centered vertical main flow with equal-width action blocks, consistent spacing, and straight downward arrows.
  Keep the title, principal input, actions, output, and footer on the same central axis.
- Balance the left and right sides with equal-width side cards, equal margins, and aligned rows where useful content
  permits. Pair an actual input with a relevant explanatory card when appropriate. Do not invent dependencies, add
  filler, or duplicate information merely to obtain symmetry. Keep unavoidable branches balanced around the main axis.
- Use solid outlines for inputs, actions, and outputs. Use dashed outlines for explanatory cards. Connect only actual
  data flow with arrows; explanatory cards have no data arrows. Route side-input arrows horizontally where possible.
- Use monochrome on white, restrained corner rounding, a readable sans-serif font, bold action headings, and smaller
  supporting text. Distinguish input/output labels from action headings. Allow ample whitespace and keep wires clear
  of text. Do not shrink long labels to fit; shorten or wrap them.
- Add a short legend when both connected inputs and unconnected explanatory cards appear: arrows carry data; dashed
  cards explain behavior. Keep layout commentary out of the chapter's explanatory content.

**Delivery and verification**

- Produce an SVG for precise layout and a PNG preview. This named style permits SVG instead of Mermaid.
- Save the artifacts under the chapter's topic path in `close/diagrams/` as `<Topic>.blocks.svg` and
  `<Topic>.blocks.png`. Keep the diagram and any caption or preview link outside the Form chapter while page placement
  remains undecided. Preserve existing prose, formulas, code listings, and separate implementation diagrams.
- Render and inspect the result for readable text, clear arrows, clipping, overlap, and left/right visual balance.
  Verify actions, inputs, ordering, limits, and output against the chapter and source. Check local links and show the
  preview inline when presenting the result.
