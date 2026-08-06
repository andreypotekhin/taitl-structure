# Prose automation

## Reference  
Main: [Authoring.auto.md](Authoring.auto.md)
Lists existing text operators and processes defined elsewhere.
More operators and processes are defined below. 

## Collection

### Collect process
- Input: close/annotated
- Output: close/collected
- Scope: close/annotated/search
- Name: Collect. Usage: Collect(dir)
- Invocation: manual

### Collect operator
- Name: collect(), usage: collect(dir)
- Input: .py.md describing a transform. Ex: rerank.py.md
- Output: .cnd.md describing a transform in collectd form. Ex: rerank.cond.md
- Goal: present annotated source as continuous narrative.
- Instructions
  - Top header: convert to gerund, optionally extend to fuller phrase.
    - Ex: 'Rerank Documents' becomes 'Reranking the Documents'
  - Low-level headers: drop low-level headers. If that affects clarity, repeat the header as part of intro section paragraph.
    - Ex: 'Select fallback options' dropped, 'select fallback options' embedded in section paragraph. 
  - Workflow transform (main transform in dir, usually alphabetically the last one. Ex: 'SearchDocuments')
    - Replace subsections as described above; leave top header intact.
    - Split off class listing from intro section into new section, 'Workflow' 
    - Insert the content (.cnd.md) of subtransforms after intro section to form continuous narrative. 
    - The 'Workflow' section becomes the last. 
      - Since it is now starts with bare code listing, include a sentence before that describing that
      this is the resulting workflow transform. 
      - Since stages are already defined in the preceding content, text sections may be trimmed to avoid
      repeatition, and even merged into one 'Stages' section with minimal text between code sections.
