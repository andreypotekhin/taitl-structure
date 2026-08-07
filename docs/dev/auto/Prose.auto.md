# Prose automation

## Reference  
Authoring: [Authoring.auto.md](Authoring.auto.md)
Defines text operators and processes, lists existing text operators and processes.
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
    - Avoid merging code listings, maintain a text sentence in between.
  - Workflow transform is main transform in a package - a staged transform that rules other transforms in the package.   
  Usually, alphabetically the last file in dir (Ex: 'SearchDocuments'). Not all dirs contain the workflow transform. 
    - Top header: as-is, do not convert to gerund
    - Replace subsections as described above
    - Split class listing from intro section into a new section, 'Workflow' 
    - Thus, the 'Workflow' section now includes all former sections, except to intro section. 
      - Since it is now starts with bare code listing, include a sentence before that describing that
      this is the resulting workflow transform. 
    - Insert the content (.cnd.md) of subtransforms after 'Workflow' section to form continuous narrative.
      - Convert the headers in inserted content to one step lower, to maintain header structure.
      - Do not remove any headers - former top-level headers now become section headers. 
    - Some stages may be outside of workflow dir. For those: 
      - Include section heading 
      - Instead of content, include brief description, ", as described in 'doc_header'". 
      - Include code listing of its staged transform (from workflow section, without retaining 'Workflow' heading or text).
        If the stage call appears inside the workflow class listing, extract only that assignment and its continuation;
        do not copy the enclosing workflow class listing.
    - Quality assurance:
      - Verify transform sections follow the order of main transform stages, with no stages missing.
      - Verify the same code listing does not repeat twice in one document, including workflow class listings copied as
        external-stage listings.
    - Thus, the texts between code listings in 'Workflow' section may become redundant, since the stages describe them in detail in the included content.
      - Trim texts between code listings in 'Workflow' to avoid repetition.
