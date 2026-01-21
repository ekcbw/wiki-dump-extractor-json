<span class="badge-placeholder">[![GitHub release](https://img.shields.io/github/v/release/ekcbw/wiki-dump-extractor-json)](https://github.com/ekcbw/wiki-dump-extractor-json/releases/latest)</span>
<span class="badge-placeholder">[![License: MIT](https://img.shields.io/github/license/ekcbw/wiki-dump-extractor-json)](https://github.com/ekcbw/wiki-dump-extractor-json/blob/main/LICENSE)</span>

# Wiki Dump Extractor

An efficient extractor for Wikipedia XML dump files that extracts structured content into JSON format. Designed for memory-efficient processing of large Wikipedia dumps with structured output preservation and utilized [mwparserfromhell](https://github.com/earwig/mwparserfromhell) for source parsing. 

## Features

- **Memory Efficient**: Uses streaming XML parsing to handle 10GB+ dump files without memory growth
- **Parallel Processing**: Multi-core support for faster processing of large datasets
- **Resumable Processing**: Saves progress and can resume from interrupted extractions
- **Structured Output**: Preserves Wikipedia's hierarchical structure (sections, tables of contents, infoboxes)
- **Reference Handling**: Deduplicates and renumbers citation references
- **Redirection Support**: Properly handles Wikipedia redirect pages
- **Benchmarking Tools**: Built-in performance testing utilities

## Installation

```bash
pip install wiki-dump-extractor-json
```

### Dependencies

- Python 3.10 or higher
- tqdm (for progress bars)
- mwparserfromhell (for parsing source)
- python-dateutil (for date parsing)

## Usage

### Command Line Interface

The package provides a command-line tool `wiki-dump-extractor-json`:

```bash
# Parse a Wikipedia XML dump file
wiki-dump-extractor-json -o output_directory pages-meta-current.xml

# Parse with specific number of worker processes
wiki-dump-extractor-json -o output_directory --workers 8 pages-meta-current.xml

# Run a benchmark test
wiki-dump-extractor-json --benchmark pages-meta-current.xml

# Look up a specific article from extracted data
wiki-dump-extractor-json --lookup "title" output_directory
```

### Python API

#### Parse Wikipedia XML Dumps

```python
from wiki_dump_extractor_json import parse_xml_dump

# Stream parse XML dump file
with open('enwiki-latest-pages-meta-current.xml', 'rb') as f:
    for page in parse_xml_dump(f):
        print(f"Page: {page.title}")
        print(f"Timestamp: {page.timestamp}")
        # Process page.source as needed
```

#### Parse Individual Wikipedia Articles

```python
from wiki_dump_extractor_json import parse_source

# Parse a single Wikipedia article source
source = """== Section Title ==
Some content with a reference<ref>Reference text</ref>.
Another paragraph."""

parsed = parse_source(source)
print(f"Leading text: {parsed['leading']}")
print(f"References: {parsed['references']}")
print(f"Subsections: {len(parsed['subSections'])}")
```

#### Lookup Extracted Articles

```python
from wiki_dump_extractor_json import lookup_from_extracted

# Look up a specific article from extracted data
article_data = lookup_from_extracted('output_directory', '1462年')
print(f"Article title: {article_data['title']}")
print(f"Article sections: {len(article_data['subSections'])}")
```

## Output Format

The parser extracts Wikipedia articles into the following JSON structure:

```json
{
  "title": "wiki-dump-extractor-json",
  "timestamp": 1532158619.0,
  "leading": "wiki-dump-extractor-json is an efficient parser for Wikimedia XML dumps.\n\n",
  "infobox": null,
  "toc": [
    {
      "title": "Sub section",
      "sub": []
    }
  ],
  "subSections": [
    {
      "title": "Sub section",
      "leading": "== Sub section ==\n\nSub section part.",
      "subSections": []
    }
  ],
  "references": [],
  "redirectedTo": null
}
```

### Fields

- `title`: The article title
- `timestamp`: Last edit timestamp as Unix timestamp
- `leading`: The lead section content (before first section)
- `infobox`: The infobox template content if present
- `toc`: Table of contents hierarchy
- `subSections`: Array of article sections with hierarchical structure
- `references`: List of unique reference texts
- `redirectedTo`: Target title if this is a redirect page

## Output Path Structure

Extracted data is organized as:
```
output_directory/
├── index.json          # Progress tracking and article index
├── 00/
│   ├── 0.jsonl         # JSON Lines files with articles
│   ├── 1.jsonl
│   └── ...
├── 01/
│   ├── 0.jsonl
│   └── ...
└── ...
```
Each `.jsonl` file contains one JSON object per line, each representing one Wikipedia article.  
The format of `index.json` (for details, see [wiki_dump/extractor.py](wiki_dump/extractor.py)):
```json
{
    "pages": {"Template:Wikipedialang": [0, 0, 0],
              "wiki-dump-extractor-json": [0, 0, 1]},
    "_progress": [0, 0, 536, 2]
}
```
