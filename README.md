# ADL PDF Ingest (PhD Evidence Pipeline)

A compact but realistic **evidence-ingestion pipeline** for PDF research papers, implemented in Python with **PostgreSQL** for metadata and **Elasticsearch** for full-text search.

This project ingests PDF documents, extracts and cleans text, enriches metadata from Paperpile, and indexes everything into Elasticsearch to support structured and free-text search. It uses an **enhancement-based architecture** where robots process documents asynchronously, creating typed enhancement records.

Locally, it runs across my ~620-document PhD corpus, making it a practical sandbox for exploring research-document ingestion, metadata workflows, and search patterns similar to evidence-platform pipelines.

## Features

- **PDF text extraction** using PyMuPDF with text cleaning (whitespace normalization, page number removal)
- **Enhancement-based architecture** - Documents + typed Enhancements (FULL_TEXT, PAPERPILE_METADATA)
- **Robot pattern** - Async workers (pdf-extractor, paperpile-sync) with state machine tracking
- **Rich metadata** from Paperpile CSV (title, abstract, authors, keywords, DOI, venue, year, tags)
- **Boosted multi-field search** - title^4, abstract^3, keywords^3, authors^2, full_text
- **Zero-downtime ES migrations** - Alias-based versioned indices (papers_v1, papers_v2, ...)
- **Context search** (grep-style) showing text snippets around matches

## Architecture

```
PDF Source (all_papers_raw/)
    |
    v
[Stage] --> processing/ (copy PDFs for ingestion)
    |
    v
[Register] --> PostgreSQL documents table
    |          + queue PendingEnhancement(FULL_TEXT)
    v
[pdf-extractor robot] --> Enhancement(FULL_TEXT, content={text: "..."})
    |
    v
[queue-metadata] --> queue PendingEnhancement(PAPERPILE_METADATA)
    |
    v
[paperpile-sync robot] --> Enhancement(PAPERPILE_METADATA, content={title, abstract, authors, ...})
    |
    v
[sync-es] --> Elasticsearch (papers_v1 via "papers" alias)
```

### Data Model

```
Document (id, file_path, created_at)
    |
    +-- Enhancement (FULL_TEXT)
    |       content: {text: "extracted pdf text..."}
    |
    +-- Enhancement (PAPERPILE_METADATA)
            content: {title, abstract, authors, keywords, doi, arxiv_id, venue, year, tags, item_type}
```

## Prerequisites

- Python 3.10+
- PostgreSQL (via Docker)
- Elasticsearch 8.x (via Docker)

## Setup

1. **Install the package:**
   ```bash
   pip install -e .
   ```

2. **Start PostgreSQL:**
   ```bash
   docker run -d --name pdf-postgres \
     -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=pdf_ingest \
     -p 5432:5432 \
     postgres:15
   ```

3. **Start Elasticsearch:**
   ```bash
   docker run -d --name pdf-es \
     -e "discovery.type=single-node" \
     -e "xpack.security.enabled=false" \
     -p 9200:9200 \
     elasticsearch:8.11.0
   ```

4. **Initialize database:**
   ```bash
   pdf-ingest init-db
   pdf-ingest init-es
   ```

## Usage

### Full Pipeline

```bash
# 1. Stage PDFs from source to processing directory
pdf-ingest stage                    # Copy all PDFs
pdf-ingest stage --limit 50         # Copy first 50

# 2. Register documents and queue for extraction
pdf-ingest register

# 3. Extract text from PDFs
pdf-ingest run-robot pdf-extractor

# 4. Queue and sync Paperpile metadata
pdf-ingest queue-metadata
pdf-ingest run-robot paperpile-sync --manifest metadata/papers_manifest.csv

# 5. Sync to Elasticsearch
pdf-ingest sync-es
```

### Search

```bash
# Basic search (boosted multi-field: title^4, abstract^3, keywords^3, authors^2, full_text)
pdf-ingest search -q "deduplication" --size 10

# Filter by year
pdf-ingest search -q "content-defined chunking" --year-from 2015

# Filter by Paperpile tag
pdf-ingest search -q "" --tag "Chunking"

# Count matching documents
pdf-ingest search -q "encryption" --tag "Secure Dedup" --count

# Combined filters
pdf-ingest search -q "encrypted" --tag "Secure Dedup" --year-from 2018
```

### Context Search (grep-style)

Show text snippets around matches:

```bash
pdf-ingest grep -q "FSL" --size 5
pdf-ingest grep -q "Rabin fingerprint" --size 3 --fragments 5 --fragment-size 200
pdf-ingest grep -q "neural" --sort year-desc
```

### Elasticsearch Management

```bash
# Check index status
pdf-ingest es-status

# Migrate to new index version (zero-downtime)
pdf-ingest es-migrate

# Rollback to previous version
pdf-ingest es-rollback

# Clean up old versions (keep latest 2)
pdf-ingest es-cleanup --keep 2

# Full rebuild
pdf-ingest sync-es --rebuild
```

## Project Structure

```
adl-pdf-ingest-phd/
├── pdf_ingest/
│   ├── cli.py              # Command-line interface
│   ├── config.py           # Settings (env vars, paths)
│   ├── models.py           # Document, Enhancement, PendingEnhancement
│   ├── db.py               # PostgreSQL operations
│   ├── extractor.py        # PyMuPDF text extraction
│   ├── cleaning.py         # Text cleaning utilities
│   ├── es_client.py        # ES client + IndexManager (migrations)
│   ├── queries.py          # Search functions (SEARCH_FIELDS)
│   └── robots/
│       ├── pdf_extractor.py    # Text extraction robot
│       └── paperpile_sync.py   # Metadata sync robot
├── metadata/
│   ├── papers_manifest.csv             # Full Paperpile export (rich metadata)
│   └── papers_manifest_normalized.csv  # Normalized format (basic fields)
├── tests/
│   ├── test_cleaning.py
│   ├── test_enhancements.py
│   ├── test_extractor_basic.py
│   └── test_queries.py
├── notes/
│   └── demo_queries.md     # Example queries with results
└── tools/
    └── select_dev_corpus.py
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `init-db` | Create PostgreSQL schema |
| `init-es` | Create Elasticsearch index |
| `stage` | Copy PDFs from source to processing directory |
| `register` | Register PDFs and queue for extraction |
| `run-robot pdf-extractor` | Extract text from queued documents |
| `queue-metadata` | Queue documents for metadata sync |
| `run-robot paperpile-sync` | Sync metadata from Paperpile CSV |
| `sync-es` | Sync documents to Elasticsearch |
| `search` | Search with filters (year, tag) |
| `grep` | Search with context snippets |
| `es-status` | Show ES index status |
| `es-migrate` | Migrate to new index version |
| `es-rollback` | Rollback to previous version |
| `es-cleanup` | Delete old index versions |

## Environment Variables

```bash
PG_DSN=postgresql://postgres:postgres@localhost:5432/pdf_ingest
ES_URL=http://localhost:9200
ES_INDEX=papers
PDF_SOURCE=/path/to/your/pdfs      # Default: all_papers_raw/
PDF_PROCESSING=/path/to/processing  # Default: processing/
```

## Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_enhancements.py
```

## Metadata

The `metadata/` directory contains:

- **`papers_manifest.csv`** - Full Paperpile CSV export with rich metadata (abstract, authors, keywords, DOI, etc.)
- **`papers_manifest_normalized.csv`** - Normalized format with basic fields only (file_name, title, venue, year, tags)

The paperpile-sync robot auto-detects the format and extracts all available fields.

## Search Fields

Queries use boosted multi-field search:

```python
SEARCH_FIELDS = [
    "title^4",      # Highest boost
    "abstract^3",   # High boost
    "keywords^3",   # High boost
    "authors^2",    # Medium boost
    "full_text",    # No boost
]
```

## License

MIT