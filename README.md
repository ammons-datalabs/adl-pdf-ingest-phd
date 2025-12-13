# ADL PDF Ingest (PhD Evidence Pipeline)

A compact but realistic **evidence-ingestion pipeline** for PDF research papers, implemented in Python with **PostgreSQL** for metadata and **Elasticsearch** for full-text search.

This project ingests PDF documents, extracts and cleans text, enriches metadata from Paperpile, and indexes everything into Elasticsearch to support structured and free-text search. It includes unit + integration tests, a small synthetic test corpus, and a modular codebase ready to be extended with queue/worker orchestration or an API layer.

Locally, it runs across my ~700-document PhD corpus, making it a practical sandbox for exploring research-document ingestion, metadata workflows, and search patterns similar to evidence-platform pipelines.

## Features

- **PDF text extraction** using PyMuPDF with text cleaning (whitespace normalization, page number removal)
- **PostgreSQL** for document registry and status tracking
- **Elasticsearch** for full-text search with relevance scoring
- **Metadata enrichment** from Paperpile CSV exports (title, venue, year, tags)
- **Tag-aware search** using your Paperpile labels
- **Context search** (grep-style) showing text snippets around matches

## Architecture

```
PDF Source (all_papers_raw/)
    |
    v
[Stage] --> processing/ (copy PDFs for ingestion)
    |
    v
[Discover] --> PostgreSQL (document registry, status: NEW/INDEXED/FAILED)
    |
    v
[Extract] --> PyMuPDF text extraction
    |
    v
[Clean] --> Normalize whitespace, remove page numbers
    |
    v
[Index] --> Elasticsearch (full_text + metadata for search)
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

### Stage and Ingest PDFs

```bash
# Stage PDFs from source to processing directory
pdf-ingest stage                    # Copy all PDFs
pdf-ingest stage --limit 50         # Copy first 50 PDFs
pdf-ingest stage --pattern "*2024*" # Copy PDFs matching pattern

# Run ingestion pipeline on staged PDFs
pdf-ingest run
```

### Enrich with Paperpile Metadata

```bash
pdf-ingest enrich-metadata --manifest metadata/papers_manifest_normalized.csv
pdf-ingest run  # Re-index with metadata
```

### Search

```bash
# Basic search
pdf-ingest search --query "deduplication" --size 10

# Filter by year
pdf-ingest search --query "content-defined chunking" --year-from 2015 --year-to 2025

# Filter by Paperpile tag
pdf-ingest search --query "" --tag "Fingerprint-Indexing"

# Combined filters
pdf-ingest search --query "encrypted" --tag "Secure Dedup" --year-from 2018
```

### Context Search (grep-style)

Show text snippets around matches:

```bash
pdf-ingest grep --query "FSL" --size 5
pdf-ingest grep --query "Rabin fingerprint" --size 3 --fragments 5
```

## Project Structure

```
adl-pdf-ingest-phd/
├── pdf_ingest/
│   ├── cli.py              # Command-line interface
│   ├── config.py           # Settings (env vars, paths)
│   ├── models.py           # Document dataclass, DocumentStatus enum
│   ├── db.py               # PostgreSQL operations
│   ├── discover.py         # PDF discovery and registration
│   ├── extractor.py        # PyMuPDF text extraction
│   ├── cleaning.py         # Text cleaning utilities
│   ├── es_client.py        # Elasticsearch client
│   ├── metadata.py         # CSV manifest enrichment
│   ├── queries.py          # Search functions
│   └── pipeline.py         # Main orchestration
├── all_papers_raw/         # Source PDFs (gitignored)
├── processing/             # Staged PDFs for ingestion (gitignored)
├── metadata/
│   ├── papers_manifest.csv             # Raw Paperpile export (example)
│   └── papers_manifest_normalized.csv  # Normalized manifest for ingestion
├── tests/
│   ├── test_extractor_basic.py
│   ├── test_cleaning.py
│   ├── test_db_roundtrip.py
│   └── test_integration.py
└── tools/
    ├── select_dev_corpus.py    # Select diverse dev PDFs
    └── convert_manifest.py     # Convert Paperpile CSV
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `stage` | Copy PDFs from source to processing directory |
| `init-db` | Create PostgreSQL schema |
| `init-es` | Create Elasticsearch index |
| `run` | Run ingestion pipeline |
| `enrich-metadata` | Apply CSV metadata to documents |
| `search` | Search with filters (year, tag) |
| `grep` | Search with context snippets |

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
# Run all tests (requires PostgreSQL + Elasticsearch)
pytest

# Run only unit tests (no services required)
pytest -m "not integration"

# Run only integration tests
pytest -m integration
```

## Metadata

The `metadata/` directory contains:

- **`papers_manifest.csv`** - Example raw Paperpile CSV export. To use your own, export from Paperpile and run `tools/convert_manifest.py`.
- **`papers_manifest_normalized.csv`** - Normalized format used by `enrich-metadata` command. Contains: `file_name`, `title`, `venue`, `year`, `tags`.

To convert your own Paperpile export:

```bash
python tools/convert_manifest.py
```

## License

MIT
