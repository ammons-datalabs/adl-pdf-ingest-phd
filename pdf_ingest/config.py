from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root if present
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


@dataclass
class Settings:
    pg_dsn: str
    es_url: str
    es_index: str
    pdf_source: Path
    pdf_processing: Path


def get_settings() -> Settings:
    pg_dsn = os.getenv(
        "PG_DSN",
        "postgresql://postgres:postgres@localhost:5432/pdf_ingest",
    )
    es_url = os.getenv("ES_URL", "http://localhost:9200")
    es_index = os.getenv("ES_INDEX", "papers")

    # Source directory for raw PDFs (your collection)
    pdf_source = Path(os.getenv("PDF_SOURCE", str(PROJECT_ROOT / "all_papers_raw")))
    # Processing directory where PDFs are copied for ingestion
    pdf_processing = Path(os.getenv("PDF_PROCESSING", str(PROJECT_ROOT / "processing")))

    return Settings(
        pg_dsn=pg_dsn,
        es_url=es_url,
        es_index=es_index,
        pdf_source=pdf_source,
        pdf_processing=pdf_processing,
    )
