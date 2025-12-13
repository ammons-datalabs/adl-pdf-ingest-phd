from __future__ import annotations

from pathlib import Path

import fitz  # PyMuPDF


class ExtractionError(Exception):
    pass


def extract_text(pdf_path: Path) -> str:
    """
    Extract full text from a PDF.
    For now: simple concatenation of per-page text.
    """
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        raise ExtractionError(f"Failed to open PDF {pdf_path}: {e}") from e

    try:
        parts: list[str] = []
        for page in doc:
            parts.append(page.get_text("text"))
        return "\n".join(parts)
    except Exception as e:
        raise ExtractionError(f"Failed to extract text from {pdf_path}: {e}") from e
    finally:
        doc.close()
