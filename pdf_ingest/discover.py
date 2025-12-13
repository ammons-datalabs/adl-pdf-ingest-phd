from __future__ import annotations

from pathlib import Path
from typing import Iterable

from .config import get_settings
from .db import register_files


def _iter_pdfs(root: Path) -> Iterable[Path]:
    for path in root.rglob("*.pdf"):
        if path.is_file():
            yield path


def sync_files() -> int:
    """
    Discover PDFs in the processing directory and register any new ones in the DB.
    Returns count of newly inserted documents.
    """
    settings = get_settings()
    root = settings.pdf_processing
    if not root.exists():
        raise FileNotFoundError(f"Processing directory does not exist: {root}")

    paths = list(_iter_pdfs(root))
    return register_files(paths)
