"""Text cleaning utilities for extracted PDF content."""

from __future__ import annotations

import re
import unicodedata

# Explicit ligature expansion map (belt + suspenders after NFKC)
LIGATURE_MAP = {
    "\uFB00": "ff",   # ﬀ
    "\uFB01": "fi",   # ﬁ
    "\uFB02": "fl",   # ﬂ
    "\uFB03": "ffi",  # ﬃ
    "\uFB04": "ffl",  # ﬄ
    "\uFB05": "st",   # ﬅ
    "\uFB06": "st",   # ﬆ
}


def _normalize_ligatures(text: str) -> str:
    """
    Normalize Unicode ligatures to ASCII equivalents.

    Uses NFKC normalization plus explicit replacement for reliability.
    Critical for academic PDFs where ligatures break search.
    """
    # 1. Unicode normalization (compatibility decomposition)
    text = unicodedata.normalize("NFKC", text)

    # 2. Explicit ligature replacement (belt + suspenders)
    for ligature, expansion in LIGATURE_MAP.items():
        text = text.replace(ligature, expansion)

    return text


def clean_text(raw: str) -> str:
    """
    Clean extracted PDF text.

    - Normalize Unicode ligatures to ASCII
    - Normalize line endings to \n
    - Drop lines that are only digits (page numbers)
    - Collapse runs of whitespace within lines
    - Collapse 3+ blank lines to max 2
    """
    # Normalize ligatures first (critical for search)
    text = _normalize_ligatures(raw)

    # Normalize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Process line by line
    lines = text.split("\n")
    cleaned_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        # Drop lines that are only digits (page numbers)
        if stripped and stripped.isdigit():
            continue
        # Normalize whitespace within the line
        if stripped:
            cleaned_lines.append(" ".join(stripped.split()))
        else:
            cleaned_lines.append("")

    # Re-join
    text = "\n".join(cleaned_lines)

    # Collapse 3+ consecutive blank lines to 2
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()
