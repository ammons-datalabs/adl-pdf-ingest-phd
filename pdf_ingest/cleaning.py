"""Text cleaning utilities for extracted PDF content."""

from __future__ import annotations

import re


def clean_text(raw: str) -> str:
    """
    Clean extracted PDF text.

    - Normalize line endings to \n
    - Drop lines that are only digits (page numbers)
    - Collapse runs of whitespace within lines
    - Collapse 3+ blank lines to max 2
    """
    # Normalize line endings
    text = raw.replace("\r\n", "\n").replace("\r", "\n")

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
