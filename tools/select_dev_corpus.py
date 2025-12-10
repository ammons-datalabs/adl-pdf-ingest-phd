#!/usr/bin/env python3
"""
Select a diverse development corpus of 30 PDFs from a larger collection.

Selects PDFs across different page count ranges to ensure variety:
- Short (1-10 pages): 10 PDFs
- Medium (11-50 pages): 10 PDFs
- Long (51+ pages): 10 PDFs

Usage:
    python tools/select_dev_corpus.py <source_dir>
"""

import sys
import random
from pathlib import Path

import fitz  # PyMuPDF


def get_page_count(pdf_path: Path) -> int | None:
    """Return page count for a PDF, or None if it can't be read."""
    try:
        with fitz.open(pdf_path) as doc:
            return len(doc)
    except Exception:
        return None


def scan_pdfs(source_dir: Path) -> list[tuple[Path, int]]:
    """Scan all PDFs in source_dir and return list of (path, page_count)."""
    results = []
    pdf_files = list(source_dir.glob("*.pdf"))

    print(f"Scanning {len(pdf_files)} PDFs...")

    for i, pdf_path in enumerate(pdf_files, 1):
        if i % 100 == 0:
            print(f"  Scanned {i}/{len(pdf_files)}...")

        page_count = get_page_count(pdf_path)
        if page_count is not None and page_count > 0:
            results.append((pdf_path, page_count))

    print(f"Successfully scanned {len(results)} readable PDFs.")
    return results


def select_diverse_corpus(
    pdfs: list[tuple[Path, int]],
    target_count: int = 30
) -> list[Path]:
    """Select a diverse set of PDFs across page count ranges."""

    # Categorize by page count
    short = [(p, c) for p, c in pdfs if c <= 10]
    medium = [(p, c) for p, c in pdfs if 11 <= c <= 50]
    long = [(p, c) for p, c in pdfs if c > 50]

    print(f"\nDistribution: {len(short)} short, {len(medium)} medium, {len(long)} long")

    # Target 10 from each category, but adjust if categories are sparse
    per_category = target_count // 3

    random.seed(42)  # Reproducible selection

    selected = []

    # Select from each category
    for category, name in [(short, "short"), (medium, "medium"), (long, "long")]:
        available = min(len(category), per_category)
        picks = random.sample(category, available)
        selected.extend([p for p, _ in picks])
        print(f"  Selected {available} {name} PDFs")

    # If we don't have enough, fill from any remaining
    if len(selected) < target_count:
        all_paths = {p for p, _ in pdfs}
        remaining = all_paths - set(selected)
        remaining_list = list(remaining)
        random.shuffle(remaining_list)
        needed = target_count - len(selected)
        selected.extend(remaining_list[:needed])
        print(f"  Added {min(needed, len(remaining_list))} additional PDFs")

    return selected[:target_count]


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/select_dev_corpus.py <source_dir>")
        sys.exit(1)

    source_dir = Path(sys.argv[1])
    if not source_dir.is_absolute():
        source_dir = Path.cwd() / source_dir

    if not source_dir.exists():
        print(f"Error: Directory not found: {source_dir}")
        sys.exit(1)

    # Scan all PDFs
    pdfs = scan_pdfs(source_dir)

    if not pdfs:
        print("No readable PDFs found.")
        sys.exit(1)

    # Select diverse corpus
    selected = select_diverse_corpus(pdfs, target_count=30)

    # Write output
    output_file = Path.cwd() / "dev_corpus_list.txt"
    with open(output_file, "w") as f:
        for pdf_path in selected:
            f.write(f"{pdf_path}\n")

    print(f"\nSelected {len(selected)} PDFs. Written to {output_file}")


if __name__ == "__main__":
    main()
