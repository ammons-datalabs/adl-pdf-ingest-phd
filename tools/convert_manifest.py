#!/usr/bin/env python3
"""Convert Paperpile CSV export to simplified manifest format."""

import csv
from pathlib import Path

INPUT_CSV = Path(__file__).parent.parent / "metadata" / "papers_manifest.csv"
OUTPUT_CSV = Path(__file__).parent.parent / "metadata" / "papers_manifest_normalized.csv"


def extract_filename(attachments: str) -> str:
    """Extract just the filename from Paperpile attachment path."""
    if not attachments:
        return ""
    # Format: "All Papers/X/Xia et al. 2025 - Title.pdf"
    # We want just the filename
    path = Path(attachments.strip())
    return path.name


def get_venue(row: dict) -> str:
    """Extract venue from various possible columns."""
    # Priority: Conference > Proceedings title > Journal > Source
    venue = (
        row.get("Conference", "").strip()
        or row.get("Proceedings title", "").strip()
        or row.get("Journal", "").strip()
        or row.get("Source", "").strip()
    )
    return venue


def main():
    rows_written = 0

    with open(INPUT_CSV, "r", encoding="utf-8", newline="") as infile:
        reader = csv.DictReader(infile)

        with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as outfile:
            writer = csv.DictWriter(
                outfile,
                fieldnames=["file_name", "title", "venue", "year", "tags"],
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()

            for row in reader:
                file_name = extract_filename(row.get("Attachments", ""))
                if not file_name:
                    continue  # Skip rows without attachments

                title = row.get("Title", "").strip()
                venue = get_venue(row)
                year = row.get("Publication year", "").strip()

                # Labels are semicolon-separated in our output format
                labels = row.get("Labels filed in", "").strip()
                # Paperpile might use different separators; normalize to semicolon
                tags = labels.replace(",", ";") if labels else ""

                writer.writerow({
                    "file_name": file_name,
                    "title": title,
                    "venue": venue,
                    "year": year,
                    "tags": tags,
                })
                rows_written += 1

    print(f"Wrote {rows_written} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
