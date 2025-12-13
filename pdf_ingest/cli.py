from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from . import queries
from .config import get_settings
from .db import init_db
from .es_client import ESClient
from .metadata import apply_manifest_to_db
from .pipeline import run_pipeline
from .worker import run_worker_loop


def main() -> None:
    parser = argparse.ArgumentParser(prog="pdf-ingest", description="PDF ingestion pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # stage
    stage_parser = subparsers.add_parser("stage", help="Copy PDFs from source to processing directory")
    stage_parser.add_argument(
        "--limit",
        type=int,
        help="Max number of PDFs to copy (default: all)",
    )
    stage_parser.add_argument(
        "--pattern",
        type=str,
        default="*.pdf",
        help="Glob pattern to match (default: *.pdf)",
    )

    # run
    run_parser = subparsers.add_parser("run", help="Run ingestion pipeline on staged PDFs")
    run_parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Max docs per batch (for DB fetch).",
    )

    # init-db
    subparsers.add_parser("init-db", help="Initialise database schema")

    # init-es
    subparsers.add_parser("init-es", help="Create Elasticsearch index if missing")

    # search
    search_parser = subparsers.add_parser(
        "search", help="Free-text search over indexed PDFs"
    )
    search_parser.add_argument("--query", "-q", required=True)
    search_parser.add_argument("--size", type=int, default=10)
    search_parser.add_argument("--year-from", type=int)
    search_parser.add_argument("--year-to", type=int)
    search_parser.add_argument("--tag", type=str, help="Filter by tag")
    search_parser.add_argument("--count", action="store_true", help="Only show count of matching documents")

    # grep (search with context)
    grep_parser = subparsers.add_parser(
        "grep", help="Search with context snippets around matches"
    )
    grep_parser.add_argument("--query", "-q", required=True)
    grep_parser.add_argument("--size", type=int, default=10, help="Number of documents")
    grep_parser.add_argument("--fragments", type=int, default=3, help="Snippets per document")
    grep_parser.add_argument("--fragment-size", type=int, default=150, help="Characters per snippet")
    grep_parser.add_argument("--sort", choices=["relevance", "year-desc", "year-asc"], default="relevance", help="Sort order")
    grep_parser.add_argument("--highlight", type=str, help="Term to highlight (defaults to query)")

    # enrich-metadata
    meta_parser = subparsers.add_parser(
        "enrich-metadata",
        help="Apply CSV metadata manifest to documents in the DB",
    )
    meta_parser.add_argument(
        "--manifest",
        type=str,
        default="metadata/papers_manifest_normalized.csv",
        help="Path to metadata CSV (default: metadata/papers_manifest_normalized.csv)",
    )
    meta_parser.add_argument(
        "--no-reset",
        action="store_true",
        help="Do not reset document status to NEW after updating metadata.",
    )

    # run-worker
    worker_parser = subparsers.add_parser(
        "run-worker",
        help="Run worker to process jobs from the queue",
    )
    worker_parser.add_argument(
        "--poll-interval",
        type=float,
        default=1.0,
        help="Seconds to wait between polls when no jobs (default: 1.0)",
    )
    worker_parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Stop after N iterations (for testing; default: run forever)",
    )

    args = parser.parse_args()

    if args.command == "stage":
        settings = get_settings()
        source = settings.pdf_source
        dest = settings.pdf_processing

        if not source.exists():
            print(f"Error: Source directory does not exist: {source}")
            return

        dest.mkdir(parents=True, exist_ok=True)

        # Get existing files in dest to skip
        existing = {f.name for f in dest.glob("*.pdf")}

        # Find source PDFs matching pattern
        source_pdfs = list(source.glob(args.pattern))
        to_copy = [p for p in source_pdfs if p.name not in existing]

        if args.limit:
            to_copy = to_copy[:args.limit]

        copied = 0
        for pdf in to_copy:
            shutil.copy2(pdf, dest / pdf.name)
            copied += 1
            if copied % 50 == 0:
                print(f"  Copied {copied} PDFs...")

        print(f"Staged {copied} PDFs to {dest}")
        print(f"  (skipped {len(source_pdfs) - len(to_copy)} already present)")
    elif args.command == "init-db":
        init_db()
    elif args.command == "init-es":
        ESClient().ensure_index()
    elif args.command == "run":
        run_pipeline(batch_size=args.batch_size)
    elif args.command == "search":
        q = args.query
        size = args.size
        year_from = args.year_from
        year_to = args.year_to
        tag = args.tag

        if args.count:
            count = queries.count_full_text_filtered(
                query=q,
                year_from=year_from,
                year_to=year_to,
                tag=tag,
            )
            print(count)
        else:
            hits = queries.search_full_text_filtered(
                query=q,
                year_from=year_from,
                year_to=year_to,
                tag=tag,
                size=size,
            )

            for h in hits:
                src = h["_source"]
                score = h.get("_score", 0.0)
                print(f"{score:6.2f}  {src.get('year')}  {src.get('title')}")
                print(f"        venue={src.get('venue')} tags={src.get('tags')}")
                print(f"        {src.get('file_path')}")
                print()
    elif args.command == "grep":
        hits = queries.search_with_context(
            query=args.query,
            size=args.size,
            fragment_size=args.fragment_size,
            num_fragments=args.fragments,
            sort=args.sort,
            highlight_term=args.highlight,
        )

        for h in hits:
            src = h["_source"]
            highlights = h.get("highlight", {}).get("full_text", [])
            score = h.get("_score") or 0.0

            print(f"{'='*80}")
            print(f"{score:6.2f}  {src.get('year')}  {src.get('title')}")
            print(f"        {src.get('file_path')}")
            print()
            for snippet in highlights:
                # Clean up the snippet for display
                print(f"    ...{snippet}...")
                print()
    elif args.command == "enrich-metadata":
        manifest_path = Path(args.manifest).resolve()
        reset_status = not args.no_reset
        updated = apply_manifest_to_db(
            manifest_path=manifest_path,
            reset_status=reset_status,
        )
        print(f"Updated metadata for {updated} documents.")
    elif args.command == "run-worker":
        run_worker_loop(
            poll_interval=args.poll_interval,
            max_iterations=args.max_iterations,
        )
    else:
        parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()
