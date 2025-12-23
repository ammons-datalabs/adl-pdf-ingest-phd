from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from . import queries
from .config import get_settings
from .db import (
    init_db,
    register_files,
    fetch_all_documents,
    create_pending_enhancement,
)
from .es_client import ESClient, bulk_sql_to_es
from .models import EnhancementType


def main() -> None:
    parser = argparse.ArgumentParser(prog="pdf-ingest", description="PDF ingestion pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # init-db
    subparsers.add_parser("init-db", help="Initialise database schema")

    # init-es
    subparsers.add_parser("init-es", help="Create Elasticsearch index if missing")

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

    # register
    register_parser = subparsers.add_parser(
        "register",
        help="Register PDFs in processing directory and queue for extraction",
    )
    register_parser.add_argument(
        "--no-queue",
        action="store_true",
        help="Only register, don't queue for extraction",
    )

    # run-robot
    robot_parser = subparsers.add_parser(
        "run-robot",
        help="Run a robot to process pending enhancements",
    )
    robot_parser.add_argument(
        "robot",
        choices=["pdf-extractor", "paperpile-sync"],
        help="Robot to run",
    )
    robot_parser.add_argument(
        "--max-iterations",
        type=int,
        default=None,
        help="Stop after N iterations (for testing; default: run forever)",
    )
    robot_parser.add_argument(
        "--manifest",
        type=str,
        default="metadata/papers_manifest.csv",
        help="Path to manifest CSV (for paperpile-sync)",
    )

    # queue-metadata
    subparsers.add_parser(
        "queue-metadata",
        help="Queue all documents for metadata sync (PAPERPILE_METADATA)",
    )

    # sync-es
    sync_parser = subparsers.add_parser(
        "sync-es",
        help="Sync documents from SQL to Elasticsearch",
    )
    sync_parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Delete and recreate index before syncing",
    )

    # es-status
    subparsers.add_parser(
        "es-status",
        help="Show Elasticsearch index status (version, document count)",
    )

    # es-migrate
    subparsers.add_parser(
        "es-migrate",
        help="Migrate ES index to new version with updated mapping",
    )

    # es-rollback
    subparsers.add_parser(
        "es-rollback",
        help="Roll back ES index to previous version",
    )

    # es-cleanup
    cleanup_parser = subparsers.add_parser(
        "es-cleanup",
        help="Delete old ES index versions",
    )
    cleanup_parser.add_argument(
        "--keep",
        type=int,
        default=2,
        help="Number of versions to keep (default: 2)",
    )

    # search
    search_parser = subparsers.add_parser(
        "search", help="Free-text search over indexed PDFs"
    )
    search_parser.add_argument("--query", "-q", required=True)
    search_parser.add_argument("--size", type=int, default=10)
    search_parser.add_argument("--year-from", type=int)
    search_parser.add_argument("--year-to", type=int)
    search_parser.add_argument("--tag", type=str, help="Filter by tag")
    search_parser.add_argument("--folder", type=str, help="Filter by folder")
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
    grep_parser.add_argument("--tag", type=str, help="Filter by Paperpile tag")
    grep_parser.add_argument("--folder", type=str, help="Filter by Paperpile folder")
    grep_parser.add_argument("--year-from", type=int, help="Minimum publication year")
    grep_parser.add_argument("--year-to", type=int, help="Maximum publication year")

    # venues (aggregate by venue)
    venues_parser = subparsers.add_parser(
        "venues", help="Show top venues for matching papers"
    )
    venues_parser.add_argument("--query", "-q", type=str, help="Full-text search query")
    venues_parser.add_argument("--size", type=int, default=20, help="Number of venues to show")
    venues_parser.add_argument("--tag", type=str, help="Filter by Paperpile tag")
    venues_parser.add_argument("--folder", type=str, help="Filter by Paperpile folder")
    venues_parser.add_argument("--year-from", type=int, help="Minimum publication year")
    venues_parser.add_argument("--year-to", type=int, help="Maximum publication year")

    args = parser.parse_args()

    if args.command == "init-db":
        init_db()
        print("Database schema initialised.")

    elif args.command == "init-es":
        ESClient().ensure_index()
        print("Elasticsearch index ready.")

    elif args.command == "stage":
        settings = get_settings()
        source = settings.pdf_source
        dest = settings.pdf_processing

        if not source.exists():
            print(f"Error: Source directory does not exist: {source}")
            return

        dest.mkdir(parents=True, exist_ok=True)

        existing = {f.name for f in dest.glob("*.pdf")}
        source_pdfs = list(source.glob(args.pattern))
        to_copy = [p for p in source_pdfs if p.name not in existing]

        if args.limit:
            to_copy = to_copy[: args.limit]

        copied = 0
        for pdf in to_copy:
            shutil.copy2(pdf, dest / pdf.name)
            copied += 1
            if copied % 50 == 0:
                print(f"  Copied {copied} PDFs...")

        print(f"Staged {copied} PDFs to {dest}")
        print(f"  (skipped {len(source_pdfs) - len(to_copy)} already present)")

    elif args.command == "register":
        init_db()
        settings = get_settings()
        pdfs = list(settings.pdf_processing.glob("*.pdf"))

        count = register_files(pdfs)
        print(f"Registered {count} new documents.")

        if not args.no_queue:
            # Queue all documents for extraction
            documents = fetch_all_documents()
            queued = 0
            for doc in documents:
                create_pending_enhancement(doc.id, EnhancementType.FULL_TEXT)
                queued += 1
            print(f"Queued {queued} documents for extraction.")

    elif args.command == "run-robot":
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        if args.robot == "pdf-extractor":
            from .robots.pdf_extractor import run_loop
            run_loop(max_iterations=args.max_iterations)
        elif args.robot == "paperpile-sync":
            from .robots.paperpile_sync import run_loop as paperpile_run_loop
            init_db()
            manifest_path = Path(args.manifest).resolve()
            if not manifest_path.exists():
                print(f"Error: Manifest not found: {manifest_path}")
                return
            paperpile_run_loop(manifest_path, max_iterations=args.max_iterations)

    elif args.command == "queue-metadata":
        init_db()
        documents = fetch_all_documents()
        queued = 0
        for doc in documents:
            create_pending_enhancement(doc.id, EnhancementType.PAPERPILE_METADATA)
            queued += 1
        print(f"Queued {queued} documents for metadata sync.")

    elif args.command == "sync-es":
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        if args.rebuild:
            es = ESClient()
            es.delete_index()
            print("Deleted existing index.")
        count = bulk_sql_to_es()
        print(f"Synced {count} documents to Elasticsearch.")

    elif args.command == "es-status":
        es = ESClient()
        status = es.manager.status()
        if not status.get("exists"):
            print(f"Index '{status['alias']}' does not exist.")
        else:
            print(f"Alias:     {status['alias']}")
            print(f"Index:     {status['current_index']}")
            print(f"Version:   {status['version']}")
            print(f"Documents: {status['document_count']}")
            if len(status['all_versions']) > 1:
                print(f"All versions: {', '.join(status['all_versions'])}")

    elif args.command == "es-migrate":
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        es = ESClient()
        new_index = es.manager.migrate()
        print(f"Migrated to {new_index}")

    elif args.command == "es-rollback":
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        es = ESClient()
        try:
            old_index = es.manager.rollback()
            print(f"Rolled back to {old_index}")
        except ValueError as e:
            print(f"Error: {e}")

    elif args.command == "es-cleanup":
        import logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        es = ESClient()
        deleted = es.manager.delete_old_versions(keep_latest=args.keep)
        if deleted:
            print(f"Deleted: {', '.join(deleted)}")
        else:
            print("No old versions to delete.")

    elif args.command == "search":
        q = args.query
        size = args.size
        year_from = args.year_from
        year_to = args.year_to
        tag = args.tag
        folder = args.folder

        if args.count:
            count = queries.count_full_text_filtered(
                query=q,
                year_from=year_from,
                year_to=year_to,
                tag=tag,
                folder=folder,
            )
            print(count)
        else:
            hits = queries.search_full_text_filtered(
                query=q,
                year_from=year_from,
                year_to=year_to,
                tag=tag,
                folder=folder,
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
            year_from=args.year_from,
            year_to=args.year_to,
            tag=args.tag,
            folder=args.folder,
        )

        for h in hits:
            src = h["_source"]
            highlights = h.get("highlight", {}).get("full_text", [])
            score = h.get("_score") or 0.0

            print(f"{'=' * 80}")
            print(f"{score:6.2f}  {src.get('year')}  {src.get('title')}")
            print(f"        {src.get('file_path')}")
            print()
            for snippet in highlights:
                print(f"    ...{snippet}...")
                print()

    elif args.command == "venues":
        results = queries.aggregate_venues(
            query=args.query,
            year_from=args.year_from,
            year_to=args.year_to,
            tag=args.tag,
            folder=args.folder,
            size=args.size,
        )

        if not results:
            print("No venues found.")
        else:
            for r in results:
                print(f"{r['count']:4d}  {r['venue']}")

    else:
        parser.error(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()