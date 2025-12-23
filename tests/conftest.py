"""
Pytest configuration for test isolation.

Uses a separate test database (pdf_ingest_test) to avoid wiping production data.
"""
import os

import pytest

# Set test database BEFORE any other imports that might read config
# This ensures all tests use the test database, not production
os.environ["PG_DSN"] = "postgresql://postgres:postgres@localhost:5432/pdf_ingest_test"


def _ensure_test_database_exists():
    """Create the test database if it doesn't exist."""
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    # Connect to default 'postgres' database to create our test database
    conn = psycopg2.connect(
        "postgresql://postgres:postgres@localhost:5432/postgres"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    with conn.cursor() as cur:
        # Check if database exists
        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = 'pdf_ingest_test'"
        )
        if not cur.fetchone():
            cur.execute("CREATE DATABASE pdf_ingest_test")
            print("Created test database: pdf_ingest_test")

    conn.close()


@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    """
    Session-scoped fixture to set up the test database.

    Runs once at the start of the test session:
    1. Creates the test database if it doesn't exist
    2. Initializes the schema
    """
    _ensure_test_database_exists()

    # Now initialize schema in the test database
    from pdf_ingest.db import init_db
    init_db()

    yield

    # Optional: could drop test database here, but leaving it
    # makes re-runs faster and allows inspection after tests