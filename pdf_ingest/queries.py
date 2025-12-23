from __future__ import annotations

from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch

from .config import get_settings

# Boosted fields for multi-match queries
# Higher boosts for structured metadata (title, abstract, keywords)
# Lower boost for full_text to avoid drowning signal in noise
SEARCH_FIELDS = [
    "title^4",      # Title is most important
    "abstract^3",   # Abstract is highly relevant
    "keywords^3",   # Keywords are highly relevant
    "authors^2",    # Author names
    "full_text",    # Full text (no boost)
]


def _client_and_index():
    settings = get_settings()
    return Elasticsearch(settings.es_url), settings.es_index


def search_full_text(query: str, size: int = 10) -> List[Dict[str, Any]]:
    client, index = _client_and_index()
    resp = client.search(
        index=index,
        query={
            "multi_match": {
                "query": query,
                "fields": SEARCH_FIELDS,
            }
        },
        size=size,
    )
    return resp["hits"]["hits"]


def search_by_year_range(
    query: str,
    year_from: int,
    year_to: int,
    size: int = 10,
) -> List[Dict[str, Any]]:
    client, index = _client_and_index()
    resp = client.search(
        index=index,
        query={
            "bool": {
                "must": [
                    {"multi_match": {"query": query, "fields": SEARCH_FIELDS}}
                ],
                "filter": [
                    {"range": {"year": {"gte": year_from, "lte": year_to}}},
                ],
            }
        },
        size=size,
    )
    return resp["hits"]["hits"]


def search_by_tag(tag: str, size: int = 10) -> List[Dict[str, Any]]:
    client, index = _client_and_index()
    resp = client.search(
        index=index,
        query={"term": {"tags": tag}},
        aggs={
            "by_venue": {"terms": {"field": "venue"}},
        },
        size=size,
    )
    return resp["hits"]["hits"]


def _parse_query_parts(query: str) -> tuple[list[str], list[str]]:
    """
    Parse query into regular terms and phrase terms.
    Returns (terms, phrases) where phrases were quoted in the original query.
    :rtype: tuple[list[str], list[str]]
    """
    import re
    phrases = re.findall(r'"([^"]+)"', query)
    # Remove quoted parts to get remaining terms
    remaining = re.sub(r'"[^"]+"', '', query).strip()
    terms = remaining.split() if remaining else []
    return terms, phrases


def _build_query_clause(query: str) -> Dict[str, Any]:
    """Build the appropriate query clause, using phrase matching for quoted parts."""
    terms, phrases = _parse_query_parts(query)

    # If just a simple query with no quotes
    if not phrases and terms:
        return {
            "multi_match": {"query": " ".join(terms), "fields": SEARCH_FIELDS}
        }

    # If just a single phrase (entire query quoted)
    if phrases and not terms and len(phrases) == 1:
        return {
            "multi_match": {
                "query": phrases[0],
                "fields": SEARCH_FIELDS,
                "type": "phrase",
            }
        }

    # Mixed: combine terms and phrases with bool/must
    must_clauses: list[Dict[str, Any]] = []

    if terms:
        must_clauses.append({
            "multi_match": {"query": " ".join(terms), "fields": SEARCH_FIELDS}
        })

    for phrase in phrases:
        must_clauses.append({
            "multi_match": {
                "query": phrase,
                "fields": SEARCH_FIELDS,
                "type": "phrase",
            }
        })

    return {"bool": {"must": must_clauses}}


def search_full_text_filtered(
    query: str,
    year_from: int | None = None,
    year_to: int | None = None,
    tag: str | None = None,
    folder: str | None = None,
    size: int = 10,
) -> List[Dict[str, Any]]:
    client, index = _client_and_index()

    must: list[Dict[str, Any]] = []
    if query:
        must.append(_build_query_clause(query))

    filters: list[Dict[str, Any]] = []
    if year_from is not None or year_to is not None:
        yr_from = year_from if year_from is not None else 0
        yr_to = year_to if year_to is not None else 9999
        filters.append({"range": {"year": {"gte": yr_from, "lte": yr_to}}})

    if tag:
        filters.append({"term": {"tags": tag}})

    if folder:
        filters.append({"term": {"folders": folder}})

    body: Dict[str, Any] = {
        "query": {
            "bool": {
                "must": must if must else [{"match_all": {}}],
                "filter": filters,
            }
        }
    }

    resp = client.search(index=index, body=body, size=size)
    return resp["hits"]["hits"]


def count_full_text_filtered(
    query: str,
    year_from: int | None = None,
    year_to: int | None = None,
    tag: str | None = None,
    folder: str | None = None,
) -> int:
    """
    Count documents matching the query and filters.
    """
    client, index = _client_and_index()

    must: list[Dict[str, Any]] = []
    if query:
        must.append(_build_query_clause(query))

    filters: list[Dict[str, Any]] = []
    if year_from is not None or year_to is not None:
        yr_from = year_from if year_from is not None else 0
        yr_to = year_to if year_to is not None else 9999
        filters.append({"range": {"year": {"gte": yr_from, "lte": yr_to}}})

    if tag:
        filters.append({"term": {"tags": tag}})

    if folder:
        filters.append({"term": {"folders": folder}})

    body: Dict[str, Any] = {
        "query": {
            "bool": {
                "must": must if must else [{"match_all": {}}],
                "filter": filters,
            }
        }
    }

    resp = client.count(index=index, body=body)
    return resp["count"]


def search_with_context(
    query: str,
    size: int = 10,
    fragment_size: int = 150,
    num_fragments: int = 3,
    sort: str = "relevance",
    highlight_term: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    tag: str | None = None,
    folder: str | None = None,
) -> List[Dict[str, Any]]:
    """
    Search with highlighted snippets showing context around matches.
    sort: "relevance" (default), "year-desc", or "year-asc"
    highlight_term: if provided, highlight this term instead of the query
    year_from, year_to: filter by publication year range
    tag: filter by Paperpile tag
    folder: filter by Paperpile folder
    """
    client, index = _client_and_index()

    sort_clause: list | None = None
    if sort == "year-desc":
        sort_clause = [{"year": {"order": "desc", "missing": "_last"}}]
    elif sort == "year-asc":
        sort_clause = [{"year": {"order": "asc", "missing": "_last"}}]

    highlight_config: Dict[str, Any] = {
        "fields": {
            "full_text": {
                "fragment_size": fragment_size,
                "number_of_fragments": num_fragments,
                "pre_tags": [">>>"],
                "post_tags": ["<<<"],
            }
        }
    }

    # Use separate highlight query if highlight_term provided
    if highlight_term:
        highlight_config["highlight_query"] = {
            "match": {"full_text": highlight_term}
        }

    # Build query with optional filters
    must: list[Dict[str, Any]] = []
    if query:
        must.append(_build_query_clause(query))

    filters: list[Dict[str, Any]] = []
    if year_from is not None or year_to is not None:
        yr_from = year_from if year_from is not None else 0
        yr_to = year_to if year_to is not None else 9999
        filters.append({"range": {"year": {"gte": yr_from, "lte": yr_to}}})

    if tag:
        filters.append({"term": {"tags": tag}})

    if folder:
        filters.append({"term": {"folders": folder}})

    query_body: Dict[str, Any] = {
        "bool": {
            "must": must if must else [{"match_all": {}}],
            "filter": filters,
        }
    }

    resp = client.search(
        index=index,
        query=query_body,
        highlight=highlight_config,
        size=size,
        sort=sort_clause,
    )
    return resp["hits"]["hits"]


def aggregate_venues(
    query: str | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
    tag: str | None = None,
    folder: str | None = None,
    size: int = 20,
) -> List[Dict[str, Any]]:
    """
    Aggregate documents by venue, optionally filtered by query and metadata.

    Returns list of dicts with 'venue' and 'count' keys, sorted by count descending.
    """
    client, index = _client_and_index()

    must: list[Dict[str, Any]] = []
    if query:
        must.append(_build_query_clause(query))

    filters: list[Dict[str, Any]] = []
    if year_from is not None or year_to is not None:
        yr_from = year_from if year_from is not None else 0
        yr_to = year_to if year_to is not None else 9999
        filters.append({"range": {"year": {"gte": yr_from, "lte": yr_to}}})

    if tag:
        filters.append({"term": {"tags": tag}})

    if folder:
        filters.append({"term": {"folders": folder}})

    body: Dict[str, Any] = {
        "size": 0,  # Don't return documents, just aggregations
        "query": {
            "bool": {
                "must": must if must else [{"match_all": {}}],
                "filter": filters,
            }
        },
        "aggs": {
            "venues": {"terms": {"field": "venue", "size": size}}
        },
    }

    resp = client.search(index=index, body=body)
    buckets = resp["aggregations"]["venues"]["buckets"]

    return [{"venue": b["key"], "count": b["doc_count"]} for b in buckets]
