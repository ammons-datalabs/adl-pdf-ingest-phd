"""
Tests for query building and search functions.
"""
from unittest.mock import patch, MagicMock

import pytest

from pdf_ingest.queries import (
    _parse_query_parts,
    _build_query_clause,
    search_full_text,
    search_by_year_range,
    search_by_tag,
    search_full_text_filtered,
    count_full_text_filtered,
    search_with_context,
    SEARCH_FIELDS,
)


class TestParseQueryParts:
    """Tests for query parsing logic."""

    def test_simple_terms(self):
        """Simple query with no quotes returns terms only."""
        terms, phrases = _parse_query_parts("hello world")
        assert terms == ["hello", "world"]
        assert phrases == []

    def test_single_phrase(self):
        """Quoted phrase is extracted."""
        terms, phrases = _parse_query_parts('"hello world"')
        assert terms == []
        assert phrases == ["hello world"]

    def test_mixed_terms_and_phrase(self):
        """Mix of terms and quoted phrase."""
        terms, phrases = _parse_query_parts('foo "hello world" bar')
        assert terms == ["foo", "bar"]
        assert phrases == ["hello world"]

    def test_multiple_phrases(self):
        """Multiple quoted phrases."""
        terms, phrases = _parse_query_parts('"phrase one" "phrase two"')
        assert terms == []
        assert phrases == ["phrase one", "phrase two"]

    def test_empty_query(self):
        """Empty query returns empty lists."""
        terms, phrases = _parse_query_parts("")
        assert terms == []
        assert phrases == []


class TestBuildQueryClause:
    """Tests for ES query DSL construction."""

    def test_simple_query_uses_multi_match(self):
        """Simple query produces multi_match."""
        clause = _build_query_clause("chunking")
        assert clause == {
            "multi_match": {
                "query": "chunking",
                "fields": SEARCH_FIELDS,
            }
        }

    def test_phrase_query_uses_phrase_type(self):
        """Quoted phrase produces phrase-type multi_match."""
        clause = _build_query_clause('"content-defined chunking"')
        assert clause == {
            "multi_match": {
                "query": "content-defined chunking",
                "fields": SEARCH_FIELDS,
                "type": "phrase",
            }
        }

    def test_mixed_query_uses_bool_must(self):
        """Mixed terms and phrase produces bool/must."""
        clause = _build_query_clause('dedup "message-locked encryption"')

        assert "bool" in clause
        assert "must" in clause["bool"]
        must = clause["bool"]["must"]
        assert len(must) == 2

        # First clause: regular terms
        assert must[0] == {
            "multi_match": {
                "query": "dedup",
                "fields": SEARCH_FIELDS,
            }
        }
        # Second clause: phrase
        assert must[1] == {
            "multi_match": {
                "query": "message-locked encryption",
                "fields": SEARCH_FIELDS,
                "type": "phrase",
            }
        }


class TestSearchFullText:
    """Tests for search_full_text with mocked ES client."""

    @patch("pdf_ingest.queries._client_and_index")
    def test_passes_correct_query_dsl(self, mock_client_and_index):
        """Verifies the correct DSL is passed to Elasticsearch."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_full_text("deduplication", size=5)

        mock_client.search.assert_called_once_with(
            index="papers",
            query={
                "multi_match": {
                    "query": "deduplication",
                    "fields": SEARCH_FIELDS,
                }
            },
            size=5,
        )

    @patch("pdf_ingest.queries._client_and_index")
    def test_returns_hits(self, mock_client_and_index):
        """Returns the hits from ES response."""
        mock_client = MagicMock()
        expected_hits = [{"_id": "1", "_source": {"title": "Test"}}]
        mock_client.search.return_value = {"hits": {"hits": expected_hits}}
        mock_client_and_index.return_value = (mock_client, "papers")

        result = search_full_text("test")

        assert result == expected_hits


class TestSearchFullTextFiltered:
    """Tests for search_full_text_filtered with filters."""

    @patch("pdf_ingest.queries._client_and_index")
    def test_query_only(self, mock_client_and_index):
        """Query without filters uses match_all in filter."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_full_text_filtered("test", size=10)

        call_args = mock_client.search.call_args
        body = call_args.kwargs["body"]
        assert body["query"]["bool"]["must"][0]["multi_match"]["query"] == "test"
        assert body["query"]["bool"]["filter"] == []

    @patch("pdf_ingest.queries._client_and_index")
    def test_with_year_filter(self, mock_client_and_index):
        """Year range filter is included in query."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_full_text_filtered("test", year_from=2020, year_to=2023)

        call_args = mock_client.search.call_args
        body = call_args.kwargs["body"]
        filters = body["query"]["bool"]["filter"]
        assert {"range": {"year": {"gte": 2020, "lte": 2023}}} in filters

    @patch("pdf_ingest.queries._client_and_index")
    def test_with_tag_filter(self, mock_client_and_index):
        """Tag filter is included in query."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_full_text_filtered("test", tag="Dedup")

        call_args = mock_client.search.call_args
        body = call_args.kwargs["body"]
        filters = body["query"]["bool"]["filter"]
        assert {"term": {"tags": "Dedup"}} in filters

    @patch("pdf_ingest.queries._client_and_index")
    def test_empty_query_uses_match_all(self, mock_client_and_index):
        """Empty query string uses match_all."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_full_text_filtered("", tag="Dedup")

        call_args = mock_client.search.call_args
        body = call_args.kwargs["body"]
        must = body["query"]["bool"]["must"]
        assert must == [{"match_all": {}}]


class TestCountFullTextFiltered:
    """Tests for count_full_text_filtered."""

    @patch("pdf_ingest.queries._client_and_index")
    def test_returns_count(self, mock_client_and_index):
        """Returns count from ES response."""
        mock_client = MagicMock()
        mock_client.count.return_value = {"count": 42}
        mock_client_and_index.return_value = (mock_client, "papers")

        result = count_full_text_filtered("test")

        assert result == 42

    @patch("pdf_ingest.queries._client_and_index")
    def test_uses_count_endpoint(self, mock_client_and_index):
        """Uses ES count endpoint, not search."""
        mock_client = MagicMock()
        mock_client.count.return_value = {"count": 0}
        mock_client_and_index.return_value = (mock_client, "papers")

        count_full_text_filtered("test", year_from=2020, tag="Dedup")

        mock_client.count.assert_called_once()
        mock_client.search.assert_not_called()


class TestSearchByYearRange:
    """Tests for search_by_year_range function."""

    @patch("pdf_ingest.queries._client_and_index")
    def test_passes_year_range_filter(self, mock_client_and_index):
        """Year range filter is correctly passed to ES."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_by_year_range("deduplication", year_from=2015, year_to=2020, size=5)

        call_args = mock_client.search.call_args
        query = call_args.kwargs["query"]

        # Check it's a bool query
        assert "bool" in query
        assert "must" in query["bool"]
        assert "filter" in query["bool"]

        # Check query
        assert query["bool"]["must"][0]["multi_match"]["query"] == "deduplication"

        # Check year filter
        filters = query["bool"]["filter"]
        assert {"range": {"year": {"gte": 2015, "lte": 2020}}} in filters

    @patch("pdf_ingest.queries._client_and_index")
    def test_returns_hits(self, mock_client_and_index):
        """Returns hits from ES response."""
        mock_client = MagicMock()
        expected_hits = [{"_id": "1", "_source": {"title": "Test", "year": 2018}}]
        mock_client.search.return_value = {"hits": {"hits": expected_hits}}
        mock_client_and_index.return_value = (mock_client, "papers")

        result = search_by_year_range("test", year_from=2015, year_to=2020)

        assert result == expected_hits


class TestSearchByTag:
    """Tests for search_by_tag function."""

    @patch("pdf_ingest.queries._client_and_index")
    def test_passes_tag_term_query(self, mock_client_and_index):
        """Tag term query is correctly passed to ES."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_by_tag("Chunking", size=10)

        call_args = mock_client.search.call_args
        query = call_args.kwargs["query"]

        # Check it's a term query
        assert query == {"term": {"tags": "Chunking"}}

    @patch("pdf_ingest.queries._client_and_index")
    def test_includes_venue_aggregation(self, mock_client_and_index):
        """Includes venue aggregation in query."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_by_tag("Chunking", size=10)

        call_args = mock_client.search.call_args
        aggs = call_args.kwargs["aggs"]

        assert "by_venue" in aggs
        assert aggs["by_venue"] == {"terms": {"field": "venue"}}

    @patch("pdf_ingest.queries._client_and_index")
    def test_returns_hits(self, mock_client_and_index):
        """Returns hits from ES response."""
        mock_client = MagicMock()
        expected_hits = [{"_id": "1", "_source": {"title": "Test", "tags": ["Chunking"]}}]
        mock_client.search.return_value = {"hits": {"hits": expected_hits}}
        mock_client_and_index.return_value = (mock_client, "papers")

        result = search_by_tag("Chunking")

        assert result == expected_hits


class TestSearchWithContext:
    """Tests for search_with_context (grep-style) function."""

    @patch("pdf_ingest.queries._client_and_index")
    def test_includes_highlight_config(self, mock_client_and_index):
        """Highlight configuration is included in query."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_with_context("FSL", size=5, fragment_size=150, num_fragments=3)

        call_args = mock_client.search.call_args
        highlight = call_args.kwargs["highlight"]

        assert "fields" in highlight
        assert "full_text" in highlight["fields"]
        assert highlight["fields"]["full_text"]["fragment_size"] == 150
        assert highlight["fields"]["full_text"]["number_of_fragments"] == 3
        assert highlight["fields"]["full_text"]["pre_tags"] == [">>>"]
        assert highlight["fields"]["full_text"]["post_tags"] == ["<<<"]

    @patch("pdf_ingest.queries._client_and_index")
    def test_sort_by_relevance_default(self, mock_client_and_index):
        """Default sort is relevance (no sort clause)."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_with_context("test", sort="relevance")

        call_args = mock_client.search.call_args
        sort = call_args.kwargs["sort"]

        assert sort is None

    @patch("pdf_ingest.queries._client_and_index")
    def test_sort_by_year_desc(self, mock_client_and_index):
        """Sort by year descending."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_with_context("test", sort="year-desc")

        call_args = mock_client.search.call_args
        sort = call_args.kwargs["sort"]

        assert sort == [{"year": {"order": "desc", "missing": "_last"}}]

    @patch("pdf_ingest.queries._client_and_index")
    def test_sort_by_year_asc(self, mock_client_and_index):
        """Sort by year ascending."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_with_context("test", sort="year-asc")

        call_args = mock_client.search.call_args
        sort = call_args.kwargs["sort"]

        assert sort == [{"year": {"order": "asc", "missing": "_last"}}]

    @patch("pdf_ingest.queries._client_and_index")
    def test_highlight_term_override(self, mock_client_and_index):
        """Custom highlight term is used when provided."""
        mock_client = MagicMock()
        mock_client.search.return_value = {"hits": {"hits": []}}
        mock_client_and_index.return_value = (mock_client, "papers")

        search_with_context("deduplication", highlight_term="FSL")

        call_args = mock_client.search.call_args
        highlight = call_args.kwargs["highlight"]

        assert "highlight_query" in highlight
        assert highlight["highlight_query"] == {"match": {"full_text": "FSL"}}

    @patch("pdf_ingest.queries._client_and_index")
    def test_returns_hits_with_highlights(self, mock_client_and_index):
        """Returns hits including highlight data."""
        mock_client = MagicMock()
        expected_hits = [
            {
                "_id": "1",
                "_source": {"title": "Test Paper"},
                "highlight": {"full_text": ["...>>>FSL<<< dataset..."]}
            }
        ]
        mock_client.search.return_value = {"hits": {"hits": expected_hits}}
        mock_client_and_index.return_value = (mock_client, "papers")

        result = search_with_context("FSL")

        assert result == expected_hits
        assert "highlight" in result[0]
