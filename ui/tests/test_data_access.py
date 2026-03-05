"""
Tests for the DataAccessLayer class.

Covers parameterized query construction, SQL injection prevention,
filter logic, and error handling.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from pathlib import Path
from datetime import datetime

from data_access import DataAccessLayer


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def dal():
    """Return a DataAccessLayer instance with patched DB paths."""
    layer = DataAccessLayer()
    layer.dbt_warehouse_path = Path("/fake/dev.duckdb")
    layer.iceberg_warehouse_path = Path("/fake/iceberg/warehouse")
    return layer


def _make_conn(rows, columns):
    """Build a mock DuckDB connection that returns rows + description."""
    conn = MagicMock()
    result = MagicMock()
    result.fetchall.return_value = rows
    result.fetchone.return_value = rows[0] if rows else None
    conn.execute.return_value = result
    conn.description = [(col,) for col in columns]
    # Support context-manager usage
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn


# ---------------------------------------------------------------------------
# get_filtered_transactions – parameterized query construction
# ---------------------------------------------------------------------------

class TestGetFilteredTransactions:
    """Verify that get_filtered_transactions builds safe parameterized queries."""

    def test_no_filters_produces_no_where_clause(self, dal):
        conn = _make_conn([], ["transaction_id"])
        with patch.object(dal, "get_dbt_connection", return_value=conn):
            with patch.object(dal, "query_to_dict_list", wraps=dal.query_to_dict_list):
                dal.get_filtered_transactions()
        # Should have called query_to_dict_list — confirm no WHERE fragment in query
        call_args = conn.execute.call_args
        if call_args:
            query = call_args[0][0]
            assert "WHERE" not in query.upper()

    def test_year_filter_uses_placeholders(self, dal):
        """String-concatenated years would look like 'IN (2021, 2022)';
        parameterized version must use '?' placeholders."""
        captured = {}

        def fake_query_to_dict_list(query, params=None, **kwargs):
            captured["query"] = query
            captured["params"] = params or []
            return []

        with patch.object(dal, "query_to_dict_list", side_effect=fake_query_to_dict_list):
            dal.get_filtered_transactions(years=[2021, 2022])

        assert "?" in captured["query"], "Expected ? placeholders in query"
        assert 2021 in captured["params"]
        assert 2022 in captured["params"]
        # No literal years in the query string itself
        assert "2021" not in captured["query"]
        assert "2022" not in captured["query"]

    def test_account_code_filter_uses_placeholders(self, dal):
        """Account codes must use ? placeholders, never raw string interpolation."""
        captured = {}

        def fake_query_to_dict_list(query, params=None, **kwargs):
            captured["query"] = query
            captured["params"] = params or []
            return []

        malicious_code = "'; DROP TABLE mart_transaction_details; --"
        with patch.object(dal, "query_to_dict_list", side_effect=fake_query_to_dict_list):
            dal.get_filtered_transactions(account_codes=[malicious_code])

        # The raw malicious string must NOT appear in the query
        assert malicious_code not in captured["query"], (
            "SQL injection payload found in query string – parameterization failed"
        )
        assert malicious_code in captured["params"], (
            "Malicious input should be passed as a safe parameter value"
        )

    def test_amount_category_filter_uses_placeholders(self, dal):
        captured = {}

        def fake_query_to_dict_list(query, params=None, **kwargs):
            captured["query"] = query
            captured["params"] = params or []
            return []

        with patch.object(dal, "query_to_dict_list", side_effect=fake_query_to_dict_list):
            dal.get_filtered_transactions(amount_categories=["Large", "Medium"])

        assert "Large" not in captured["query"]
        assert "Large" in captured["params"]

    def test_limit_and_offset_are_parameterized(self, dal):
        captured = {}

        def fake_query_to_dict_list(query, params=None, **kwargs):
            captured["query"] = query
            captured["params"] = params or []
            return []

        with patch.object(dal, "query_to_dict_list", side_effect=fake_query_to_dict_list):
            dal.get_filtered_transactions(limit=500, offset=100)

        assert 500 in captured["params"]
        assert 100 in captured["params"]
        # Should NOT have literal numbers in LIMIT/OFFSET clause
        assert "LIMIT 500" not in captured["query"]
        assert "OFFSET 100" not in captured["query"]

    def test_multiple_filters_combined(self, dal):
        captured = {}

        def fake_query_to_dict_list(query, params=None, **kwargs):
            captured["query"] = query
            captured["params"] = params or []
            return []

        with patch.object(dal, "query_to_dict_list", side_effect=fake_query_to_dict_list):
            dal.get_filtered_transactions(
                years=[2023],
                months=[1, 2, 3],
                account_codes=["80", "81"],
            )

        params = captured["params"]
        assert 2023 in params
        assert 1 in params
        assert 2 in params
        assert 3 in params
        assert "80" in params
        assert "81" in params

    def test_returns_list_of_dicts(self, dal):
        """get_filtered_transactions should return a list (via query_to_dict_list)."""
        expected = [{"transaction_id": 1, "account_code": "80", "account_name": "Test Account"}]

        with patch.object(dal, "query_to_dict_list", return_value=expected):
            result = dal.get_filtered_transactions()

        assert isinstance(result, list)
        assert result == expected


# ---------------------------------------------------------------------------
# get_account_summary
# ---------------------------------------------------------------------------

class TestGetAccountSummary:
    def test_no_limit_produces_no_limit_clause(self, dal):
        captured = {}

        def fake_qtdl(query, params=None, **kwargs):
            captured["query"] = query
            captured["params"] = params or []
            return []

        with patch.object(dal, "query_to_dict_list", side_effect=fake_qtdl):
            dal.get_account_summary()

        assert "LIMIT" not in captured["query"].upper()
        assert captured["params"] == []

    def test_limit_and_offset_parameterized(self, dal):
        captured = {}

        def fake_qtdl(query, params=None, **kwargs):
            captured["query"] = query
            captured["params"] = params or []
            return []

        with patch.object(dal, "query_to_dict_list", side_effect=fake_qtdl):
            dal.get_account_summary(limit=25, offset=50)

        assert 25 in captured["params"]
        assert 50 in captured["params"]
        assert "LIMIT 25" not in captured["query"]


# ---------------------------------------------------------------------------
# get_top_accounts_by_balance
# ---------------------------------------------------------------------------

class TestGetTopAccountsByBalance:
    def test_limit_is_parameterized(self, dal):
        captured = {}

        def fake_qtdl(query, params=None, **kwargs):
            captured["query"] = query
            captured["params"] = params or []
            return []

        with patch.object(dal, "query_to_dict_list", side_effect=fake_qtdl):
            dal.get_top_accounts_by_balance(limit=5)

        assert 5 in captured["params"]
        assert "LIMIT 5" not in captured["query"]


# ---------------------------------------------------------------------------
# get_transaction_details
# ---------------------------------------------------------------------------

class TestGetTransactionDetails:
    def test_limit_offset_parameterized(self, dal):
        captured = {}

        def fake_qtdl(query, params=None, **kwargs):
            captured["query"] = query
            captured["params"] = params or []
            return []

        with patch.object(dal, "query_to_dict_list", side_effect=fake_qtdl):
            dal.get_transaction_details(limit=200, offset=40)

        assert 200 in captured["params"]
        assert 40 in captured["params"]


# ---------------------------------------------------------------------------
# get_monthly_trends
# ---------------------------------------------------------------------------

class TestGetMonthlyTrends:
    def test_months_param_is_parameterized(self, dal):
        captured = {}

        def fake_qtdl(query, params=None, **kwargs):
            captured["query"] = query
            captured["params"] = params or []
            return []

        with patch.object(dal, "query_to_dict_list", side_effect=fake_qtdl):
            dal.get_monthly_trends(months=6)

        # The literal '6' should not appear in the query (only as params)
        assert "6" not in captured["query"].replace("GROUP BY", "").replace("ORDER BY", "")
        assert 6 in captured["params"]


# ---------------------------------------------------------------------------
# get_last_refresh_time – error handling
# ---------------------------------------------------------------------------

class TestGetLastRefreshTime:
    def test_returns_none_on_exception(self, dal):
        with patch.object(dal, "execute_dbt_query", side_effect=RuntimeError("db down")):
            result = dal.get_last_refresh_time()
        assert result is None, "Should return None when DB query fails"

    def test_returns_datetime_on_success(self, dal):
        now = datetime(2025, 1, 15, 10, 30)
        with patch.object(dal, "execute_dbt_query", return_value=(now,)):
            result = dal.get_last_refresh_time()
        assert result == now


# ---------------------------------------------------------------------------
# query_to_dict_list – column mapping
# ---------------------------------------------------------------------------

class TestQueryToDictList:
    def test_maps_rows_to_dicts(self, dal):
        conn = _make_conn(
            rows=[(1, "80", 999.99)],
            columns=["id", "account_code", "balance"]
        )
        with patch.object(dal, "get_dbt_connection", return_value=conn):
            with patch("utils.source_filter.source_filter") as mock_sf:
                mock_sf.apply_filter_to_query.return_value = ("SELECT 1", [])
                result = dal.query_to_dict_list("SELECT 1", apply_source_filter=False)

        assert result == [{"id": 1, "account_code": "80", "balance": 999.99}]

    def test_empty_result_returns_empty_list(self, dal):
        conn = _make_conn(rows=[], columns=["id"])
        with patch.object(dal, "get_dbt_connection", return_value=conn):
            result = dal.query_to_dict_list("SELECT 1", apply_source_filter=False)
        assert result == []


# ---------------------------------------------------------------------------
# get_available_versions – iceberg
# ---------------------------------------------------------------------------

class TestGetAvailableVersions:
    def test_returns_empty_when_path_missing(self, dal):
        dal.iceberg_warehouse_path = Path("/nonexistent/path")
        result = dal.get_available_versions()
        assert result == []
