"""
Tests for data schema validation and standardization.
"""

import pytest
import polars as pl
from decimal import Decimal
from config.data_schemas import (
    standardize_financial_data,
    validate_schema,
    EXCEL_FINANCIAL_SCHEMA,
    DATABASE_ACCOUNT_SCHEMA
)


class TestDataSchemas:
    """Test data schema functionality."""
    
    def test_standardize_financial_data_empty(self):
        """Test standardizing empty data."""
        result = standardize_financial_data([])
        assert result == []
    
    def test_standardize_financial_data_with_decimals(self):
        """Test standardizing data with Decimal values."""
        data = [
            {
                "account_code": "80",
                "total_debit": Decimal("9348770.89"),
                "total_credit": Decimal("696284.18"),
                "net_balance": Decimal("8652486.71")
            }
        ]
        
        result = standardize_financial_data(data)
        
        assert len(result) == 1
        assert isinstance(result[0]["total_debit"], float)
        assert isinstance(result[0]["total_credit"], float)
        assert isinstance(result[0]["net_balance"], float)
        assert result[0]["total_debit"] == 9348770.89
    
    def test_standardize_financial_data_mixed_types(self):
        """Test standardizing data with mixed numeric types."""
        data = [
            {
                "account_code": "80",
                "total_debit": 1000.50,
                "total_credit": Decimal("500.25"),
                "transaction_count": 10
            }
        ]
        
        result = standardize_financial_data(data)
        
        assert isinstance(result[0]["total_debit"], float)
        assert isinstance(result[0]["total_credit"], float)
        assert result[0]["total_debit"] == 1000.50
        assert result[0]["total_credit"] == 500.25
    
    def test_validate_schema_strict_mode(self):
        """Test schema validation in strict mode."""
        # Create test dataframe with correct types
        df = pl.DataFrame({
            "account_code": ["80", "81"],
            "account_name": ["Test Account 1", "Test Account 2"],
            "total_debit": [1000.0, 2000.0]
        })
        
        expected_schema = {
            "account_code": pl.Utf8,
            "account_name": pl.Utf8,
            "total_debit": pl.Float64
        }
        
        # Should pass validation
        result = validate_schema(df, expected_schema, strict=True)
        assert result.shape == df.shape
    
    def test_validate_schema_lenient_mode(self):
        """Test schema validation in lenient mode."""
        # Create test dataframe with wrong types
        df = pl.DataFrame({
            "account_code": ["80", "81"],
            "total_debit": ["1000", "2000"]  # String instead of float
        })
        
        expected_schema = {
            "account_code": pl.Utf8,
            "total_debit": pl.Float64
        }
        
        # Should convert types
        result = validate_schema(df, expected_schema, strict=False)
        assert result["total_debit"].dtype == pl.Float64
    
    def test_excel_financial_schema_completeness(self):
        """Test that Excel schema contains expected columns."""
        expected_columns = [
            'account_code', 'account_name', 'transaction_date',
            'booking_date', 'description', 'amount', 'debit_amount',
            'credit_amount', 'balance', 'reference'
        ]
        
        for col in expected_columns:
            assert col in EXCEL_FINANCIAL_SCHEMA
    
    def test_database_account_schema_completeness(self):
        """Test that database account schema contains expected columns."""
        expected_columns = [
            'account_code', 'account_name', 'total_transactions',
            'total_debit', 'total_credit', 'net_balance'
        ]
        
        for col in expected_columns:
            assert col in DATABASE_ACCOUNT_SCHEMA
            
    def test_standardize_financial_data_preserves_non_financial(self):
        """Test that non-financial columns are preserved."""
        data = [
            {
                "account_code": "80",
                "account_name": "Test Account",
                "total_debit": Decimal("1000.00"),
                "description": "Test description"
            }
        ]
        
        result = standardize_financial_data(data)
        
        assert result[0]["account_code"] == "80"
        assert result[0]["account_name"] == "Test Account"
        assert result[0]["description"] == "Test description"
        assert isinstance(result[0]["total_debit"], float)