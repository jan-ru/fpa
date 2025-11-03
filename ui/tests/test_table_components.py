"""
Tests for table components and formatting.
"""

import pytest
from decimal import Decimal
from components.table_components import create_bulma_table


class TestTableComponents:
    """Test table component functionality."""
    
    def _get_html_content(self, ui_element):
        """Helper to extract HTML content from NiceGUI element."""
        if hasattr(ui_element, 'content'):
            return ui_element.content
        elif hasattr(ui_element, '_content'):
            return ui_element._content
        else:
            return str(ui_element)
    
    def test_create_bulma_table_empty_data(self):
        """Test creating table with empty data."""
        result = create_bulma_table([], ["col1", "col2"])
        result_str = self._get_html_content(result)
        assert "No data available" in result_str
    
    def test_create_bulma_table_basic(self, sample_account_data):
        """Test creating basic table."""
        columns = ["account_code", "account_name", "total_transactions"]
        result = create_bulma_table(sample_account_data, columns)
        
        # Check that table structure is created
        result_str = self._get_html_content(result)
        assert "table" in result_str
        assert "Account Code" in result_str
        assert "Account Name" in result_str
        assert "Total Transactions" in result_str
    
    def test_create_bulma_table_with_selection(self, sample_excel_data):
        """Test creating table with selection checkboxes."""
        columns = ["filename", "size_mb", "status"]
        result = create_bulma_table(sample_excel_data, columns, show_selection=True)
        
        result_str = self._get_html_content(result)
        assert "checkbox" in result_str
        assert "row-selector" in result_str
        assert "Select" in result_str  # Header
    
    def test_formatting_euro_amounts(self, sample_account_data):
        """Test Euro formatting for financial amounts."""
        # Create test data with Decimal values
        test_data = [
            {
                "account_code": "80",
                "debit_amount": Decimal("9348770.89"),
                "credit_amount": 696284.18,
                "balance": 8652486.71
            }
        ]
        
        columns = ["account_code", "debit_amount", "credit_amount", "balance"]
        result = create_bulma_table(test_data, columns)
        
        result_str = self._get_html_content(result)
        # Regular financial amounts should have EUR sign
        assert "€9,348,770.89" in result_str
        assert "€696,284.18" in result_str
    
    def test_formatting_total_debit_credit_no_euro(self):
        """Test that total_debit and total_credit don't get EUR sign."""
        test_data = [
            {
                "filename": "test.xlsx",
                "total_debit": 50000.00,
                "total_credit": 45000.00,
                "debit_amount": 1000.00  # This should get EUR
            }
        ]
        
        columns = ["filename", "total_debit", "total_credit", "debit_amount"]
        result = create_bulma_table(test_data, columns)
        
        result_str = self._get_html_content(result)
        # total_debit and total_credit should NOT have EUR
        assert "50,000.00" in result_str
        assert "45,000.00" in result_str
        # But debit_amount should have EUR
        assert "€1,000.00" in result_str
    
    def test_formatting_net_balance_no_euro(self):
        """Test that net_balance doesn't get EUR sign."""
        test_data = [
            {
                "account_code": "80",
                "net_balance": 8652486.71,
                "balance": 1000.00  # This should get EUR
            }
        ]
        
        columns = ["account_code", "net_balance", "balance"]
        result = create_bulma_table(test_data, columns)
        
        result_str = self._get_html_content(result)
        # net_balance should NOT have EUR
        assert "8,652,486.71" in result_str
        # But balance should have EUR
        assert "€1,000.00" in result_str
    
    def test_status_icon_formatting(self, sample_excel_data):
        """Test status column with icons."""
        columns = ["filename", "status"]
        result = create_bulma_table(sample_excel_data, columns)
        
        result_str = self._get_html_content(result)
        # Should contain icon markup for Processed status
        assert "fa-check-circle" in result_str
        assert "color: #28a745" in result_str
    
    def test_numeric_formatting_with_commas(self):
        """Test numeric formatting with thousand separators."""
        test_data = [
            {
                "account_code": "80",
                "total_transactions": 1234567,
                "size_mb": 123.45
            }
        ]
        
        columns = ["account_code", "total_transactions", "size_mb"]
        result = create_bulma_table(test_data, columns)
        
        result_str = self._get_html_content(result)
        assert "1,234,567" in result_str
        assert "123" in result_str  # size_mb gets formatted as integer-like
    
    def test_table_css_classes(self, sample_account_data):
        """Test that proper CSS classes are applied."""
        columns = ["account_code", "account_name"]
        result = create_bulma_table(sample_account_data, columns, table_id="test-table")
        
        result_str = self._get_html_content(result)
        assert 'id="test-table"' in result_str
        assert "table is-striped is-hoverable is-fullwidth" in result_str
        assert "table-container" in result_str