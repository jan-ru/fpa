"""
Tests for data service functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import polars as pl
from pathlib import Path
from services.data_service import get_excel_files_data, get_sorted_accounts, get_accounts_paginated, get_transactions_paginated, get_limited_transactions


@pytest.fixture(autouse=True)
def clear_caches():
    """Clear all caches before each test."""
    # Clear lazy loader cache
    try:
        from components.lazy_loader import lazy_loader
        lazy_loader.loaded_data.clear()
        lazy_loader.loading_states.clear()
        lazy_loader.error_states.clear()
    except ImportError:
        pass
    
    # Clear source filter state
    try:
        from utils.source_filter import source_filter
        source_filter.all_files.clear()
        source_filter.selected_files.clear()
    except ImportError:
        pass


class TestDataService:
    """Test data service functions."""
    
    @patch('pathlib.Path')
    def test_get_excel_files_data_no_directory(self, mock_path):
        """Test when data directory doesn't exist."""
        mock_path.return_value.exists.return_value = False
        
        result = get_excel_files_data()
        assert result == []
    
    @patch('services.data_service.read_excel_with_schema')
    @patch('pathlib.Path')
    def test_get_excel_files_data_basic(self, mock_path, mock_read_excel):
        """Test basic Excel files data retrieval."""
        # Mock directory and file structure
        mock_data_dir = Mock()
        mock_path.return_value = mock_data_dir
        mock_data_dir.exists.return_value = True
        
        # Mock file
        mock_file = Mock()
        mock_file.name = "test_file.xlsx"
        mock_file.stat.return_value.st_size = 1024 * 1024  # 1MB
        mock_file.stat.return_value.st_mtime = 1640995200  # 2022-01-01
        mock_data_dir.glob.return_value = [mock_file]
        
        # Mock Excel reading
        mock_df = pl.DataFrame({
            "col1": [1, 2, 3],
            "Debit Amount": [100.0, 200.0, 300.0],
            "Credit Amount": [50.0, 75.0, 125.0]
        })
        mock_read_excel.return_value = mock_df
        
        result = get_excel_files_data()
        
        assert len(result) == 1
        assert result[0]["filename"] == "test_file.xlsx"
        assert result[0]["size_mb"] == 1.0
        assert result[0]["n_columns"] == 3
        assert result[0]["n_rows"] == 3
        assert result[0]["total_debit"] == 600.0  # 100+200+300
        assert result[0]["total_credit"] == 250.0  # 50+75+125
    
    @patch('services.data_service.read_excel_with_schema')
    @patch('pathlib.Path')
    def test_get_excel_files_data_column_l_calculation(self, mock_path, mock_read_excel):
        """Test calculation using column L (index 11)."""
        # Mock directory and file structure
        mock_data_dir = Mock()
        mock_path.return_value = mock_data_dir
        mock_data_dir.exists.return_value = True
        
        # Mock file
        mock_file = Mock()
        mock_file.name = "test_file.xlsx"
        mock_file.stat.return_value.st_size = 1024 * 1024
        mock_file.stat.return_value.st_mtime = 1640995200
        mock_data_dir.glob.return_value = [mock_file]
        
        # Create DataFrame with 12+ columns, where column L (index 11) has the amounts
        columns = [f"col_{i}" for i in range(15)]  # 15 columns (A-O)
        data = {col: [0, 0, 0] for col in columns}
        data[columns[11]] = [1000.0, 2000.0, 3000.0]  # Column L with amounts
        
        mock_df = pl.DataFrame(data)
        mock_read_excel.return_value = mock_df
        
        result = get_excel_files_data()
        
        assert len(result) == 1
        assert result[0]["total_debit"] == 6000.0  # Sum of column L
    
    @patch('services.data_service.read_excel_with_schema')
    @patch('pathlib.Path')
    def test_get_excel_files_data_error_handling(self, mock_path, mock_read_excel):
        """Test error handling when Excel reading fails."""
        # Mock directory and file structure
        mock_data_dir = Mock()
        mock_path.return_value = mock_data_dir
        mock_data_dir.exists.return_value = True
        
        # Mock file
        mock_file = Mock()
        mock_file.name = "corrupt_file.xlsx"
        mock_file.stat.return_value.st_size = 1024
        mock_file.stat.return_value.st_mtime = 1640995200
        mock_data_dir.glob.return_value = [mock_file]
        
        # Mock Excel reading to raise exception
        mock_read_excel.side_effect = Exception("File corrupted")
        
        result = get_excel_files_data()
        
        assert len(result) == 1
        assert result[0]["filename"] == "corrupt_file.xlsx"
        assert result[0]["total_debit"] == 0.0
        assert result[0]["total_credit"] == 0.0
        assert result[0]["n_columns"] == 0
        assert result[0]["n_rows"] == 0
    
    @patch('pathlib.Path')
    def test_get_excel_files_data_skip_dump2024(self, mock_path):
        """Test that DUMP2024 files are marked as skipped."""
        # Mock directory and file structure
        mock_data_dir = Mock()
        mock_path.return_value = mock_data_dir
        mock_data_dir.exists.return_value = True
        
        # Mock file with DUMP2024 in name
        mock_file = Mock()
        mock_file.name = "DUMP2024_test.xlsx"
        mock_file.stat.return_value.st_size = 1024
        mock_file.stat.return_value.st_mtime = 1640995200
        mock_data_dir.glob.return_value = [mock_file]
        
        with patch('services.data_service.read_excel_with_schema') as mock_read:
            mock_read.side_effect = Exception("Skip this")
            result = get_excel_files_data()
        
        assert len(result) == 1
        assert result[0]["status"] == "Skipped"
    
    def test_get_excel_files_data_sorting(self):
        """Test that files are sorted by filename."""
        with patch('pathlib.Path') as mock_path:
            mock_data_dir = Mock()
            mock_path.return_value = mock_data_dir
            mock_data_dir.exists.return_value = True
            
            # Create mock files with different names
            files = []
            for name in ["zebra.xlsx", "alpha.xlsx", "beta.xlsx"]:
                mock_file = Mock()
                mock_file.name = name
                mock_file.stat.return_value.st_size = 1024
                mock_file.stat.return_value.st_mtime = 1640995200
                files.append(mock_file)
            
            mock_data_dir.glob.return_value = files
            
            with patch('services.data_service.read_excel_with_schema') as mock_read:
                mock_read.side_effect = Exception("Skip reading")
                result = get_excel_files_data()
            
            # Should be sorted alphabetically
            assert result[0]["filename"] == "alpha.xlsx"
            assert result[1]["filename"] == "beta.xlsx"  
            assert result[2]["filename"] == "zebra.xlsx"
    
    @patch('services.data_service.data_access')
    def test_get_sorted_accounts(self, mock_data_access):
        """Test getting sorted accounts."""
        # Mock data access response
        mock_accounts = [
            {"account_code": "81", "account_name": "Test B"},
            {"account_code": "80", "account_name": "Test A"}
        ]
        mock_data_access.get_account_summary.return_value = mock_accounts
        
        with patch('services.data_service.standardize_financial_data') as mock_standardize:
            mock_standardize.return_value = mock_accounts
            result = get_sorted_accounts()
        
        # Should be sorted by account_code
        assert len(result) == 2
        assert result[0]["account_code"] == "80"  # Comes first when sorted
        assert result[1]["account_code"] == "81"
    
    @patch('services.data_service.data_access')
    def test_get_sorted_accounts_empty(self, mock_data_access):
        """Test getting accounts when none exist."""
        # Clear any cached data
        from components.lazy_loader import lazy_loader
        lazy_loader.loaded_data.pop('accounts', None)
        
        mock_data_access.get_account_summary.return_value = []
        
        with patch('services.data_service.standardize_financial_data') as mock_standardize:
            mock_standardize.return_value = []
            result = get_sorted_accounts()
            assert result == []
    
    @patch('services.data_service.get_sorted_accounts')
    def test_get_accounts_paginated(self, mock_get_accounts):
        """Test paginated accounts function."""
        mock_accounts = [{"account_code": f"ACC{i}", "account_name": f"Account {i}"} for i in range(25)]
        mock_get_accounts.return_value = mock_accounts
        
        # Test pagination
        page_data, total_count = get_accounts_paginated(0, 10)
        assert total_count == 25
        assert len(page_data) == 10
        assert page_data[0]["account_code"] == "ACC0"
    
    @patch('services.data_service.get_limited_transactions')  
    def test_get_transactions_paginated(self, mock_get_transactions):
        """Test paginated transactions function."""
        mock_transactions = [{"account_code": f"ACC{i}", "description": f"Transaction {i}"} for i in range(15)]
        mock_get_transactions.return_value = mock_transactions
        
        # Test pagination
        page_data, total_count = get_transactions_paginated(0, 5)
        assert total_count == 15
        assert len(page_data) == 5
        assert page_data[0]["account_code"] == "ACC0"
    
    @patch('services.data_service.data_access')
    def test_get_limited_transactions_sorting(self, mock_data_access):
        """Test that transactions are sorted by account code and transaction date."""
        # Clear any cached data
        from components.lazy_loader import lazy_loader
        lazy_loader.loaded_data.pop('transactions', None)
        
        # Mock unsorted transaction data
        mock_transactions = [
            {"account_code": "8513", "transaction_date": "2024-03-14", "description": "Transaction 1"},
            {"account_code": "1300", "transaction_date": "2024-03-14", "description": "Transaction 2"},
            {"account_code": "1520", "transaction_date": "2024-02-23", "description": "Transaction 3"},
            {"account_code": "1520", "transaction_date": "2024-03-14", "description": "Transaction 4"},
        ]
        mock_data_access.get_transaction_details.return_value = mock_transactions
        
        with patch('services.data_service.standardize_financial_data') as mock_standardize:
            mock_standardize.return_value = mock_transactions
            result = get_limited_transactions()
        
        # Check that results are sorted by account_code, then by transaction_date
        assert len(result) == 4
        assert result[0]["account_code"] == "1300"  # First by account code
        assert result[1]["account_code"] == "1520"  # 1520 comes after 1300
        assert result[1]["transaction_date"] == "2024-02-23"  # Earlier date first
        assert result[2]["account_code"] == "1520"  # Same account code
        assert result[2]["transaction_date"] == "2024-03-14"  # Later date second
        assert result[3]["account_code"] == "8513"  # Last by account code