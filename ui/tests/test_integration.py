"""
Integration tests for the Financial Data Platform.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock
import polars as pl


class TestIntegration:
    """Integration tests for the application."""
    
    def test_import_all_modules(self):
        """Test that all main modules can be imported without errors."""
        try:
            # Test core imports
            from config.constants import Styles, UIConfig, Paths
            from config.data_schemas import EXCEL_FINANCIAL_SCHEMA, DATABASE_ACCOUNT_SCHEMA
            from utils.source_filter import SourceFileFilter
            from utils.error_boundaries import ErrorBoundary
            from components.table_components import create_bulma_table
            from components.cards import create_data_card
            from services.data_service import get_excel_files_data
            
            # If we get here, all imports succeeded
            assert True
        except ImportError as e:
            pytest.fail(f"Failed to import modules: {e}")
    
    def test_configuration_constants_exist(self):
        """Test that all required configuration constants exist."""
        from config.constants import Styles, UIConfig, Paths, ErrorMessages
        
        # Test Styles constants
        assert hasattr(Styles, 'CARD_CLASSES')
        assert hasattr(Styles, 'CARD_TITLE_CLASSES')
        
        # Test UIConfig constants
        assert hasattr(UIConfig, 'IFRAME_WIDTH')
        assert hasattr(UIConfig, 'IFRAME_HEIGHT')
        
        # Test Paths constants
        assert hasattr(Paths, 'DBT_PROJECT')
        assert hasattr(Paths, 'DATA_RAW')
        
        # Test ErrorMessages constants
        assert hasattr(ErrorMessages, 'DATABASE_CONNECTION')
    
    def test_data_schemas_valid(self):
        """Test that data schemas are properly defined."""
        from config.data_schemas import (
            EXCEL_FINANCIAL_SCHEMA,
            DATABASE_ACCOUNT_SCHEMA,
            EXCEL_FILE_METADATA_SCHEMA
        )
        
        # Test that schemas are dictionaries
        assert isinstance(EXCEL_FINANCIAL_SCHEMA, dict)
        assert isinstance(DATABASE_ACCOUNT_SCHEMA, dict)
        assert isinstance(EXCEL_FILE_METADATA_SCHEMA, dict)
        
        # Test that schemas have required fields
        assert 'account_code' in EXCEL_FINANCIAL_SCHEMA
        assert 'account_code' in DATABASE_ACCOUNT_SCHEMA
        assert 'filename' in EXCEL_FILE_METADATA_SCHEMA
    
    @patch('pathlib.Path')
    def test_excel_processing_pipeline(self, mock_path):
        """Test the complete Excel file processing pipeline."""
        from services.data_service import get_excel_files_data
        from utils.source_filter import source_filter
        
        # Mock directory structure
        mock_data_dir = Mock()
        mock_path.return_value = mock_data_dir
        mock_data_dir.exists.return_value = True
        
        # Create mock Excel files
        files = []
        for name in ["file1.xlsx", "file2.xlsx"]:
            mock_file = Mock()
            mock_file.name = name
            mock_file.stat.return_value.st_size = 1024 * 1024
            mock_file.stat.return_value.st_mtime = 1640995200
            files.append(mock_file)
        
        mock_data_dir.glob.return_value = files
        
        # Mock Excel reading
        with patch('config.data_schemas.read_excel_with_schema') as mock_read:
            mock_df = pl.DataFrame({
                "account": ["80", "81"],
                "amount": [1000.0, 2000.0]
            })
            mock_read.return_value = mock_df
            
            # Test the pipeline
            result = get_excel_files_data()
            
            # Verify results
            assert len(result) == 2
            assert all('filename' in item for item in result)
            assert all('total_debit' in item for item in result)
            assert all('total_credit' in item for item in result)
            
            # Verify source filter was updated
            assert len(source_filter.all_files) == 2
    
    def test_error_boundary_integration(self):
        """Test error boundary functionality."""
        from utils.error_boundaries import ErrorBoundary, database_boundary
        
        # Test ErrorBoundary context manager
        with ErrorBoundary('test', 'test operation') as boundary:
            # This should not raise an error
            result = 1 + 1
        
        assert boundary.error is None
        
        # Test with an error
        with ErrorBoundary('test', 'test operation') as boundary:
            raise ValueError("Test error")
        
        assert boundary.error is not None
        assert "Test error" in str(boundary.error)
    
    def test_table_component_integration(self, sample_account_data):
        """Test table component with real-like data."""
        from components.table_components import create_bulma_table
        
        columns = ["account_code", "account_name", "total_debit", "total_credit", "net_balance"]
        result = create_bulma_table(sample_account_data, columns)
        
        # Verify that a ui.html object is returned
        assert result is not None
        assert hasattr(result, 'content') or hasattr(result, '_content')
        
        # Get the HTML content
        if hasattr(result, 'content'):
            result_str = result.content
        elif hasattr(result, '_content'):
            result_str = result._content
        else:
            result_str = str(result)
        
        # Verify table structure
        assert "table" in result_str
        assert "thead" in result_str
        assert "tbody" in result_str
        
        # Verify data is present
        assert "80" in result_str  # account_code
        assert "Ontwikkelingskosten" in result_str  # account_name
        
        # Verify formatting
        assert "9,348,770.89" in result_str  # net_balance without EUR
    
    def test_source_filter_integration(self):
        """Test source filter integration with data access."""
        from utils.source_filter import SourceFileFilter
        from data_access import DataAccessLayer
        
        # Create filter instance
        filter_instance = SourceFileFilter()
        files = ["file1.xlsx", "file2.xlsx", "file3.xlsx"]
        selected = ["file1.xlsx", "file2.xlsx"]
        
        filter_instance.set_available_files(files)
        filter_instance.select_files(selected)
        
        # Test query modification
        base_query = "SELECT * FROM accounts"
        filtered_query = filter_instance.apply_filter_to_query(base_query)
        
        assert "WHERE source_file IN" in filtered_query
        assert "file1.xlsx" in filtered_query
        assert "file2.xlsx" in filtered_query
        assert "file3.xlsx" not in filtered_query
    
    def test_lazy_loading_integration(self):
        """Test lazy loading system integration."""
        from components.lazy_loader import lazy_loader, lazy_data
        
        # Register a test loader
        @lazy_data('test_data')
        def test_loader():
            return [{"id": 1, "name": "test"}]
        
        # Test data loading through the global lazy_loader instance
        data = lazy_loader.get_data('test_data')
        assert data == [{"id": 1, "name": "test"}]
        
        # Test caching
        data2 = lazy_loader.get_data('test_data')
        assert data2 == data  # Should be cached
        
        # Test status
        status = lazy_loader.get_lazy_data_status()
        assert status['loaded_count'] >= 1
        assert 'test_data' in status['details']