"""
Tests for the source file filtering functionality.
"""

import pytest
from utils.source_filter import SourceFileFilter


class TestSourceFileFilter:
    """Test the SourceFileFilter class."""
    
    def test_initialization(self):
        """Test filter initialization."""
        filter_instance = SourceFileFilter()
        assert len(filter_instance.selected_files) == 0
        assert len(filter_instance.all_files) == 0
        assert filter_instance.filter_enabled is False
    
    def test_set_available_files(self):
        """Test setting available files."""
        filter_instance = SourceFileFilter()
        files = ["file1.xlsx", "file2.xlsx", "file3.xlsx"]
        
        filter_instance.set_available_files(files)
        
        assert filter_instance.all_files == files
        assert filter_instance.selected_files == set(files)  # All selected by default
    
    def test_select_files(self):
        """Test selecting specific files."""
        filter_instance = SourceFileFilter()
        all_files = ["file1.xlsx", "file2.xlsx", "file3.xlsx"]
        selected_files = ["file1.xlsx", "file2.xlsx"]
        
        filter_instance.set_available_files(all_files)
        filter_instance.select_files(selected_files)
        
        assert filter_instance.selected_files == set(selected_files)
        assert filter_instance.filter_enabled is True  # Less than all files selected
    
    def test_select_all_files(self):
        """Test selecting all files."""
        filter_instance = SourceFileFilter()
        files = ["file1.xlsx", "file2.xlsx", "file3.xlsx"]
        
        filter_instance.set_available_files(files)
        filter_instance.select_files(["file1.xlsx"])  # Select fewer first
        filter_instance.select_all_files()
        
        assert filter_instance.selected_files == set(files)
        assert filter_instance.filter_enabled is False  # All files selected
    
    def test_clear_selection(self):
        """Test clearing selection."""
        filter_instance = SourceFileFilter()
        files = ["file1.xlsx", "file2.xlsx"]
        
        filter_instance.set_available_files(files)
        filter_instance.clear_selection()
        
        assert len(filter_instance.selected_files) == 0
        assert filter_instance.filter_enabled is True
    
    def test_is_file_selected(self):
        """Test checking if file is selected."""
        filter_instance = SourceFileFilter()
        files = ["file1.xlsx", "file2.xlsx", "file3.xlsx"]
        selected = ["file1.xlsx", "file3.xlsx"]
        
        filter_instance.set_available_files(files)
        filter_instance.select_files(selected)
        
        assert filter_instance.is_file_selected("file1.xlsx") is True
        assert filter_instance.is_file_selected("file2.xlsx") is False
        assert filter_instance.is_file_selected("file3.xlsx") is True
    
    def test_get_filter_condition(self):
        """Test SQL filter condition generation."""
        filter_instance = SourceFileFilter()
        files = ["file1.xlsx", "file2.xlsx", "file3.xlsx"]
        selected = ["file1.xlsx", "file2.xlsx"]
        
        # No filter when all files selected
        filter_instance.set_available_files(files)
        assert filter_instance.get_filter_condition() is None
        
        # Filter condition when subset selected
        filter_instance.select_files(selected)
        condition = filter_instance.get_filter_condition()
        assert "source_file IN" in condition
        assert "file1.xlsx" in condition
        assert "file2.xlsx" in condition
    
    def test_apply_filter_to_query(self):
        """Test applying filter to SQL queries."""
        filter_instance = SourceFileFilter()
        files = ["file1.xlsx", "file2.xlsx", "file3.xlsx"]
        selected = ["file1.xlsx"]
        
        filter_instance.set_available_files(files)
        filter_instance.select_files(selected)
        
        # Test query without WHERE clause
        base_query = "SELECT * FROM accounts"
        filtered_query = filter_instance.apply_filter_to_query(base_query)
        assert "WHERE" in filtered_query
        assert "source_file IN" in filtered_query
        
        # Test query with existing WHERE clause
        base_query_with_where = "SELECT * FROM accounts WHERE account_code = '80'"
        filtered_query_with_where = filter_instance.apply_filter_to_query(base_query_with_where)
        assert "AND source_file IN" in filtered_query_with_where
    
    def test_get_status_summary(self):
        """Test status summary generation."""
        filter_instance = SourceFileFilter()
        files = ["file1.xlsx", "file2.xlsx", "file3.xlsx"]
        selected = ["file1.xlsx", "file2.xlsx"]
        
        filter_instance.set_available_files(files)
        filter_instance.select_files(selected)
        
        status = filter_instance.get_status_summary()
        
        assert status['total_files'] == 3
        assert status['selected_files'] == 2
        assert status['filter_enabled'] is True
        assert set(status['selected_file_names']) == set(selected)