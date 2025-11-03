"""
Tests for pagination functionality.
"""

import pytest
from unittest.mock import Mock, patch
from components.pagination import PaginationState, create_pagination_controls, get_pagination_state
from components.table_components import create_paginated_table
from services.data_service import get_accounts_paginated, get_transactions_paginated


class TestPaginationState:
    """Test pagination state management."""
    
    def test_pagination_state_initialization(self):
        """Test basic pagination state initialization."""
        state = PaginationState(page_size=20)
        
        assert state.current_page == 1
        assert state.page_size == 20
        assert state.total_records == 0
        assert state.total_pages == 0
    
    def test_update_total_records(self):
        """Test updating total records and calculating pages."""
        state = PaginationState(page_size=20)
        
        # Test with exact multiple
        state.update_total_records(60)
        assert state.total_records == 60
        assert state.total_pages == 3
        
        # Test with remainder
        state.update_total_records(65)
        assert state.total_records == 65
        assert state.total_pages == 4
        
        # Test with less than one page
        state.update_total_records(10)
        assert state.total_records == 10
        assert state.total_pages == 1
    
    def test_get_offset_and_limit(self):
        """Test offset and limit calculations."""
        state = PaginationState(page_size=20)
        state.update_total_records(100)
        
        # Page 1
        assert state.get_offset() == 0
        assert state.get_limit() == 20
        
        # Page 2
        state.current_page = 2
        assert state.get_offset() == 20
        assert state.get_limit() == 20
        
        # Page 3
        state.current_page = 3
        assert state.get_offset() == 40
        assert state.get_limit() == 20
    
    def test_navigation_methods(self):
        """Test page navigation methods."""
        state = PaginationState(page_size=20)
        state.update_total_records(100)  # 5 pages
        
        # Test can_go methods
        assert not state.can_go_previous()  # On page 1
        assert state.can_go_next()
        
        # Go to middle page
        state.current_page = 3
        assert state.can_go_previous()
        assert state.can_go_next()
        
        # Go to last page
        state.current_page = 5
        assert state.can_go_previous()
        assert not state.can_go_next()
    
    def test_go_to_page(self):
        """Test going to specific page."""
        state = PaginationState(page_size=20)
        state.update_total_records(100)  # 5 pages
        
        # Valid page
        state.go_to_page(3)
        assert state.current_page == 3
        
        # Invalid pages (should not change)
        state.go_to_page(0)
        assert state.current_page == 3
        
        state.go_to_page(10)
        assert state.current_page == 3
    
    def test_get_page_info(self):
        """Test page info string generation."""
        state = PaginationState(page_size=20)
        state.update_total_records(65)
        
        # Page 1
        assert state.get_page_info() == "Showing 1-20 of 65 records"
        
        # Page 2
        state.current_page = 2
        assert state.get_page_info() == "Showing 21-40 of 65 records"
        
        # Last page (partial)
        state.current_page = 4
        assert state.get_page_info() == "Showing 61-65 of 65 records"


class TestPaginationFunctions:
    """Test pagination data functions."""
    
    @patch('services.data_service.get_sorted_accounts')
    def test_get_accounts_paginated(self, mock_get_accounts):
        """Test accounts pagination function."""
        # Mock data: 50 accounts
        mock_accounts = [{"account_code": f"ACC{i:03d}", "account_name": f"Account {i}"} for i in range(50)]
        mock_get_accounts.return_value = mock_accounts
        
        # Test first page
        page_data, total_count = get_accounts_paginated(0, 20)
        assert total_count == 50
        assert len(page_data) == 20
        assert page_data[0]["account_code"] == "ACC000"
        assert page_data[19]["account_code"] == "ACC019"
        
        # Test second page
        page_data, total_count = get_accounts_paginated(20, 20)
        assert total_count == 50
        assert len(page_data) == 20
        assert page_data[0]["account_code"] == "ACC020"
        assert page_data[19]["account_code"] == "ACC039"
        
        # Test last page (partial)
        page_data, total_count = get_accounts_paginated(40, 20)
        assert total_count == 50
        assert len(page_data) == 10
        assert page_data[0]["account_code"] == "ACC040"
        assert page_data[9]["account_code"] == "ACC049"
    
    @patch('services.data_service.get_limited_transactions')
    def test_get_transactions_paginated(self, mock_get_transactions):
        """Test transactions pagination function."""
        # Mock data: 30 transactions
        mock_transactions = [{"account_code": f"ACC{i:03d}", "description": f"Transaction {i}"} for i in range(30)]
        mock_get_transactions.return_value = mock_transactions
        
        # Test first page
        page_data, total_count = get_transactions_paginated(0, 15)
        assert total_count == 30
        assert len(page_data) == 15
        assert page_data[0]["account_code"] == "ACC000"
        assert page_data[14]["account_code"] == "ACC014"
        
        # Test second page
        page_data, total_count = get_transactions_paginated(15, 15)
        assert total_count == 30
        assert len(page_data) == 15
        assert page_data[0]["account_code"] == "ACC015"
        assert page_data[14]["account_code"] == "ACC029"
    
    def test_get_pagination_state(self):
        """Test global pagination state management."""
        # Clear any existing states
        from components.pagination import pagination_states
        pagination_states.clear()
        
        # Test creating new state
        state1 = get_pagination_state("table1", 20)
        assert state1.page_size == 20
        assert state1.current_page == 1
        
        # Test retrieving existing state
        state2 = get_pagination_state("table1", 30)  # Different page size should be ignored
        assert state2 is state1  # Same object
        assert state2.page_size == 20  # Original page size preserved
        
        # Test creating different state
        state3 = get_pagination_state("table2", 10)
        assert state3 is not state1
        assert state3.page_size == 10


class TestPaginationIntegration:
    """Test pagination integration with UI components."""
    
    def test_pagination_state_consistency(self):
        """Test that pagination state remains consistent across operations."""
        state = PaginationState(page_size=10)
        state.update_total_records(35)  # 4 pages
        
        # Simulate navigation
        state.go_next()
        assert state.current_page == 2
        assert state.get_page_info() == "Showing 11-20 of 35 records"
        
        state.go_next()
        assert state.current_page == 3
        assert state.get_page_info() == "Showing 21-30 of 35 records"
        
        state.go_next()
        assert state.current_page == 4
        assert state.get_page_info() == "Showing 31-35 of 35 records"
        
        # Can't go further
        state.go_next()
        assert state.current_page == 4  # Should stay on last page
    
    def test_pagination_with_empty_data(self):
        """Test pagination behavior with no data."""
        page_data, total_count = get_accounts_paginated(0, 20)
        
        # Even with no data, should return valid structure
        assert isinstance(page_data, list)
        assert isinstance(total_count, int)
        assert total_count >= 0


class TestPaginationEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_pagination_with_changing_data_size(self):
        """Test pagination when underlying data size changes."""
        state = PaginationState(page_size=20)
        
        # Start with large dataset
        state.update_total_records(100)
        state.current_page = 5  # Last page
        
        # Data shrinks
        state.update_total_records(60)  # Now only 3 pages
        assert state.current_page == 3  # Should be adjusted to last valid page
        assert state.total_pages == 3
    
    def test_pagination_state_boundaries(self):
        """Test pagination at boundaries."""
        state = PaginationState(page_size=20)
        state.update_total_records(20)  # Exactly one page
        
        assert state.total_pages == 1
        assert not state.can_go_next()
        assert not state.can_go_previous()
        
        # With zero records
        state.update_total_records(0)
        assert state.total_pages == 1  # Minimum 1 page
        assert state.get_page_info() == "Showing 1-0 of 0 records"