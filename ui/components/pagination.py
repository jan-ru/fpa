"""
Pagination components for tables with large datasets.
"""

from typing import Dict, List, Callable, Optional
from nicegui import ui
from config.constants import Styles


class PaginationState:
    """Manages pagination state for a table."""
    
    def __init__(self, page_size: int = 20):
        self.current_page = 1
        self.page_size = page_size
        self.total_records = 0
        self.total_pages = 0
        
    def update_total_records(self, total: int):
        """Update total records and recalculate total pages."""
        self.total_records = total
        self.total_pages = max(1, (total + self.page_size - 1) // self.page_size)
        
        # Ensure current page is valid
        if self.current_page > self.total_pages:
            self.current_page = self.total_pages
    
    def get_offset(self) -> int:
        """Get the offset for database queries."""
        return (self.current_page - 1) * self.page_size
    
    def get_limit(self) -> int:
        """Get the limit for database queries."""
        return self.page_size
    
    def can_go_previous(self) -> bool:
        """Check if we can go to previous page."""
        return self.current_page > 1
    
    def can_go_next(self) -> bool:
        """Check if we can go to next page."""
        return self.current_page < self.total_pages
    
    def go_previous(self):
        """Go to previous page."""
        if self.can_go_previous():
            self.current_page -= 1
    
    def go_next(self):
        """Go to next page.""" 
        if self.can_go_next():
            self.current_page += 1
    
    def go_to_page(self, page: int):
        """Go to specific page."""
        if 1 <= page <= self.total_pages:
            self.current_page = page
    
    def get_page_info(self) -> str:
        """Get human-readable page information."""
        start = self.get_offset() + 1
        end = min(start + self.page_size - 1, self.total_records)
        return f"Showing {start}-{end} of {self.total_records} records"


def create_pagination_controls(
    pagination_state: PaginationState,
    on_page_change: Callable[[int], None],
    table_id: str = "table"
) -> ui.element:
    """
    Create pagination controls with previous/next buttons and page info.
    
    Args:
        pagination_state: Current pagination state
        on_page_change: Callback function when page changes
        table_id: ID for the table (for styling)
    
    Returns:
        UI element containing pagination controls
    """
    
    def refresh_page_data():
        """Refresh the data when page changes."""
        on_page_change(pagination_state.current_page)
    
    def go_first():
        """Go to first page."""
        pagination_state.go_to_page(1)
        refresh_page_data()
    
    def go_previous():
        """Go to previous page."""
        pagination_state.go_previous()
        refresh_page_data()
    
    def go_next():
        """Go to next page."""
        pagination_state.go_next()
        refresh_page_data()
    
    def go_last():
        """Go to last page."""
        pagination_state.go_to_page(pagination_state.total_pages)
        refresh_page_data()
    
    # Create pagination container
    with ui.element('div').classes('pagination-container').style(
        'display: flex; justify-content: space-between; align-items: center; '
        'margin: 10px 0; padding: 10px; background: #f8f9fa; border-radius: 6px;'
    ) as container:
        
        # Left side: Page info
        page_info = ui.label(pagination_state.get_page_info()).classes(
            'pagination-info has-text-grey'
        ).style('font-size: 0.85rem; font-weight: 500;')
        
        # Right side: Navigation buttons
        with ui.element('div').classes('pagination-controls').style(
            'display: flex; gap: 5px; align-items: center;'
        ):
            
            # First page button
            first_btn = ui.button('', on_click=go_first).classes('is-small').style(
                'min-width: 32px; height: 28px; padding: 0; border-radius: 4px;'
            )
            first_btn.props('flat dense')
            with first_btn:
                ui.icon('first_page').style('font-size: 16px;')
            
            # Previous page button  
            prev_btn = ui.button('', on_click=go_previous).classes('is-small').style(
                'min-width: 32px; height: 28px; padding: 0; border-radius: 4px;'
            )
            prev_btn.props('flat dense')
            with prev_btn:
                ui.icon('chevron_left').style('font-size: 16px;')
            
            # Page indicator
            page_indicator = ui.label(f'{pagination_state.current_page} / {pagination_state.total_pages}').style(
                'margin: 0 10px; font-size: 0.85rem; font-weight: 600; min-width: 60px; text-align: center;'
            )
            
            # Next page button
            next_btn = ui.button('', on_click=go_next).classes('is-small').style(
                'min-width: 32px; height: 28px; padding: 0; border-radius: 4px;'
            )
            next_btn.props('flat dense')
            with next_btn:
                ui.icon('chevron_right').style('font-size: 16px;')
            
            # Last page button
            last_btn = ui.button('', on_click=go_last).classes('is-small').style(
                'min-width: 32px; height: 28px; padding: 0; border-radius: 4px;'
            )
            last_btn.props('flat dense')
            with last_btn:
                ui.icon('last_page').style('font-size: 16px;')
        
        # Function to update pagination display
        def update_pagination_display():
            """Update the pagination controls based on current state."""
            page_info.text = pagination_state.get_page_info()
            page_indicator.text = f'{pagination_state.current_page} / {pagination_state.total_pages}'
            
            # Enable/disable buttons based on current page
            first_btn.enabled = pagination_state.can_go_previous()
            prev_btn.enabled = pagination_state.can_go_previous()
            next_btn.enabled = pagination_state.can_go_next()
            last_btn.enabled = pagination_state.can_go_next()
        
        # Store update function on the container for external access
        container.update_pagination = update_pagination_display
        
        # Initial update
        update_pagination_display()
    
    return container


def create_page_size_selector(
    pagination_state: PaginationState,
    on_page_size_change: Callable[[int], None],
    options: List[int] = [10, 20, 50, 100]
) -> ui.element:
    """
    Create a page size selector dropdown.
    
    Args:
        pagination_state: Current pagination state
        on_page_size_change: Callback when page size changes
        options: List of page size options
    
    Returns:
        UI element containing page size selector
    """
    
    def change_page_size(size: int):
        """Change the page size and refresh data."""
        pagination_state.page_size = size
        pagination_state.current_page = 1  # Reset to first page
        pagination_state.update_total_records(pagination_state.total_records)  # Recalculate pages
        on_page_size_change(size)
    
    with ui.element('div').classes('page-size-selector').style(
        'display: flex; align-items: center; gap: 8px; margin: 5px 0;'
    ) as container:
        
        ui.label('Rows per page:').style('font-size: 0.85rem; color: #666;')
        
        # Create dropdown
        size_select = ui.select(
            options={str(opt): str(opt) for opt in options},
            value=str(pagination_state.page_size),
            on_change=lambda e: change_page_size(int(e.value))
        ).classes('is-small').style('min-width: 80px;')
    
    return container


# Global pagination states for different tables
pagination_states = {}

def get_pagination_state(table_id: str, page_size: int = 20) -> PaginationState:
    """Get or create pagination state for a table."""
    if table_id not in pagination_states:
        pagination_states[table_id] = PaginationState(page_size)
    return pagination_states[table_id]