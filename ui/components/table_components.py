"""
Table Components for Financial Data Platform
Professional Bulma table creation and management.
"""

from typing import Dict, List, Callable, Optional
from decimal import Decimal
from nicegui import ui
from .pagination import PaginationState, create_pagination_controls, get_pagination_state


def create_bulma_table(data: List[Dict], columns: List[str], table_id: str = "bulma-table", show_selection: bool = False) -> ui.html:
    """Create a professional Bulma table with data."""
    if not data:
        return ui.html('<p class="has-text-grey">No data available</p>', sanitize=False)
    
    # Create table headers with appropriate alignment
    headers_html = ""
    
    # Add selection header if needed
    if show_selection:
        headers_html += f'<th class="has-text-centered" style="width: 50px;">Select</th>'
    
    for col in columns:
        display_name = col.replace('_', ' ').title()
        # Right-align headers for numeric columns
        header_class = ""
        if ('amount' in col or 'balance' in col or 'debit' in col or 'credit' in col or 
            'transactions' in col or 'records_' in col or 'size_' in col or col.endswith('_mb')):
            header_class = 'has-text-right'
        headers_html += f'<th class="{header_class}">{display_name}</th>'
    
    # Create table rows (limit to first 20 for performance)
    rows_html = ""
    for i, row in enumerate(data[:20]):
        rows_html += "<tr>"
        
        # Add selection checkbox if needed
        if show_selection:
            primary_key = row.get('filename', row.get('account_code', row.get('snapshot_id', i)))
            rows_html += f'<td class="has-text-centered"><input type="checkbox" class="row-selector" data-key="{primary_key}"></td>'
        
        for col in columns:
            value = row.get(col, "")
            # Format numeric values (now with consistent data types)
            if col not in ['account_code', 'booking_number']:
                if isinstance(value, (int, float, Decimal)):
                    # Special handling: total_debit, total_credit, and net_balance don't get EUR sign
                    if col in ['total_debit', 'total_credit', 'net_balance']:
                        value = f"{value:,.2f}"
                    elif ('amount' in col or 'balance' in col or 'debit' in col or 'credit' in col):
                        value = f"€{value:,.2f}"
                    else:
                        value = f"{value:,}"
            
            # Handle status column with icons
            if col == 'status':
                if value == 'Processed':
                    value = '<span style="color: #28a745;"><i class="fas fa-check-circle"></i> Processed</span>'
                elif value == 'Skipped':
                    value = '<span style="color: #ffc107;"><i class="fas fa-exclamation-triangle"></i> Skipped</span>'
            
            # Add appropriate CSS classes for different data types
            css_class = ""
            if ('amount' in col or 'balance' in col or 'debit' in col or 'credit' in col or 
                'transactions' in col or 'records_' in col or 'size_' in col or col.endswith('_mb')):
                css_class = 'has-text-right'
            elif col in ['account_code', 'booking_number']:
                css_class = 'has-text-weight-semibold'
                
            rows_html += f'<td class="{css_class}">{value}</td>'
        rows_html += "</tr>"
    
    table_html = f'''
    <div class="table-container">
        <table id="{table_id}" class="table is-striped is-hoverable is-fullwidth">
            <thead class="has-background-primary-light">
                <tr>{headers_html}</tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    <style>
        .table-container {{
            border-radius: 6px;
            overflow: hidden;
            border: 1px solid #dbdbdb;
        }}
        .table {{
            font-size: 0.8rem;
        }}
        .table thead th {{
            border-bottom: 2px solid #3273dc;
            font-weight: 600;
            font-size: 0.75rem;
            padding: 0.5em 0.75em;
        }}
        .table tbody td {{
            padding: 0.4em 0.75em;
            font-size: 0.8rem;
        }}
        .table tbody tr:hover {{
            background-color: #f5f5f5;
        }}
        input[type="checkbox"] {{
            transform: scale(1.1);
        }}
    </style>
    '''
    
    return ui.html(table_html, sanitize=False)


def create_paginated_table(
    data_func: Callable[[int, int], tuple[List[Dict], int]],  # Returns (data, total_count)
    columns: List[str],
    table_id: str = "paginated-table",
    show_selection: bool = False,
    page_size: int = 20,
    title: str = "",
    subtitle: str = ""
) -> ui.card:
    """
    Create a paginated table with navigation controls.
    
    Args:
        data_func: Function that takes (offset, limit) and returns (data, total_count)
        columns: List of column names to display
        table_id: Unique identifier for the table
        show_selection: Whether to show selection checkboxes
        page_size: Number of rows per page
        title: Table title
        subtitle: Table subtitle
    
    Returns:
        Card containing the paginated table
    """
    
    # Get pagination state for this table
    pagination_state = get_pagination_state(table_id, page_size)
    
    # Create card container
    with ui.card().classes('w-full') as card:
        
        # Title and subtitle
        if title:
            ui.label(title).classes('text-h6 q-mb-xs')
        if subtitle:
            ui.label(subtitle).classes('text-caption text-grey q-mb-md')
        
        # Container for table content
        table_container = ui.element('div').classes('table-container-wrapper')
        
        # Container for pagination controls
        pagination_container = ui.element('div').classes('pagination-wrapper')
        pagination_controls = None
        
        def load_page_data(page_number: Optional[int] = None):
            """Load data for the current page."""
            if page_number is not None:
                pagination_state.current_page = page_number
            
            offset = pagination_state.get_offset()
            limit = pagination_state.get_limit()
            
            try:
                # Get data and total count
                page_data, total_count = data_func(offset, limit)
                
                # Update pagination state
                pagination_state.update_total_records(total_count)
                
                # Clear and rebuild table
                table_container.clear()
                
                with table_container:
                    if page_data:
                        create_bulma_table(page_data, columns, table_id, show_selection)
                    else:
                        ui.label('No data available').classes('text-grey text-center q-pa-md')
                
                # Update pagination controls if they exist
                if pagination_controls and hasattr(pagination_controls, 'update_pagination'):
                    # Update existing pagination display
                    pagination_controls.update_pagination()
                
            except Exception as e:
                table_container.clear()
                with table_container:
                    ui.label(f'Error loading data: {str(e)}').classes('text-negative text-center q-pa-md')
        
        # Initial pagination controls
        with pagination_container:
            pagination_controls = create_pagination_controls(
                pagination_state,
                load_page_data,
                table_id
            )
        
        # Load initial data
        load_page_data()
    
    return card


def create_data_count_badge(count: int, total: int = None) -> ui.element:
    """Create a badge showing data count."""
    
    if total is not None and total > count:
        text = f"{count:,} of {total:,}"
        color = "orange"
    else:
        text = f"{count:,}"
        color = "blue"
    
    return ui.badge(text).style(f'background-color: {color}; color: white; font-size: 0.75rem;')