"""
Table Components for Financial Data Platform
Professional Bulma table creation and management.
"""

from typing import Dict, List
from decimal import Decimal
from nicegui import ui


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
                    if ('amount' in col or 'balance' in col or 'debit' in col or 'credit' in col or 
                        col in ['total_debit', 'total_credit', 'net_balance']):
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