"""
Table Components for Financial Data Platform
Professional Bulma table creation and management.
"""

from typing import Dict, List
from nicegui import ui


def create_bulma_table(data: List[Dict], columns: List[str], table_id: str = "bulma-table") -> ui.html:
    """Create a professional Bulma table with data."""
    if not data:
        return ui.html('<p class="has-text-grey">No data available</p>', sanitize=False)
    
    # Create table headers with appropriate alignment
    headers_html = ""
    for col in columns:
        display_name = col.replace('_', ' ').title()
        # Right-align headers for numeric columns
        header_class = ""
        if 'amount' in col or 'balance' in col or 'debit' in col or 'credit' in col or 'transactions' in col:
            header_class = 'has-text-right'
        headers_html += f'<th class="{header_class}">{display_name}</th>'
    
    # Create table rows (limit to first 20 for performance)
    rows_html = ""
    for row in data[:20]:
        rows_html += "<tr>"
        for col in columns:
            value = row.get(col, "")
            # Format numeric values
            if isinstance(value, (int, float)) and col not in ['account_code', 'booking_number']:
                if 'amount' in col or 'balance' in col or 'debit' in col or 'credit' in col:
                    value = f"€{value:,.2f}"
                else:
                    value = f"{value:,}"
            
            # Add appropriate CSS classes for different data types
            css_class = ""
            if 'amount' in col or 'balance' in col or 'debit' in col or 'credit' in col or 'transactions' in col:
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
    </style>
    '''
    
    return ui.html(table_html, sanitize=False)