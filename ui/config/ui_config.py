"""
UI Configuration
Column definitions and grid configurations.
"""


def create_aggrid_config(columns, data, **kwargs):
    """Create standardized AG-Grid configuration."""
    default_config = {
        'columnDefs': columns,
        'rowData': data,
        'defaultColDef': {
            'resizable': True,
            'sortable': True,
            'filter': False,  # Disable filters globally
            'minWidth': 80,
        },
        'theme': 'alpine',
        'pagination': True,
        'paginationPageSize': 50,
    }
    default_config.update(kwargs)
    return default_config


def create_column_def(header_name, field, width=120, **kwargs):
    """Create standardized column definition."""
    column = {
        'headerName': header_name,
        'field': field,
        'sortable': True,
        'filter': False,  # Disable individual column filters
        'width': width
    }
    column.update(kwargs)
    return column


# Column definitions
TRANSACTION_COLUMNS = [
    create_column_def('Account Code', 'account_code', 130),
    create_column_def('Account Name', 'account_name', 200),
    create_column_def('Date', 'transaction_date', 120),
    create_column_def('Booking #', 'booking_number', 120),
    create_column_def('Description', 'description', 200),
    create_column_def('Debit', 'debit_amount', 120, type='numericColumn'),
    create_column_def('Credit', 'credit_amount', 120, type='numericColumn'),
    create_column_def('Net Amount', 'net_amount', 120, type='numericColumn'),
    create_column_def('Balance', 'balance_amount', 120, type='numericColumn'),
    create_column_def('VAT Amount', 'vat_amount', 120, type='numericColumn'),
    create_column_def('Type', 'transaction_type', 100),
    create_column_def('Source File', 'source_file', 150),
]

ACCOUNT_COLUMNS = [
    create_column_def('Account Code', 'account_code', 130),
    create_column_def('Account Name', 'account_name', 250),
    create_column_def('Transactions', 'total_transactions', 120, type='numericColumn'),
    create_column_def('Total Debit', 'total_debit', 150, type='numericColumn'),
    create_column_def('Total Credit', 'total_credit', 150, type='numericColumn'),
    create_column_def('Net Balance', 'net_balance', 150, type='numericColumn'),
    create_column_def('Activity Status', 'activity_status', 120),
    create_column_def('Balance Type', 'account_balance_type', 120),
]