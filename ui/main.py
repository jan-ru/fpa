#!/usr/bin/env python3

import duckdb
import polars as pl
from nicegui import ui
import os

# Database utilities
DB_PATH = '../data/warehouse/financial_data.db'

def get_db_connection():
    """Get database connection"""
    return duckdb.connect(DB_PATH)

def execute_query(query, fetch_all=True):
    """Execute query and return results"""
    with get_db_connection() as conn:
        result = conn.execute(query)
        return result.fetchall() if fetch_all else result.fetchone()

def query_to_dict_list(query):
    """Execute query and convert results to list of dictionaries"""
    with get_db_connection() as conn:
        result = conn.execute(query).fetchall()
        columns = [desc[0] for desc in conn.description]
        return [dict(zip(columns, row)) for row in result]

# Load data from DuckDB database
def load_data():
    """Load all transaction data from database"""
    if not os.path.exists('../data/warehouse/financial_data.db'):
        print("Database not found. Please run setup_database.py first!")
        return [], 0
    
    query = """
    SELECT 
        CodeAdministratie,
        NaamAdministratie,
        CodeGrootboekrekening,
        NaamGrootboekrekening,
        Code,
        Boekingsnummer,
        strftime(Boekdatum, '%Y-%m-%d') as Boekdatum,
        Periode,
        Code1,
        Code2,
        Omschrijving,
        CAST(Debet AS DOUBLE) as Debet,
        CAST(Credit AS DOUBLE) as Credit,
        CAST(Saldo AS DOUBLE) as Saldo,
        CAST(Btwbedrag AS DOUBLE) as Btwbedrag,
        Btwcode,
        Boekingsstatus,
        CAST(Nummer AS DOUBLE) as Nummer,
        Factuurnummer
    FROM financial_transactions
    ORDER BY Boekdatum, Boekingsnummer
    """
    
    data = query_to_dict_list(query)
    total_count = execute_query("SELECT COUNT(*) FROM financial_transactions", fetch_all=False)[0]
    
    return data, total_count

def get_accounts_data():
    """Get aggregated account data"""
    query = """
    SELECT 
        CodeGrootboekrekening,
        NaamGrootboekrekening,
        COUNT(*) as transaction_count,
        SUM(CAST(Debet AS DOUBLE)) as total_debit,
        SUM(CAST(Credit AS DOUBLE)) as total_credit,
        SUM(CAST(Saldo AS DOUBLE)) as total_balance
    FROM financial_transactions
    GROUP BY CodeGrootboekrekening, NaamGrootboekrekening
    ORDER BY CodeGrootboekrekening
    """
    return query_to_dict_list(query)

# Global state for selected filters and grid
selected_years = set()
selected_months = set()
selected_quarters = set()
data, total_rows = load_data()
grid = None  # Will be set later

def build_where_clause(years_filter=None, months_filter=None, quarters_filter=None):
    """Build WHERE clause for date filtering"""
    where_conditions = []
    
    if years_filter and len(years_filter) > 0:
        year_list = ', '.join(map(str, years_filter))
        where_conditions.append(f"EXTRACT(YEAR FROM Boekdatum) IN ({year_list})")
    
    if months_filter and len(months_filter) > 0:
        month_list = ', '.join(map(str, months_filter))
        where_conditions.append(f"EXTRACT(MONTH FROM Boekdatum) IN ({month_list})")
    
    if quarters_filter and len(quarters_filter) > 0:
        quarter_list = ', '.join(map(str, quarters_filter))
        where_conditions.append(f"EXTRACT(QUARTER FROM Boekdatum) IN ({quarter_list})")
    
    return "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

# Functions for filtering
def query_filtered_data(years_filter=None, months_filter=None, quarters_filter=None):
    """Query database with year, month, and quarter filters"""
    where_clause = build_where_clause(years_filter, months_filter, quarters_filter)
    
    query = f"""
    SELECT 
        CodeAdministratie,
        NaamAdministratie,
        CodeGrootboekrekening,
        NaamGrootboekrekening,
        Code,
        Boekingsnummer,
        strftime(Boekdatum, '%Y-%m-%d') as Boekdatum,
        Periode,
        Code1,
        Code2,
        Omschrijving,
        CAST(Debet AS DOUBLE) as Debet,
        CAST(Credit AS DOUBLE) as Credit,
        CAST(Saldo AS DOUBLE) as Saldo,
        CAST(Btwbedrag AS DOUBLE) as Btwbedrag,
        Btwcode,
        Boekingsstatus,
        CAST(Nummer AS DOUBLE) as Nummer,
        Factuurnummer
    FROM financial_transactions
    {where_clause}
    ORDER BY Boekdatum, Boekingsnummer
    """
    
    return query_to_dict_list(query)

# AG-Grid builders
def create_aggrid_config(columns, data, **kwargs):
    """Create standardized AG-Grid configuration"""
    default_config = {
        'columnDefs': columns,
        'rowData': data,
        'defaultColDef': {
            'resizable': True,
            'sortable': True,
            'filter': True,
            'minWidth': 80,
        },
        'theme': 'alpine',
        'pagination': True,
        'paginationPageSize': 50,
    }
    default_config.update(kwargs)
    return default_config

def create_column_def(header_name, field, width=120, **kwargs):
    """Create standardized column definition"""
    column = {
        'headerName': header_name,
        'field': field,
        'sortable': True,
        'filter': True,
        'width': width
    }
    column.update(kwargs)
    return column

# Column definitions
TRANSACTION_COLUMNS = [
    create_column_def('Code Admin', 'CodeAdministratie', 140),
    create_column_def('Admin Name', 'NaamAdministratie', 160),
    create_column_def('Account Code', 'CodeGrootboekrekening', 130),
    create_column_def('Account Name', 'NaamGrootboekrekening', 200),
    create_column_def('Code', 'Code', 100),
    create_column_def('Booking #', 'Boekingsnummer', 120),
    create_column_def('Date', 'Boekdatum', 120),
    create_column_def('Period', 'Periode', 100),
    create_column_def('Code1', 'Code1', 100),
    create_column_def('Code2', 'Code2', 100),
    create_column_def('Description', 'Omschrijving', 200),
    create_column_def('Debit', 'Debet', 120, type='numericColumn'),
    create_column_def('Credit', 'Credit', 120, type='numericColumn'),
    create_column_def('Balance', 'Saldo', 120, type='numericColumn'),
    create_column_def('VAT Amount', 'Btwbedrag', 120, type='numericColumn'),
    create_column_def('VAT Code', 'Btwcode', 100),
    create_column_def('Status', 'Boekingsstatus', 120),
    create_column_def('Number', 'Nummer', 100),
    create_column_def('Invoice #', 'Factuurnummer', 120),
]

# Filter button utilities
def create_filter_buttons(label, items, selected_set, button_list, on_click_fn, **kwargs):
    """Create a row of filter buttons with consistent styling"""
    default_style = 'flex: 1; min-width: 0; height: 32px; font-size: 14px'
    default_classes = 'px-2 py-1'
    
    # Override defaults for specific cases
    style = kwargs.get('style', default_style)
    classes = kwargs.get('classes', default_classes)
    
    with ui.row().classes('gap-4 mb-1 items-center'):
        ui.label(f'{label}:').classes('text-subtitle2').style('min-width: 60px')
        with ui.row().classes('gap-1').style('max-width: 840px'):
            for item in items:
                btn = ui.button(str(item), on_click=lambda x=item: on_click_fn(x)).classes(classes).style(style).props('color=primary')
                button_list.append(btn)

def toggle_selection(value, selected_set, button_list, value_converter=None):
    """Generic toggle function for filter selections"""
    if value is None:
        return
        
    # Convert value if needed (e.g., for months and quarters that use indices)
    actual_value = value_converter(value) if value_converter else value
    
    if actual_value in selected_set:
        selected_set.remove(actual_value)
    else:
        selected_set.add(actual_value)
    
    # Update button colors
    update_button_colors(button_list, selected_set, value_converter)
    update_selection_display()

def update_button_colors(button_list, selected_set, value_converter=None):
    """Update button colors based on selection state"""
    for i, btn in enumerate(button_list):
        # Get the value this button represents
        if value_converter:
            button_value = value_converter(btn.text)
        else:
            button_value = int(btn.text) if btn.text.isdigit() else btn.text
        
        if button_value in selected_set:
            btn.props('color=positive')
        else:
            btn.props('color=primary')

# Specific toggle functions
def toggle_year(year):
    toggle_selection(year, selected_years, year_buttons)

def toggle_month(month_num):
    toggle_selection(month_num, selected_months, month_buttons)

def toggle_quarter(quarter_num):
    toggle_selection(quarter_num, selected_quarters, quarter_buttons)

def update_selection_display():
    """Update the selection display label"""
    parts = []
    if selected_years:
        parts.append(f"Years: {', '.join(map(str, sorted(selected_years)))}")
    if selected_quarters:
        quarters_str = ', '.join([f"Q{q}" for q in sorted(selected_quarters)])
        parts.append(f"Quarters: {quarters_str}")
    if selected_months:
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        months_str = ', '.join([month_names[m-1] for m in sorted(selected_months)])
        parts.append(f"Months: {months_str}")
    
    if parts:
        selection_label.text = f'Selected - {" | ".join(parts)}'
    else:
        selection_label.text = 'No filters selected (showing all data)'

def submit_query():
    """Execute query with selected filters and update grid"""
    if grid is None:
        ui.notify('Grid not ready yet!')
        return
        
    try:
        filtered_data = query_filtered_data(
            selected_years if selected_years else None,
            selected_months if selected_months else None,
            selected_quarters if selected_quarters else None
        )
        
        # Update grid data
        grid.options['rowData'] = filtered_data
        grid.update()
        
        # Update row count
        total_label.text = f'Showing {len(filtered_data):,} rows'
        
        # Create filter summary
        filters = []
        if selected_years:
            filters.append(f"years {', '.join(map(str, sorted(selected_years)))}")
        if selected_quarters:
            filters.append(f"quarters {', '.join([f'Q{q}' for q in sorted(selected_quarters)])}")
        if selected_months:
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            filters.append(f"months {', '.join([month_names[m-1] for m in sorted(selected_months)])}")
        
        if filters:
            ui.notify(f'Filtered to {len(filtered_data):,} records for {" + ".join(filters)}')
        else:
            ui.notify(f'Showing all {len(filtered_data):,} records')
            
    except Exception as e:
        ui.notify(f'Error filtering data: {str(e)}')

# Main navigation tabs
with ui.tabs().classes('w-full') as main_tabs:
    inputs_tab = ui.tab('Inputs')
    data_tab = ui.tab('Data')
    outputs_tab = ui.tab('Outputs')

with ui.tab_panels(main_tabs, value=data_tab).classes('w-full'):
    # Inputs screen
    with ui.tab_panel(inputs_tab):
        ui.label('Data Inputs').classes('text-h4 mb-4')
        ui.label('Import and manage financial data files').classes('text-subtitle1 mb-4')
        
        with ui.card().classes('w-full p-4'):
            ui.label('Excel File Import').classes('text-h6 mb-2')
            ui.label('Current data files in database:').classes('mb-2')
            with ui.column().classes('gap-2'):
                ui.label('• DUMP2021_24feb25.xlsx - 12,066 records')
                ui.label('• DUMP2022_24feb25.xlsx - 14,155 records') 
                ui.label('• DUMP2023_24feb25.xlsx - 15,856 records')
                ui.label('• DUMP2024_18mrt25.xlsx - Skipped (column mismatch)')
            
            ui.separator().classes('my-4')
            ui.label('Import new Excel files:').classes('mb-2')
            ui.button('Select Excel File', on_click=lambda: ui.notify('File import feature - coming soon!')).classes('mb-2')
            ui.label('Supported formats: .xlsx files with 19-column financial data structure').classes('text-caption')

    # Data screen with sub-tabs
    with ui.tab_panel(data_tab):
        ui.label('Financial Data Analysis').classes('text-h4 text-center mb-4')
        
        # Sub-tabs for accounts and transactions
        with ui.tabs().classes('w-full') as data_tabs:
            accounts_tab = ui.tab('Accounts')
            transactions_tab = ui.tab('Transactions')
        
        with ui.tab_panels(data_tabs, value=transactions_tab).classes('w-full'):
            # Accounts tab
            with ui.tab_panel(accounts_tab):
                accounts_label = ui.label('Account Summary').classes('text-h5 mb-4')
                
                # Create accounts grid
                accounts_data = get_accounts_data()
                
                # Define account columns with currency formatting
                currency_formatter = {'function': 'params.value.toLocaleString("en-US", {style: "currency", currency: "EUR"})'}
                accounts_columns = [
                    create_column_def('Account Code', 'CodeGrootboekrekening', 130),
                    create_column_def('Account Name', 'NaamGrootboekrekening', 300),
                    create_column_def('Transactions', 'transaction_count', 120, type='numericColumn'),
                    create_column_def('Total Debit', 'total_debit', 150, type='numericColumn', valueFormatter=currency_formatter),
                    create_column_def('Total Credit', 'total_credit', 150, type='numericColumn', valueFormatter=currency_formatter),
                    create_column_def('Balance', 'total_balance', 150, type='numericColumn', valueFormatter=currency_formatter),
                ]
                
                accounts_config = create_aggrid_config(accounts_columns, accounts_data, paginationPageSize=25)
                ui.aggrid(accounts_config).classes('w-full').style('height: 600px')
            
            # Transactions tab
            with ui.tab_panel(transactions_tab):
                total_label = ui.label(f'Total transactions: {total_rows:,}').classes('text-subtitle1 mb-4')
                
                # Create filter buttons using reusable function
                years = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
                year_buttons = []
                create_filter_buttons('Years', years, selected_years, year_buttons, toggle_year)

                quarters = ['Q1', 'Q2', 'Q3', 'Q4']
                quarter_buttons = []
                create_filter_buttons('Quarters', quarters, selected_quarters, quarter_buttons, lambda q: toggle_quarter(['Q1', 'Q2', 'Q3', 'Q4'].index(q) + 1))

                months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                month_buttons = []
                month_style = 'flex: 1; min-width: 0; height: 32px; font-size: 12px'
                month_classes = 'px-1 py-1'
                create_filter_buttons('Months', months, selected_months, month_buttons, 
                                    lambda m: toggle_month(months.index(m) + 1), 
                                    style=month_style, classes=month_classes)

                # Selection status and submit button
                selection_label = ui.label('No filters selected (showing all data)').classes('mb-2')
                with ui.row().classes('gap-4 mb-4'):
                    ui.button('Submit Query', on_click=submit_query).classes('px-6 py-2').props('color=positive').style('height: 36px')
                    def clear_selection():
                        selected_years.clear()
                        selected_months.clear()
                        selected_quarters.clear()
                        # Update all button colors to primary (blue)
                        for btn in year_buttons:
                            btn.props('color=primary')
                        for btn in month_buttons:
                            btn.props('color=primary')
                        for btn in quarter_buttons:
                            btn.props('color=primary')
                        update_selection_display()
                    
                    ui.button('Clear All', on_click=clear_selection).classes('px-4 py-2').style('height: 36px')
                
                # Create the transaction AG-Grid using reusable configuration
                transaction_config = create_aggrid_config(
                    TRANSACTION_COLUMNS, 
                    data, 
                    rowSelection='multiple',
                    animateRows=True,
                    sideBar=True
                )
                
                # Create the AG-Grid and store in global variable
                grid = ui.aggrid(transaction_config).classes('w-full').style('height: 600px')

    # Outputs screen
    with ui.tab_panel(outputs_tab):
        ui.label('Data Outputs').classes('text-h4 mb-4')
        ui.label('Export and visualize financial data').classes('text-subtitle1 mb-4')
        
        with ui.row().classes('gap-4 w-full'):
            # Export options
            with ui.card().classes('flex-1 p-4'):
                ui.label('Export Data').classes('text-h6 mb-2')
                ui.label('Export filtered data in various formats:').classes('mb-4')
                
                with ui.column().classes('gap-2'):
                    ui.button('Export to CSV', on_click=lambda: ui.notify('CSV export - coming soon!')).classes('w-full')
                    ui.button('Export to Excel', on_click=lambda: ui.notify('Excel export - coming soon!')).classes('w-full')
                    ui.button('Export to PDF Report', on_click=lambda: ui.notify('PDF export - coming soon!')).classes('w-full')
            
            # Visualizations
            with ui.card().classes('flex-1 p-4'):
                ui.label('Visualizations').classes('text-h6 mb-2')
                ui.label('Generate charts and reports:').classes('mb-4')
                
                with ui.column().classes('gap-2'):
                    ui.button('Account Balance Chart', on_click=lambda: ui.notify('Chart generation - coming soon!')).classes('w-full')
                    ui.button('Monthly Trends', on_click=lambda: ui.notify('Trend analysis - coming soon!')).classes('w-full')
                    ui.button('Custom Report', on_click=lambda: ui.notify('Custom reports - coming soon!')).classes('w-full')
        
        # Database statistics
        with ui.card().classes('w-full p-4 mt-4'):
            ui.label('Database Statistics').classes('text-h6 mb-2')
            
            with ui.row().classes('gap-8'):
                with ui.column():
                    ui.label('Data Overview:').classes('font-bold mb-2')
                    ui.label(f'Total Records: {total_rows:,}')
                    ui.label('Date Range: 2020-2024')
                    ui.label('Accounts: Multiple')
                
                with ui.column():
                    ui.label('Last Updated:').classes('font-bold mb-2')
                    ui.label('Database: Recently')
                    ui.label('Source Files: 3 Excel files')
                    ui.label('Status: Ready')

# Add database statistics
def get_db_stats():
    """Get database statistics using reusable query functions"""
    stats = {}
    
    # Total transactions
    stats['total_transactions'] = execute_query("SELECT COUNT(*) FROM financial_transactions", fetch_all=False)[0]
    
    # Date range
    date_range = execute_query("SELECT MIN(Boekdatum) as min_date, MAX(Boekdatum) as max_date FROM financial_transactions", fetch_all=False)
    stats['date_range'] = f"{date_range[0]} to {date_range[1]}"
    
    # Total debit and credit
    totals = execute_query("SELECT SUM(Debet) as total_debit, SUM(Credit) as total_credit FROM financial_transactions", fetch_all=False)
    stats['total_debit'] = f"€{totals[0]:,.2f}" if totals[0] else "€0.00"
    stats['total_credit'] = f"€{totals[1]:,.2f}" if totals[1] else "€0.00"
    
    # Unique accounts
    stats['unique_accounts'] = execute_query("SELECT COUNT(DISTINCT CodeGrootboekrekening) FROM financial_transactions", fetch_all=False)[0]
    
    return stats

# Display statistics
with ui.expansion('Database Statistics', icon='analytics').classes('w-full mb-4'):
    stats = get_db_stats()
    with ui.grid(columns=2).classes('w-full gap-4'):
        ui.card().classes('p-4').add_slot('default', f"Total Transactions: {stats['total_transactions']:,}")
        ui.card().classes('p-4').add_slot('default', f"Date Range: {stats['date_range']}")
        ui.card().classes('p-4').add_slot('default', f"Total Debit: {stats['total_debit']}")
        ui.card().classes('p-4').add_slot('default', f"Total Credit: {stats['total_credit']}")
        ui.card().classes('p-4').add_slot('default', f"Unique Accounts: {stats['unique_accounts']}")
        ui.card().classes('p-4').add_slot('default', "Powered by DuckDB + Polars")

# Add some interactive buttons below the grid
with ui.row().classes('mt-4 gap-4'):
    ui.button('Get Selected', on_click=lambda: ui.notify(f'Selected {len(grid.selected_rows)} rows'))
    ui.button('Clear Selection', on_click=lambda: grid.clear_selection())
    
    def run_custom_query():
        result = execute_query("SELECT NaamGrootboekrekening, SUM(Debet) as total_debit FROM financial_transactions GROUP BY NaamGrootboekrekening ORDER BY total_debit DESC LIMIT 5")
        summary = "Top 5 accounts by debit:\n" + "\n".join([f"{row[0]}: €{row[1]:,.2f}" for row in result])
        ui.notify(summary)
    
    ui.button('Top Accounts Query', on_click=run_custom_query)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(port=8081)