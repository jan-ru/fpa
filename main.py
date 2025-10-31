#!/usr/bin/env python3

import duckdb
import polars as pl
from nicegui import ui
import os

# Load data from DuckDB database
def load_data():
    # Check if database exists, if not create it
    db_path = 'data/financial_data.db'
    if not os.path.exists(db_path):
        print("Database not found. Please run setup_database.py first!")
        return [], 0
    
    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    
    # Query data using DuckDB
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
    
    # Execute query and get results
    result = conn.execute(query).fetchall()
    columns = [desc[0] for desc in conn.description]
    
    # Get total count
    total_count = conn.execute("SELECT COUNT(*) FROM financial_transactions").fetchone()[0]
    
    conn.close()
    
    # Convert to list of dictionaries for AG-Grid
    data = []
    for row in result:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col] = row[i]
        data.append(row_dict)
    
    return data, total_count

data, total_rows = load_data()

# Add a title above the grid
ui.label('Financial Data - AG-Grid').classes('text-h4 text-center mb-4')
ui.label(f'Total rows: {total_rows:,}').classes('text-subtitle1 text-center mb-4')

# Column definitions for AG-Grid based on Excel structure
columns = [
    {'headerName': 'Code Admin', 'field': 'CodeAdministratie', 'sortable': True, 'filter': True, 'width': 140},
    {'headerName': 'Admin Name', 'field': 'NaamAdministratie', 'sortable': True, 'filter': True, 'width': 160},
    {'headerName': 'Account Code', 'field': 'CodeGrootboekrekening', 'sortable': True, 'filter': True, 'width': 130},
    {'headerName': 'Account Name', 'field': 'NaamGrootboekrekening', 'sortable': True, 'filter': True, 'width': 200},
    {'headerName': 'Code', 'field': 'Code', 'sortable': True, 'filter': True, 'width': 100},
    {'headerName': 'Booking #', 'field': 'Boekingsnummer', 'sortable': True, 'filter': True, 'width': 120},
    {'headerName': 'Date', 'field': 'Boekdatum', 'sortable': True, 'filter': True, 'width': 120},
    {'headerName': 'Period', 'field': 'Periode', 'sortable': True, 'filter': True, 'width': 100},
    {'headerName': 'Code1', 'field': 'Code1', 'sortable': True, 'filter': True, 'width': 100},
    {'headerName': 'Code2', 'field': 'Code2', 'sortable': True, 'filter': True, 'width': 100},
    {'headerName': 'Description', 'field': 'Omschrijving', 'sortable': True, 'filter': True, 'width': 200},
    {'headerName': 'Debit', 'field': 'Debet', 'sortable': True, 'filter': True, 'width': 120, 'type': 'numericColumn'},
    {'headerName': 'Credit', 'field': 'Credit', 'sortable': True, 'filter': True, 'width': 120, 'type': 'numericColumn'},
    {'headerName': 'Balance', 'field': 'Saldo', 'sortable': True, 'filter': True, 'width': 120, 'type': 'numericColumn'},
    {'headerName': 'VAT Amount', 'field': 'Btwbedrag', 'sortable': True, 'filter': True, 'width': 120, 'type': 'numericColumn'},
    {'headerName': 'VAT Code', 'field': 'Btwcode', 'sortable': True, 'filter': True, 'width': 100},
    {'headerName': 'Status', 'field': 'Boekingsstatus', 'sortable': True, 'filter': True, 'width': 120},
    {'headerName': 'Number', 'field': 'Nummer', 'sortable': True, 'filter': True, 'width': 100},
    {'headerName': 'Invoice #', 'field': 'Factuurnummer', 'sortable': True, 'filter': True, 'width': 120},
]

# Create the AG-Grid
grid = ui.aggrid({
    'columnDefs': columns,
    'rowData': data,
    'defaultColDef': {
        'resizable': True,
        'sortable': True,
        'filter': True,
        'minWidth': 80,
    },
    'rowSelection': 'multiple',
    'animateRows': True,
    'theme': 'alpine',
    'pagination': True,
    'paginationPageSize': 50,
    'sideBar': True,
}).classes('w-full').style('height: 600px')

# Add database statistics
def get_db_stats():
    conn = duckdb.connect('data/financial_data.db')
    stats = {}
    
    # Total transactions
    stats['total_transactions'] = conn.execute("SELECT COUNT(*) FROM financial_transactions").fetchone()[0]
    
    # Date range
    date_range = conn.execute("SELECT MIN(Boekdatum) as min_date, MAX(Boekdatum) as max_date FROM financial_transactions").fetchone()
    stats['date_range'] = f"{date_range[0]} to {date_range[1]}"
    
    # Total debit and credit
    totals = conn.execute("SELECT SUM(Debet) as total_debit, SUM(Credit) as total_credit FROM financial_transactions").fetchone()
    stats['total_debit'] = f"€{totals[0]:,.2f}" if totals[0] else "€0.00"
    stats['total_credit'] = f"€{totals[1]:,.2f}" if totals[1] else "€0.00"
    
    # Unique accounts
    stats['unique_accounts'] = conn.execute("SELECT COUNT(DISTINCT CodeGrootboekrekening) FROM financial_transactions").fetchone()[0]
    
    conn.close()
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
        conn = duckdb.connect('data/financial_data.db')
        result = conn.execute("SELECT NaamGrootboekrekening, SUM(Debet) as total_debit FROM financial_transactions GROUP BY NaamGrootboekrekening ORDER BY total_debit DESC LIMIT 5").fetchall()
        conn.close()
        summary = "Top 5 accounts by debit:\n" + "\n".join([f"{row[0]}: €{row[1]:,.2f}" for row in result])
        ui.notify(summary)
    
    ui.button('Top Accounts Query', on_click=run_custom_query)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(port=8081)