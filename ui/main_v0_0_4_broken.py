#!/usr/bin/env python3
"""
Financial Data Platform v0.0.4
Proof-of-concept with Bulma CSS + UI5 Web Components integration.
"""

import os
import subprocess
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import yaml

import polars as pl
from nicegui import ui

# Import plotly for analytics
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False

# Import our data access layer
from data_access import data_access

# Global state for filters and components
selected_years = set()
selected_months = set()
selected_quarters = set()
grid = None
accounts_grid = None
stats_cards = {}

# Version information
APP_VERSION = "0.0.4"
APP_NAME = "FPA"

def setup_css_and_js():
    """Setup Bulma CSS and UI5 Web Components."""
    # Add Bulma CSS
    ui.add_head_html('''
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.9.4/css/bulma.min.css">
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    ''')
    
    # Add UI5 Web Components
    ui.add_head_html('''
        <script type="module">
            import { setTheme } from "https://sap.github.io/ui5-webcomponents/assets/js/ui5-webcomponents/dist/Assets.js";
            setTheme("sap_fiori_3");
        </script>
        <script type="module" src="https://sap.github.io/ui5-webcomponents/assets/js/ui5-webcomponents/dist/Table.js"></script>
        <script type="module" src="https://sap.github.io/ui5-webcomponents/assets/js/ui5-webcomponents/dist/TableColumn.js"></script>
        <script type="module" src="https://sap.github.io/ui5-webcomponents/assets/js/ui5-webcomponents/dist/TableRow.js"></script>
        <script type="module" src="https://sap.github.io/ui5-webcomponents/assets/js/ui5-webcomponents/dist/TableCell.js"></script>
        <script type="module" src="https://sap.github.io/ui5-webcomponents/assets/js/ui5-webcomponents/dist/Card.js"></script>
        <script type="module" src="https://sap.github.io/ui5-webcomponents/assets/js/ui5-webcomponents/dist/Button.js"></script>
        <script type="module" src="https://sap.github.io/ui5-webcomponents/assets/js/ui5-webcomponents/dist/Panel.js"></script>
        <script type="module" src="https://sap.github.io/ui5-webcomponents/assets/js/ui5-webcomponents/dist/Input.js"></script>
    ''')
    
    # Custom CSS for integration
    ui.add_head_html('''
        <style>
            /* Override NiceGUI defaults with Bulma styling */
            .nicegui-content {
                background: #f5f5f5;
            }
            
            /* Custom Bulma-UI5 integration styles */
            .stat-card {
                background: white;
                border-radius: 8px;
                padding: 1.5rem;
                box-shadow: 0 2px 3px rgba(10, 10, 10, 0.1);
                border: 1px solid #dbdbdb;
            }
            
            .ui5-table-container {
                background: white;
                border-radius: 8px;
                padding: 1rem;
                box-shadow: 0 2px 3px rgba(10, 10, 10, 0.1);
            }
            
            /* SAP Fiori color scheme */
            .has-sap-blue { background-color: #0a6ed1 !important; }
            .has-sap-green { background-color: #107e3e !important; }
            .has-sap-orange { background-color: #d04343 !important; }
            .has-sap-grey { background-color: #6a6d70 !important; }
        </style>
    ''')

def get_dbt_version() -> str:
    """Get dbt project version from dbt_project.yml."""
    try:
        dbt_project_path = Path("../dbt_project/dbt_project.yml")
        if dbt_project_path.exists():
            with open(dbt_project_path, 'r') as f:
                dbt_config = yaml.safe_load(f)
                return f"v{dbt_config.get('version', '1.0.0')}"
    except Exception:
        pass
    return "v1.0.0"

def get_data_timestamp() -> str:
    """Get latest data timestamp from available Iceberg data."""
    try:
        # Look for the most recent parquet file in the iceberg warehouse
        iceberg_dir = Path("../pipelines/data/iceberg/warehouse")
        if iceberg_dir.exists():
            parquet_files = list(iceberg_dir.glob("financial_transactions_*.parquet"))
            if parquet_files:
                # Get the most recent file
                latest_file = max(parquet_files, key=lambda x: x.stat().st_mtime)
                # Extract full timestamp from filename
                filename = latest_file.stem
                if "_" in filename:
                    # Split and get the last two parts (date_time)
                    parts = filename.split("_")
                    if len(parts) >= 2:
                        # Get the last two parts: date and time
                        date_part = parts[-2]  # 20251101
                        time_part = parts[-1]  # 091325
                        return f"v{date_part}_{time_part}"
    except Exception:
        pass
    return "v20251101_091324"

def create_version_footer() -> ui.html:
    """Create footer with comprehensive version information."""
    dbt_version = get_dbt_version()
    data_timestamp = get_data_timestamp()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    
    footer_html = f"""
    <div class="footer has-background-light">
        <div class="content has-text-centered">
            <p class="is-size-7 has-text-grey">
                App: v{APP_VERSION} | Data: {data_timestamp} | Queries: {dbt_version} | {current_time}
            </p>
        </div>
    </div>
    """
    
    return ui.html(footer_html, sanitize=False)

def get_excel_files() -> List[Dict]:
    """Get list of Excel files in the data directory with detailed information."""
    data_dir = Path("../data/raw")
    excel_files = []
    
    if data_dir.exists():
        for file_path in data_dir.glob("*.xlsx"):
            stat = file_path.stat()
            
            # Get Excel file dimensions using polars
            try:
                # Read just the first few rows to get dimensions efficiently
                df_sample = pl.read_excel(file_path, read_options={"n_rows": 1})
                n_columns = len(df_sample.columns)
                
                # For row count, we need to read the full file (but efficiently)
                df_full = pl.read_excel(file_path)
                n_rows = len(df_full)
                
            except Exception as e:
                # If we can't read the file, set defaults
                n_columns = 0
                n_rows = 0
            
            excel_files.append({
                "filename": file_path.name,
                "path": str(file_path),
                "size_mb": round(stat.st_size / (1024*1024), 2),
                "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                "n_columns": n_columns,
                "n_rows": n_rows,
                "status": "Skipped" if 'DUMP2024' in file_path.name else "Processed"
            })
    
    return sorted(excel_files, key=lambda x: x["modified"], reverse=True)

def create_ui5_table(data: List[Dict], columns: List[str]) -> ui.html:
    """Create a UI5 table with data."""
    if not data:
        return ui.html('<p class="has-text-grey">No data available</p>', sanitize=False)
    
    # Create table headers
    headers_html = ""
    for col in columns:
        headers_html += f'<ui5-table-column slot="columns"><span>{col}</span></ui5-table-column>'
    
    # Create table rows
    rows_html = ""
    for row in data[:10]:  # Limit to first 10 rows for demo
        row_html = "<ui5-table-row>"
        for col in columns:
            value = row.get(col, "")
            # Format numeric values
            if isinstance(value, (int, float)) and col.lower() not in ['n_columns', 'n_rows']:
                if abs(value) > 1000:
                    value = f"{value:,.2f}"
                else:
                    value = f"{value:.2f}"
            row_html += f"<ui5-table-cell>{value}</ui5-table-cell>"
        row_html += "</ui5-table-row>"
        rows_html += row_html
    
    table_html = f"""
    <div class="ui5-table-container">
        <ui5-table>
            {headers_html}
            {rows_html}
        </ui5-table>
    </div>
    """
    
    return ui.html(table_html, sanitize=False)

def get_filtered_stats() -> Dict:
    """Get statistics for currently filtered data."""
    if not any([selected_years, selected_months, selected_quarters]):
        # No filters, return overall stats
        return data_access.get_dashboard_stats()
    
    # Get filtered transactions
    filtered_transactions = data_access.get_filtered_transactions(
        years=list(selected_years) if selected_years else None,
        quarters=list(selected_quarters) if selected_quarters else None,
        months=list(selected_months) if selected_months else None,
        limit=50000  # High limit to get all for stats
    )
    
    if not filtered_transactions:
        return {
            "accounts": {"total": 0, "active": 0, "assets": 0, "liabilities": 0},
            "transactions": {"total_transactions": 0, "unique_accounts": 0, 
                           "total_debit": 0, "total_credit": 0, "net_total": 0}
        }
    
    # Calculate filtered stats
    df = pl.DataFrame(filtered_transactions)
    unique_accounts = df["account_code"].n_unique()
    total_transactions = len(df)
    total_debit = df["debit_amount"].sum()
    total_credit = df["credit_amount"].sum()
    net_total = df["net_amount"].sum()
    
    # Get unique accounts with their balances
    account_balances = df.group_by("account_code").agg([
        pl.col("net_amount").sum().alias("balance")
    ])
    
    total_assets = account_balances.filter(pl.col("balance") > 0)["balance"].sum() or 0
    total_liabilities = abs(account_balances.filter(pl.col("balance") < 0)["balance"].sum() or 0)
    
    return {
        "accounts": {
            "total": unique_accounts,
            "active": unique_accounts,  # All filtered accounts considered active
            "assets": total_assets,
            "liabilities": total_liabilities
        },
        "transactions": {
            "total_transactions": total_transactions,
            "unique_accounts": unique_accounts,
            "total_debit": total_debit,
            "total_credit": total_credit,
            "net_total": net_total
        }
    }

def update_dashboard_stats():
    """Update dashboard statistics cards."""
    stats = get_filtered_stats()
    
    if "total_accounts_card" in stats_cards:
        stats_cards["total_accounts_card"].text = f"Total Accounts: {stats['accounts']['total']:,}"
    if "active_accounts_card" in stats_cards:
        stats_cards["active_accounts_card"].text = f"Active Accounts: {stats['accounts']['active']:,}"
    if "total_assets_card" in stats_cards:
        stats_cards["total_assets_card"].text = f"Total Assets: €{stats['accounts']['assets']:,.2f}"
    if "total_liabilities_card" in stats_cards:
        stats_cards["total_liabilities_card"].text = f"Total Liabilities: €{stats['accounts']['liabilities']:,.2f}"
    if "total_transactions_card" in stats_cards:
        stats_cards["total_transactions_card"].text = f"Total Transactions: {stats['transactions']['total_transactions']:,}"
    if "total_debit_card" in stats_cards:
        stats_cards["total_debit_card"].text = f"Total Debit: €{stats['transactions']['total_debit']:,.2f}"

def create_filter_buttons(items, selected_set, button_list, on_click_fn, style_class=""):
    """Create a row of filter buttons using Bulma styling."""
    with ui.row().classes('field is-grouped is-grouped-multiline'):
        for item in items:
            with ui.column().classes('control'):
                btn = ui.button(str(item), on_click=lambda x=item: on_click_fn(x)).classes(f'button is-small {style_class}')
                button_list.append(btn)

def toggle_selection(value, selected_set, button_list, value_converter=None):
    """Generic toggle function for filter selections."""
    if value is None:
        return
        
    actual_value = value_converter(value) if value_converter else value
    
    if actual_value in selected_set:
        selected_set.remove(actual_value)
    else:
        selected_set.add(actual_value)
    
    update_button_colors(button_list, selected_set, value_converter)
    update_grids_and_stats()

def update_button_colors(button_list, selected_set, value_converter=None):
    """Update button colors based on selection state."""
    for btn in button_list:
        if value_converter:
            button_value = value_converter(btn.text)
        else:
            button_value = int(btn.text) if btn.text.isdigit() else btn.text
        
        if button_value in selected_set:
            btn.classes = 'button is-small is-primary'
        else:
            btn.classes = 'button is-small'

def toggle_year(year):
    toggle_selection(year, selected_years, year_buttons)

def toggle_month(month_name):
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    month_num = month_names.index(month_name) + 1
    toggle_selection(month_num, selected_months, month_buttons)

def toggle_quarter(quarter_name):
    quarter_num = int(quarter_name[1])  # Extract number from "Q1", "Q2", etc.
    toggle_selection(quarter_num, selected_quarters, quarter_buttons)

def update_grids_and_stats():
    """Update both grids and dashboard statistics when filters change."""
    try:
        update_dashboard_stats()
        ui.notify('Filters updated successfully', type='positive')
    except Exception as e:
        ui.notify(f'Error filtering data: {str(e)}', type='negative')

def clear_all_filters():
    """Clear all filter selections."""
    selected_years.clear()
    selected_months.clear()
    selected_quarters.clear()
    
    # Update all button colors
    for btn in year_buttons + month_buttons + quarter_buttons:
        btn.classes = 'button is-small'
    
    update_grids_and_stats()

def create_plotly_sample():
    """Create sample Plotly charts for analytics page."""
    if not PLOTLY_AVAILABLE:
        return ui.label("Plotly not available. Install with: uv add plotly").classes('has-text-grey')
    
    # Get some sample data for charts
    try:
        monthly_data = data_access.get_monthly_trends(12)
        
        if monthly_data:
            # Convert to format suitable for plotting
            months = [f"{d['transaction_year']}-{d['transaction_month']:02d}" for d in monthly_data]
            debits = [d['total_debit'] for d in monthly_data]
            credits = [d['total_credit'] for d in monthly_data]
            
            # Create subplots
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Monthly Debit/Credit Trends', 'Transaction Volume', 
                              'Net Cash Flow', 'Account Activity'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"type": "pie"}]]
            )
            
            # Monthly trends
            fig.add_trace(
                go.Scatter(x=months, y=debits, name='Debits', line=dict(color='#d04343')),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=months, y=credits, name='Credits', line=dict(color='#107e3e')),
                row=1, col=1
            )
            
            # Transaction volume
            volumes = [d['transaction_count'] for d in monthly_data]
            fig.add_trace(
                go.Bar(x=months, y=volumes, name='Transactions', marker_color='#0a6ed1'),
                row=1, col=2
            )
            
            # Net cash flow
            net_flows = [d['net_amount'] for d in monthly_data]
            fig.add_trace(
                go.Bar(x=months, y=net_flows, name='Net Flow', 
                      marker_color=['#107e3e' if x > 0 else '#d04343' for x in net_flows]),
                row=2, col=1
            )
            
            # Pie chart for account activity
            account_summary = data_access.get_account_activity_breakdown()
            if account_summary:
                labels = [item['activity_status'] for item in account_summary]
                values = [item['account_count'] for item in account_summary]
                fig.add_trace(
                    go.Pie(labels=labels, values=values, name="Account Activity"),
                    row=2, col=2
                )
            
            fig.update_layout(height=600, showlegend=True, title_text="Financial Analytics Dashboard")
            
            return ui.plotly(fig)
        else:
            return ui.label("No data available for charts").classes('has-text-grey')
            
    except Exception as e:
        return ui.label(f"Error creating charts: {str(e)}").classes('has-text-danger')

# Initialize data and button lists
year_buttons = []
month_buttons = []
quarter_buttons = []

# Setup CSS and JS
setup_css_and_js()

# Create the main application
ui.page_title('Financial Data Platform v0.0.4 - Bulma + UI5')

# Hero section with Bulma styling
with ui.element('div').classes('hero is-primary is-small'):
    with ui.element('div').classes('hero-body'):
        with ui.element('div').classes('container'):
            ui.label('Financial Planning & Analysis').classes('title is-3 has-text-white')
            ui.label('Powered by Bulma + UI5 Web Components').classes('subtitle is-5 has-text-white')

# Main navigation tabs with NiceGUI
with ui.tabs().classes('w-full') as main_tabs:
    inputs_tab = ui.tab('Inputs')
    dashboard_tab = ui.tab('Dashboard')
    analytics_tab = ui.tab('Analytics')
    lineage_tab = ui.tab('Lineage')
    admin_tab = ui.tab('Admin')

with ui.tab_panels(main_tabs, value=dashboard_tab).classes('w-full').style('padding-bottom: 50px;'):
    
    # INPUTS TAB
    with ui.tab_panel(inputs_tab):
        ui.label('Data Inputs').classes('title is-4')
        ui.label('Excel file inventory and processing status').classes('subtitle is-6 has-text-grey')
        
        with ui.element('div').classes('box'):
            ui.label('Excel Files in Data Directory').classes('title is-5 mb-4')
            
            # Get and display Excel files with UI5 table
            excel_files = get_excel_files()
            
            if excel_files:
                columns = ['filename', 'size_mb', 'n_columns', 'n_rows', 'modified', 'status']
                create_ui5_table(excel_files, columns)
            else:
                ui.label('No Excel files found in ../data/raw directory').classes('has-text-grey')
            
            ui.html('<hr class="my-4">', sanitize=False)
            ui.label('File Processing Status:').classes('title is-6')
            with ui.element('div').classes('content'):
                ui.html('<p class="has-text-success"><i class="fas fa-check"></i> Processed files have been imported into the data warehouse</p>', sanitize=False)
                ui.html('<p class="has-text-warning"><i class="fas fa-exclamation-triangle"></i> Skipped files have format incompatibilities and require manual review</p>', sanitize=False)

    # DASHBOARD TAB
    with ui.element('div').classes('content') as dashboard_content:
        ui.label('Financial Data Dashboard').classes('title is-4 has-text-centered mb-5')
        
        # Dashboard Statistics Cards using Bulma
        with ui.element('div').classes('columns is-multiline mb-5'):
            with ui.element('div').classes('column is-4'):
                with ui.element('div').classes('stat-card has-text-centered'):
                    stats_cards["total_accounts_card"] = ui.label('Total Accounts: Loading...').classes('title is-5 has-sap-blue has-text-white p-3')
            with ui.element('div').classes('column is-4'):
                with ui.element('div').classes('stat-card has-text-centered'):
                    stats_cards["active_accounts_card"] = ui.label('Active Accounts: Loading...').classes('title is-5 has-sap-green has-text-white p-3')
            with ui.element('div').classes('column is-4'):
                with ui.element('div').classes('stat-card has-text-centered'):
                    stats_cards["total_assets_card"] = ui.label('Total Assets: Loading...').classes('title is-5 has-sap-orange has-text-white p-3')
            with ui.element('div').classes('column is-4'):
                with ui.element('div').classes('stat-card has-text-centered'):
                    stats_cards["total_liabilities_card"] = ui.label('Total Liabilities: Loading...').classes('title is-5 has-sap-grey has-text-white p-3')
            with ui.element('div').classes('column is-4'):
                with ui.element('div').classes('stat-card has-text-centered'):
                    stats_cards["total_transactions_card"] = ui.label('Total Transactions: Loading...').classes('title is-5 has-sap-blue has-text-white p-3')
            with ui.element('div').classes('column is-4'):
                with ui.element('div').classes('stat-card has-text-centered'):
                    stats_cards["total_debit_card"] = ui.label('Total Debit: Loading...').classes('title is-5 has-sap-green has-text-white p-3')
        
        # Filters using Bulma styling
        with ui.element('div').classes('box'):
            ui.label('Filter Data').classes('title is-5 mb-4')
            
            # Years filter
            ui.label('Years').classes('label')
            years = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
            create_filter_buttons(years, selected_years, year_buttons, toggle_year)
            
            # Quarters filter
            ui.label('Quarters').classes('label mt-4')
            quarters = ['Q1', 'Q2', 'Q3', 'Q4']
            create_filter_buttons(quarters, selected_quarters, quarter_buttons, toggle_quarter)
            
            # Months filter
            ui.label('Months').classes('label mt-4')
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            create_filter_buttons(months, selected_months, month_buttons, toggle_month)
            
            # Action buttons
            ui.html('<hr class="my-4">', sanitize=False)
            with ui.element('div').classes('field is-grouped'):
                with ui.element('div').classes('control'):
                    ui.button('Apply Filters', on_click=update_grids_and_stats).classes('button is-primary')
                with ui.element('div').classes('control'):
                    ui.button('Clear All', on_click=clear_all_filters).classes('button is-light')
        
        # Data display using UI5 tables
        with ui.element('div').classes('box mt-5'):
            ui.label('Account Summary').classes('title is-5')
            
            # Load initial accounts data and display in UI5 table
            initial_accounts = data_access.get_account_summary()[:10]  # Limit for demo
            if initial_accounts:
                account_columns = ['account_code', 'account_name', 'total_transactions', 'total_debit', 'total_credit', 'net_balance']
                create_ui5_table(initial_accounts, account_columns)

    # ANALYTICS TAB
    with ui.element('div').classes('content').style('display: none;') as analytics_content:
        ui.label('Financial Analytics').classes('title is-4')
        ui.label('Advanced analytics and reporting features').classes('subtitle is-6 has-text-grey')
        
        # Lightdash Integration Section
        with ui.element('div').classes('box mb-5'):
            ui.label('Lightdash Integration').classes('title is-5')
            ui.label('Business Intelligence Dashboard').classes('subtitle is-6 mb-4')
            
            # Mock Lightdash interface using Bulma cards
            with ui.element('div').classes('columns is-multiline'):
                with ui.element('div').classes('column is-6'):
                    with ui.element('div').classes('card'):
                        with ui.element('div').classes('card-content'):
                            ui.label('📊 Balance Sheet').classes('title is-6')
                            ui.label('Assets vs Liabilities over time').classes('subtitle is-7')
                            ui.button('View in Lightdash', on_click=lambda: ui.notify('Lightdash integration - coming soon!')).classes('button is-primary is-small')
                
                with ui.element('div').classes('column is-6'):
                    with ui.element('div').classes('card'):
                        with ui.element('div').classes('card-content'):
                            ui.label('📈 Income Statement').classes('title is-6')
                            ui.label('Revenue and expense analysis').classes('subtitle is-7')
                            ui.button('View in Lightdash', on_click=lambda: ui.notify('Lightdash integration - coming soon!')).classes('button is-primary is-small')
                
                with ui.element('div').classes('column is-6'):
                    with ui.element('div').classes('card'):
                        with ui.element('div').classes('card-content'):
                            ui.label('💰 Cash Flow Statement').classes('title is-6')
                            ui.label('Cash flow trends and projections').classes('subtitle is-7')
                            ui.button('View in Lightdash', on_click=lambda: ui.notify('Lightdash integration - coming soon!')).classes('button is-primary is-small')
                
                with ui.element('div').classes('column is-6'):
                    with ui.element('div').classes('card'):
                        with ui.element('div').classes('card-content'):
                            ui.label('📊 Metrics Over Time').classes('title is-6')
                            ui.label('Key performance indicators').classes('subtitle is-7')
                            ui.button('View in Lightdash', on_click=lambda: ui.notify('Lightdash integration - coming soon!')).classes('button is-primary is-small')
        
        # Plotly Charts Section
        with ui.element('div').classes('box'):
            ui.label('Interactive Charts (Plotly)').classes('title is-5 mb-4')
            create_plotly_sample()

    # LINEAGE TAB
    with ui.element('div').classes('content').style('display: none;') as lineage_content:
        ui.label('Data Lineage').classes('title is-4')
        ui.label('Understand data flow and dependencies').classes('subtitle is-6 has-text-grey')
        
        with ui.element('div').classes('box'):
            ui.label('DBT Model Lineage').classes('title is-5')
            ui.html('''
                <div class="notification is-info is-light">
                    <p class="subtitle is-6">Data Flow Overview</p>
                    <div class="tags">
                        <span class="tag is-link">Excel Files</span>
                        <span class="tag">→</span>
                        <span class="tag is-info">Iceberg Parquet</span>
                        <span class="tag">→</span>
                        <span class="tag is-warning">stg_financial_transactions</span>
                        <span class="tag">→</span>
                        <span class="tag is-success">mart_account_summary</span>
                    </div>
                </div>
            ''', sanitize=False)

    # ADMIN TAB  
    with ui.element('div').classes('content').style('display: none;') as admin_content:
        ui.label('Administration').classes('title is-4')
        ui.label('System administration and configuration').classes('subtitle is-6 has-text-grey')
        
        with ui.element('div').classes('box'):
            ui.label('No changes for now').classes('title is-6')
            ui.label('Future admin features will be added here').classes('has-text-grey')

# Add JavaScript for tab switching
ui.add_body_html('''
<script>
document.addEventListener('DOMContentLoaded', function() {
    const tabs = document.querySelectorAll('.tabs li');
    const contents = document.querySelectorAll('.content');
    
    tabs.forEach((tab, index) => {
        tab.addEventListener('click', function() {
            // Remove active class from all tabs
            tabs.forEach(t => t.classList.remove('is-active'));
            // Add active class to clicked tab
            this.classList.add('is-active');
            
            // Hide all content
            contents.forEach(c => c.style.display = 'none');
            // Show selected content
            contents[index].style.display = 'block';
        });
    });
});
</script>
''')

# Add version footer
create_version_footer()

# Initialize dashboard stats on startup
def initialize_app():
    """Initialize the application with default data."""
    update_dashboard_stats()

# Run initialization
ui.timer(0.1, initialize_app, once=True)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(port=8090, title='Financial Data Platform v0.0.4 - Bulma + UI5')