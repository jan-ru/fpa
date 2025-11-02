#!/usr/bin/env python3
"""
Financial Data Platform v0.0.5
Proof-of-concept with UI5 Web Components integration and dbt docs.
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

# Import extracted components and services
from components import (
    create_stats_cards,
    create_page_header,
    create_lightdash_cards,
    create_enhanced_button,
    create_bulma_date_filter,
    create_bulma_table
)
from services import (
    get_sorted_accounts,
    get_limited_transactions,
    get_excel_files_data,
    get_dbt_models_data
)
from utils import (
    get_dbt_version,
    get_data_timestamp,
    create_version_footer,
    create_filter_buttons,
    toggle_selection,
    update_button_colors,
    toggle_year,
    toggle_month,
    toggle_quarter,
    clear_all_filters,
    get_filtered_stats,
    update_dashboard_stats
)
from config import (
    APP_VERSION,
    APP_NAME,
    PORT,
    TITLE,
    TRANSACTION_COLUMNS,
    ACCOUNT_COLUMNS,
    create_aggrid_config,
    create_column_def
)

# Global state for filters and components
selected_years = set()
selected_months = set()
selected_quarters = set()
grid = None
accounts_grid = None
stats_cards = {}

def add_ui5_support():
    """Add UI5 Web Components support to the page."""
    # Add UI5 web components CSS and JS
    ui.add_head_html('''
        <script type="module" src="https://unpkg.com/@ui5/webcomponents@1.24.0/dist/Button.js"></script>
        <script type="module" src="https://unpkg.com/@ui5/webcomponents@1.24.0/dist/DatePicker.js"></script>
        <script type="module" src="https://unpkg.com/@ui5/webcomponents@1.24.0/dist/Table.js"></script>
        <script type="module" src="https://unpkg.com/@ui5/webcomponents@1.24.0/dist/TableColumn.js"></script>
        <script type="module" src="https://unpkg.com/@ui5/webcomponents@1.24.0/dist/TableRow.js"></script>
        <script type="module" src="https://unpkg.com/@ui5/webcomponents@1.24.0/dist/TableCell.js"></script>
    ''')

def create_ui5_test_button():
    """Create a test UI5 button to verify integration."""
    # First add a regular NiceGUI button as fallback
    ui.button('Regular NiceGUI Button (fallback)', 
              on_click=lambda: ui.notify('Regular button works!'))
    
    # Add some spacing
    ui.html('<div style="margin: 20px 0;"></div>', sanitize=False)
    
    # Now add the UI5 button with better visibility
    ui5_button_html = '''
    <div style="padding: 20px; border: 2px dashed #007bff; border-radius: 8px; background-color: #f8f9fa;">
        <h4 style="margin-bottom: 15px; color: #007bff;">UI5 Web Component Test Area</h4>
        <p style="margin-bottom: 15px; color: #6c757d;">If you see a blue "UI5 Test Button" below, the integration is working:</p>
        <ui5-button id="ui5TestButton" design="Emphasized" style="margin: 10px 0;">
            🚀 UI5 Test Button
        </ui5-button>
        <br>
        <small style="color: #6c757d;">Click the blue UI5 button above to test the integration</small>
    </div>
    '''
    
    ui5_button_js = '''
    <script>
        console.log('UI5 button script loading...');
        
        function initUI5Button() {
            const ui5Button = document.getElementById('ui5TestButton');
            console.log('UI5 Button element:', ui5Button);
            
            if (ui5Button) {
                ui5Button.addEventListener('click', function() {
                    alert('🎉 UI5 Button Integration Working! 🎉');
                    console.log('UI5 Button clicked successfully!');
                });
                console.log('UI5 button event listener added');
            } else {
                console.log('UI5 button element not found');
            }
        }
        
        // Try multiple times to ensure UI5 components are loaded
        document.addEventListener('DOMContentLoaded', function() {
            console.log('DOM loaded, waiting for UI5...');
            setTimeout(initUI5Button, 500);
            setTimeout(initUI5Button, 1500);
            setTimeout(initUI5Button, 3000);
        });
        
        // Also try when UI5 is ready
        if (window.addEventListener) {
            window.addEventListener('load', function() {
                setTimeout(initUI5Button, 1000);
            });
        }
    </script>
    '''
    
    # Add the button HTML and JavaScript
    ui.html(ui5_button_html, sanitize=False)
    ui.add_body_html(ui5_button_js)

def update_grids_and_stats():
    """Update both grids and dashboard statistics when filters change."""
    try:
        # Get filtered data
        filtered_transactions = data_access.get_filtered_transactions(
            years=list(selected_years) if selected_years else None,
            quarters=list(selected_quarters) if selected_quarters else None,
            months=list(selected_months) if selected_months else None,
            limit=1000
        )
        
        # Update transactions grid
        if grid and hasattr(grid, 'options'):
            grid.options['rowData'] = filtered_transactions
            grid.update()
        
        # Update accounts grid with filtered data
        filtered_accounts = data_access.get_filtered_accounts(
            years=list(selected_years) if selected_years else None,
            quarters=list(selected_quarters) if selected_quarters else None,
            months=list(selected_months) if selected_months else None,
            limit=50
        )
        
        if accounts_grid and hasattr(accounts_grid, 'options'):
            accounts_grid.options['rowData'] = filtered_accounts
            accounts_grid.update()
        
        # Update dashboard stats
        update_dashboard_stats(stats_cards, selected_years, selected_quarters, selected_months)
        
        # Show notification
        filter_summary = []
        if selected_years:
            filter_summary.append(f"Years: {', '.join(map(str, sorted(selected_years)))}")
        if selected_quarters:
            filter_summary.append(f"Quarters: {', '.join([f'Q{q}' for q in sorted(selected_quarters)])}")
        if selected_months:
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            filter_summary.append(f"Months: {', '.join([month_names[m-1] for m in sorted(selected_months)])}")
        
        if filter_summary:
            ui.notify(f'Filtered to {len(filtered_transactions)} transactions | {" | ".join(filter_summary)}')
        else:
            ui.notify(f'Showing all {len(filtered_transactions)} transactions')
            
    except Exception as e:
        ui.notify(f'Error updating data: {str(e)}', type='negative')

def submit_query():
    """Execute query with selected filters and update grids."""
    update_grids_and_stats()

# Initialize UI5 support
add_ui5_support()

# Set up custom CSS for better Bulma integration
ui.add_head_html('''
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.9.4/css/bulma.min.css">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
<style>
    /* Enhanced button styles with Bulma + UI5 compatibility */
    .btn-emphasized {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border: none;
        color: white;
        font-weight: 600;
        box-shadow: 0 4px 15px 0 rgba(116, 75, 162, 0.75);
        transition: all 0.3s ease;
    }
    .btn-emphasized:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px 0 rgba(116, 75, 162, 0.4);
    }
    .btn-positive {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border: none;
        color: white;
        font-weight: 500;
    }
    .btn-transparent {
        background: transparent;
        border: 1px solid #dbdbdb;
        color: #363636;
    }
    .btn-transparent:hover {
        background-color: #f5f5f5;
        border-color: #b5b5b5;
    }
    
    /* UI5 Button integration styles */
    .ui5-button-container {
        display: flex;
        align-items: center;
        gap: 10px;
    }
    
    ui5-button {
        margin: 0 5px;
    }
    
    /* Ensure proper spacing */
    .q-page {
        padding: 1rem;
    }
</style>
''')

# Main navigation tabs
with ui.tabs().classes('w-full') as main_tabs:
    dashboard_tab = ui.tab('Dashboard')
    data_tab = ui.tab('Data')
    lineage_tab = ui.tab('Lineage')
    inputs_tab = ui.tab('Inputs')
    outputs_tab = ui.tab('Outputs')

with ui.tab_panels(main_tabs, value=dashboard_tab).classes('w-full'):
    # Dashboard screen
    with ui.tab_panel(dashboard_tab):
        create_page_header('FPA Dashboard', 'Real-time financial data insights and analytics')
        
        # Create statistics cards
        stats_config_row1 = [
            {'key': 'total_transactions', 'label': 'Loading...'},
            {'key': 'total_accounts', 'label': 'Loading...'},
            {'key': 'date_range', 'label': 'Loading...'},
            {'key': 'total_balance', 'label': 'Loading...'}
        ]
        create_stats_cards(stats_config_row1, stats_cards)
        
        stats_config_row2 = [
            {'key': 'avg_transaction', 'label': 'Loading...'},
            {'key': 'largest_transaction', 'label': 'Loading...'},
            {'key': 'data_freshness', 'label': 'Loading...'},
            {'key': 'processing_status', 'label': 'Loading...'}
        ]
        create_stats_cards(stats_config_row2, stats_cards)
        
        # Add UI5 test button
        with ui.element('div').classes('box mb-4'):
            ui.label('UI5 Web Components Integration Test').classes('title is-5 mb-3')
            ui.label('This demonstrates UI5 web components working within NiceGUI').classes('subtitle is-6 has-text-grey mb-3')
            create_ui5_test_button()
        
        # Date filter section with enhanced Bulma styling
        with ui.element('div').classes('box mb-4'):
            ui.label('Filter Data').classes('title is-5 mb-3')
            ui.label('Use the date range picker to filter transactions by date period').classes('subtitle is-6 has-text-grey mb-3')
            create_bulma_date_filter()
        
        # Filter buttons section
        with ui.element('div').classes('box mb-4'):
            ui.label('Quick Filters').classes('title is-5 mb-3')
            
            # Initialize button lists
            year_buttons = []
            quarter_buttons = []
            month_buttons = []
            
            # Year filter buttons
            ui.label('Years:').classes('subtitle is-6 mb-2')
            create_filter_buttons([2020, 2021, 2022, 2023, 2024, 2025], 
                                selected_years, year_buttons, 
                                lambda year: toggle_year(year, selected_years, year_buttons, update_grids_and_stats))
            
            # Quarter filter buttons  
            ui.label('Quarters:').classes('subtitle is-6 mb-2 mt-3')
            create_filter_buttons(['Q1', 'Q2', 'Q3', 'Q4'], 
                                selected_quarters, quarter_buttons,
                                lambda quarter: toggle_quarter(quarter, selected_quarters, quarter_buttons, update_grids_and_stats))
            
            # Month filter buttons
            ui.label('Months:').classes('subtitle is-6 mb-2 mt-3')
            create_filter_buttons(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'], 
                                selected_months, month_buttons,
                                lambda month: toggle_month(month, selected_months, month_buttons, update_grids_and_stats))
            
            # Action buttons
            with ui.row().classes('mt-4 gap-3'):
                create_enhanced_button('Apply Filters', 'emphasized', 
                                     on_click=submit_query, icon='filter')
                create_enhanced_button('Clear All', 'transparent', 
                                     on_click=lambda: clear_all_filters(selected_years, selected_months, selected_quarters,
                                                                        year_buttons, month_buttons, quarter_buttons,
                                                                        update_grids_and_stats), icon='clear')

    # Data screen with sub-tabs
    with ui.tab_panel(data_tab):
        create_page_header('FPA Data Analysis', 'Detailed account and transaction analysis')
        
        # Sub-tabs for accounts and transactions
        with ui.tabs().classes('w-full') as data_tabs:
            accounts_data_tab = ui.tab('Accounts')
            transactions_data_tab = ui.tab('Transactions')
        
        with ui.tab_panels(data_tabs, value=accounts_data_tab).classes('w-full'):
            # Accounts grid
            with ui.tab_panel(accounts_data_tab):
                with ui.element('div').classes('box'):
                    ui.label('Account Summary').classes('title is-5 mb-3')
                    ui.label('Aggregated view of all chart of accounts with transaction summaries').classes('subtitle is-6 has-text-grey mb-3')
                    
                    # Load and display accounts data
                    initial_accounts = get_sorted_accounts()
                    if initial_accounts:
                        # Limit to first 20 for performance
                        display_accounts = initial_accounts[:20]
                        accounts_config = create_aggrid_config(ACCOUNT_COLUMNS, display_accounts, 
                                                             paginationPageSize=25, animateRows=True)
                        accounts_grid = ui.aggrid(accounts_config).classes('w-full').style('height: 600px')
                    else:
                        ui.label('No account data available').classes('has-text-danger')
                    
                    if initial_accounts:
                        ui.label(f'Showing {len(initial_accounts[:20])} accounts (limited to first 20 rows for performance)').classes('help')
            
            # Transactions grid
            with ui.tab_panel(transactions_data_tab):
                with ui.element('div').classes('box'):
                    ui.label('Transaction Details').classes('title is-5 mb-3')
                    ui.label('Individual transaction records with full details and filtering capabilities').classes('subtitle is-6 has-text-grey mb-3')
                    
                    # Load and display transactions data
                    initial_transactions = get_limited_transactions(limit=100)
                    if initial_transactions:
                        transaction_config = create_aggrid_config(TRANSACTION_COLUMNS, initial_transactions,
                                                                rowSelection='multiple', animateRows=True, 
                                                                sideBar=True, paginationPageSize=50)
                        grid = ui.aggrid(transaction_config).classes('w-full').style('height: 600px')
                    else:
                        ui.label('No transaction data available').classes('has-text-danger')
                    
                    if initial_transactions:
                        ui.label(f'Showing {len(initial_transactions)} transactions (limited to first 100 rows for performance)').classes('help')

    # Lineage screen with dbt docs integration
    with ui.tab_panel(lineage_tab):
        create_page_header('FPA Data Lineage', 'Data transformation documentation and lineage')
        
        with ui.element('div').classes('box mb-4'):
            ui.label('dbt Documentation').classes('title is-5 mb-3')
            ui.label('Interactive data lineage and model documentation').classes('subtitle is-6 has-text-grey mb-3')
            
            # Check if dbt docs exist
            dbt_docs_path = Path('../dbt_project/target/index.html')
            if dbt_docs_path.exists():
                # Create iframe to display dbt docs
                iframe_html = f'''
                <iframe 
                    src="/static/dbt_docs/index.html" 
                    width="100%" 
                    height="800px" 
                    frameborder="0"
                    style="border: 1px solid #dbdbdb; border-radius: 6px;">
                </iframe>
                '''
                ui.html(iframe_html, sanitize=False)
                
                # Add instructions
                with ui.element('div').classes('notification is-info mt-4'):
                    ui.html('''
                    <p><strong>dbt Documentation Features:</strong></p>
                    <ul>
                        <li>Interactive data lineage graph</li>
                        <li>Model and column documentation</li>
                        <li>Test results and metadata</li>
                        <li>Data freshness information</li>
                    </ul>
                    ''', sanitize=False)
            else:
                with ui.element('div').classes('notification is-warning'):
                    ui.label('dbt documentation not found. Please run "dbt docs generate" first.').classes('has-text-weight-semibold')
                    
                    def generate_docs():
                        try:
                            result = subprocess.run(['dbt', 'docs', 'generate'], 
                                                  cwd='../dbt_project', 
                                                  capture_output=True, text=True)
                            if result.returncode == 0:
                                ui.notify('dbt docs generated successfully! Please refresh the page.')
                            else:
                                ui.notify(f'Error generating docs: {result.stderr}', type='negative')
                        except Exception as e:
                            ui.notify(f'Error: {str(e)}', type='negative')
                    
                    create_enhanced_button('Generate dbt Docs', 'emphasized', 
                                         on_click=generate_docs, icon='description')

    # Inputs screen
    with ui.tab_panel(inputs_tab):
        create_page_header('FPA Data Inputs', 'Import and manage source data files')
        
        with ui.element('div').classes('box'):
            ui.label('Excel File Import').classes('title is-5 mb-3')
            ui.label('Current data files in database:').classes('subtitle is-6 mb-3')
            
            # Display current files
            excel_files_data = get_excel_files_data()
            for file_info in excel_files_data:
                with ui.element('div').classes('media'):
                    with ui.element('div').classes('media-content'):
                        ui.label(f"• {file_info['filename']} - {file_info['n_rows']:,} records").classes('has-text-weight-semibold')
                        if 'status' in file_info:
                            ui.label(f"Status: {file_info['status']}").classes('help')
            
            ui.separator().classes('my-4')
            ui.label('Import new Excel files:').classes('subtitle is-6 mb-3')
            create_enhanced_button('Select Excel File', 'emphasized', 
                                 on_click=lambda: ui.notify('File import feature - coming soon!'), 
                                 icon='upload_file')
            ui.label('Supported formats: .xlsx files with 19-column financial data structure').classes('help mt-2')

    # Outputs screen
    with ui.tab_panel(outputs_tab):
        create_page_header('FPA Data Outputs', 'Export and visualize financial data')
        
        with ui.row().classes('gap-4 w-full'):
            # Export options
            with ui.element('div').classes('box flex-1'):
                ui.label('Export Data').classes('title is-6 mb-3')
                ui.label('Export filtered data in various formats:').classes('subtitle is-6 mb-4')
                
                with ui.column().classes('gap-3'):
                    create_enhanced_button('Export to CSV', 'default', 
                                         on_click=lambda: ui.notify('CSV export - coming soon!'),
                                         icon='download')
                    create_enhanced_button('Export to Excel', 'default',
                                         on_click=lambda: ui.notify('Excel export - coming soon!'),
                                         icon='download')
                    create_enhanced_button('Export to PDF Report', 'default',
                                         on_click=lambda: ui.notify('PDF export - coming soon!'),
                                         icon='picture_as_pdf')
            
            # Visualizations
            with ui.element('div').classes('box flex-1'):
                ui.label('Visualizations').classes('title is-6 mb-3')
                ui.label('Generate charts and reports:').classes('subtitle is-6 mb-4')
                
                with ui.column().classes('gap-3'):
                    create_enhanced_button('Account Balance Chart', 'default',
                                         on_click=lambda: ui.notify('Chart generation - coming soon!'),
                                         icon='bar_chart')
                    create_enhanced_button('Monthly Trends', 'default',
                                         on_click=lambda: ui.notify('Trend analysis - coming soon!'),
                                         icon='trending_up')
                    create_enhanced_button('Custom Report', 'default',
                                         on_click=lambda: ui.notify('Custom reports - coming soon!'),
                                         icon='assessment')

# Initialize dashboard statistics on startup
update_dashboard_stats(stats_cards, selected_years, selected_quarters, selected_months)

# Add version footer
create_version_footer("v0.0.5")

# Set up static file serving for dbt docs
dbt_docs_path = Path('../dbt_project/target')
if dbt_docs_path.exists():
    from nicegui import app
    from fastapi import staticfiles
    app.mount('/static/dbt_docs', staticfiles.StaticFiles(directory=str(dbt_docs_path)), name='dbt_docs')

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(port=8081)