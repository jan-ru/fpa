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
        if grid is not None:
            grid.options['rowData'] = filtered_transactions
            grid.update()
        
        # Get filtered accounts summary
        if selected_years or selected_months or selected_quarters:
            # For filtered view, we need to aggregate accounts from filtered transactions
            if filtered_transactions:
                df = pl.DataFrame(filtered_transactions)
                filtered_accounts = df.group_by(["account_code", "account_name"]).agg([
                    pl.len().alias("total_transactions"),
                    pl.col("debit_amount").sum().alias("total_debit"),
                    pl.col("credit_amount").sum().alias("total_credit"),
                    pl.col("net_amount").sum().alias("net_balance"),
                ]).with_columns([
                    pl.when(pl.col("net_balance") > 0).then(pl.lit("Net Debit")).otherwise(pl.lit("Net Credit")).alias("account_balance_type"),
                    pl.lit("Active").alias("activity_status")  # All filtered accounts considered active
                ]).to_dicts()
            else:
                filtered_accounts = []
        else:
            # No filters, show all accounts
            filtered_accounts = data_access.get_account_summary()
        
        # Update accounts grid
        if accounts_grid is not None:
            accounts_grid.options['rowData'] = filtered_accounts
            accounts_grid.update()
        
        # Update dashboard statistics
        update_dashboard_stats(stats_cards, selected_years, selected_months, selected_quarters)
        
        # Create filter summary for notification
        filters = []
        if selected_years:
            filters.append(f"years {', '.join(map(str, sorted(selected_years)))}")
        if selected_quarters:
            filters.append(f"quarters {', '.join([f'Q{q}' for q in sorted(selected_quarters)])}")
        if selected_months:
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            filters.append(f"months {', '.join([month_names[m-1] for m in sorted(selected_months)])}")
        
        if filters:
            ui.notify(f'Filtered to {len(filtered_transactions):,} transactions for {" + ".join(filters)}')
        else:
            ui.notify(f'Showing all {len(filtered_transactions):,} transactions')
            
    except Exception as e:
        ui.notify(f'Error filtering data: {str(e)}')


def create_plotly_sample():
    """Create sample Plotly charts for analytics page."""
    if not PLOTLY_AVAILABLE:
        return ui.label("Plotly not available. Install with: uv add plotly")
    
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
                go.Scatter(x=months, y=debits, name='Debits', line=dict(color='red')),
                row=1, col=1
            )
            fig.add_trace(
                go.Scatter(x=months, y=credits, name='Credits', line=dict(color='green')),
                row=1, col=1
            )
            
            # Transaction volume
            volumes = [d['transaction_count'] for d in monthly_data]
            fig.add_trace(
                go.Bar(x=months, y=volumes, name='Transactions', marker_color='blue'),
                row=1, col=2
            )
            
            # Net cash flow
            net_flows = [d['net_amount'] for d in monthly_data]
            fig.add_trace(
                go.Bar(x=months, y=net_flows, name='Net Flow', 
                      marker_color=['green' if x > 0 else 'red' for x in net_flows]),
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
            
            return ui.plotly(fig).classes('w-full')
        else:
            return ui.label("No data available for charts")
            
    except Exception as e:
        return ui.label(f"Error creating charts: {str(e)}")

def generate_and_embed_dbt_docs():
    """Generate dbt docs and return the path to the generated index.html."""
    try:
        # Change to dbt project directory
        dbt_dir = Path("../dbt_project")
        if not dbt_dir.exists():
            ui.notify("DBT project directory not found!")
            return None
        
        # Generate docs
        ui.notify("Generating DBT docs...")
        result = subprocess.run(
            ["uv", "run", "dbt", "docs", "generate"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            ui.notify("DBT docs generated successfully!")
            
            # Find the generated index.html
            docs_path = dbt_dir / "target" / "index.html"
            if docs_path.exists():
                return str(docs_path.absolute())
            else:
                ui.notify("Generated docs file not found!")
                return None
        else:
            ui.notify(f"Error generating docs: {result.stderr}")
            return None
            
    except Exception as e:
        ui.notify(f"Error with dbt docs: {str(e)}")
        return None

def get_dbt_lineage_info():
    """Get dbt lineage information from the project files."""
    dbt_dir = Path("../dbt_project")
    
    lineage_info = {
        "models": [],
        "dependencies": [],
        "sources": []
    }
    
    if not dbt_dir.exists():
        return lineage_info
    
    try:
        # Get models from the models directory
        models_dir = dbt_dir / "models"
        if models_dir.exists():
            for sql_file in models_dir.rglob("*.sql"):
                if not sql_file.name.startswith("my_"):  # Skip example files
                    model_name = sql_file.stem
                    relative_path = sql_file.relative_to(models_dir)
                    
                    # Read the file to find dependencies
                    content = sql_file.read_text()
                    dependencies = []
                    
                    # Find ref() calls
                    import re
                    refs = re.findall(r"ref\(['\"]([^'\"]+)['\"]\)", content)
                    dependencies.extend(refs)
                    
                    # Find source() calls
                    sources = re.findall(r"source\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)", content)
                    
                    lineage_info["models"].append({
                        "name": model_name,
                        "path": str(relative_path),
                        "type": "staging" if "staging" in str(relative_path) else 
                               "intermediate" if "intermediate" in str(relative_path) else "mart",
                        "dependencies": dependencies,
                        "sources": sources
                    })
        
        return lineage_info
        
    except Exception as e:
        ui.notify(f"Error reading dbt project: {str(e)}")
        return lineage_info

def create_lineage_visualization():
    """Create a simple lineage visualization using HTML/CSS."""
    lineage = get_dbt_lineage_info()
    
    if not lineage["models"]:
        return ui.label("No dbt models found. Make sure your dbt project is set up correctly.")
    
    # Create a simple flow diagram
    html_content = """
    <div style="padding: 20px; background: #f8f9fa; border-radius: 8px; margin: 10px 0;">
        <h4 style="margin-bottom: 15px; color: #333;">DBT Model Lineage</h4>
        <div style="display: flex; flex-direction: column; gap: 15px;">
    """
    
    # Group models by type
    staging_models = [m for m in lineage["models"] if m["type"] == "staging"]
    intermediate_models = [m for m in lineage["models"] if m["type"] == "intermediate"]
    mart_models = [m for m in lineage["models"] if m["type"] == "mart"]
    
    # Create layer visualization
    layers = [
        ("Sources", ["Excel Files", "Iceberg Parquet"], "#e3f2fd"),
        ("Staging", [m["name"] for m in staging_models], "#f3e5f5"), 
        ("Intermediate", [m["name"] for m in intermediate_models], "#fff3e0"),
        ("Marts", [m["name"] for m in mart_models], "#e8f5e8")
    ]
    
    for layer_name, models, color in layers:
        if models:
            html_content += f"""
            <div style="display: flex; align-items: center; gap: 10px;">
                <div style="min-width: 100px; font-weight: bold; color: #555;">{layer_name}:</div>
                <div style="display: flex; gap: 8px; flex-wrap: wrap;">
            """
            for model in models:
                html_content += f"""
                <div style="background: {color}; padding: 8px 12px; border-radius: 6px; 
                           border: 1px solid #ddd; font-size: 14px;">{model}</div>
                """
            html_content += "</div></div>"
            
            # Add arrow if not the last layer
            if layer_name != "Marts":
                html_content += """
                <div style="text-align: center; color: #666; font-size: 18px;">↓</div>
                """
    
    html_content += """
        </div>
    </div>
    """
    
    return ui.html(html_content, sanitize=False)


def open_dbt_docs_external():
    """Open dbt docs in external browser."""
    try:
        dbt_dir = Path("../dbt_project")
        
        # Generate docs first
        result = subprocess.run(
            ["uv", "run", "dbt", "docs", "generate"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            # Start docs server
            subprocess.Popen(
                ["uv", "run", "dbt", "docs", "serve", "--port", "8080"],
                cwd=dbt_dir
            )
            
            # Open in browser
            ui.run_javascript('window.open("http://localhost:8080", "_blank")')
            ui.notify("DBT docs opened in new tab")
        else:
            ui.notify(f"Error generating docs: {result.stderr}")
            
    except Exception as e:
        ui.notify(f"Error: {str(e)}")

# Initialize data and button lists
year_buttons = []
month_buttons = []
quarter_buttons = []

# Create the main application
ui.page_title(TITLE)

# Setup Bulma CSS and FontAwesome with enhanced button styling
ui.add_head_html('''
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.9.4/css/bulma.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <style>
        /* Enhanced Bulma buttons with professional styling */
        .btn-emphasized {
            background: linear-gradient(135deg, #3273dc 0%, #2366d1 100%);
            border: none;
            color: white !important;
            font-weight: 600;
            box-shadow: 0 2px 4px rgba(50, 115, 220, 0.2);
            transition: all 0.2s ease;
        }
        .btn-emphasized:hover {
            transform: translateY(-1px);
            box-shadow: 0 4px 8px rgba(50, 115, 220, 0.3);
        }
        .btn-positive {
            background: linear-gradient(135deg, #48c774 0%, #3ec46d 100%);
            border: none;
            color: white !important;
            font-weight: 600;
            box-shadow: 0 2px 4px rgba(72, 199, 116, 0.2);
        }
        .btn-transparent {
            background: transparent;
            border: 1px solid #dbdbdb;
            color: #363636 !important;
            font-weight: 500;
        }
        .btn-transparent:hover {
            background: #f5f5f5;
            border-color: #b5b5b5;
        }
    </style>
''')

# Hero section with Bulma styling
with ui.element('div').classes('hero is-primary is-small'):
    with ui.element('div').classes('hero-body'):
        with ui.element('div').classes('container'):
            ui.label('Financial Planning & Analysis').classes('title is-3 has-text-white')

# Main navigation tabs
with ui.tabs().classes('w-full') as main_tabs:
    inputs_tab = ui.tab('Inputs')
    dashboard_tab = ui.tab('Dashboard')
    analytics_tab = ui.tab('Analytics')
    lineage_tab = ui.tab('Lineage')
    admin_tab = ui.tab('Admin')

with ui.tab_panels(main_tabs, value=dashboard_tab).classes('w-full').style('padding-bottom: 50px;'):
    
    # INPUTS TAB - Updated for v0.0.3
    with ui.tab_panel(inputs_tab):
        create_page_header('Data Inputs', 'Excel file inventory and processing status')
        
        with ui.card().classes('w-full p-4'):
            ui.label('Excel Files in Data Directory').classes('text-h6 mb-2')
            
            # Get and display Excel files with enhanced information
            excel_files = get_excel_files_data()
            
            if excel_files:
                # Use consistent Bulma table for Excel files
                file_columns = ['filename', 'size_mb', 'n_columns', 'n_rows', 'modified', 'status']
                create_bulma_table(excel_files, file_columns, "excel-files-table")
                
                ui.label(f'Showing {len(excel_files)} Excel files').classes('help')
            else:
                ui.label('No Excel files found in ../data/raw directory').classes('text-caption')
            
            ui.separator().classes('my-4')
            ui.label('File Processing Status:').classes('mb-2')
            ui.label('✅ Processed files have been imported into the data warehouse').classes('text-caption mb-1')
            ui.label('⚠️ Skipped files have format incompatibilities and require manual review').classes('text-caption')

    # DASHBOARD TAB (No changes)
    with ui.tab_panel(dashboard_tab):
        ui.label('Financial Data Dashboard').classes('text-h4 text-center mb-4')
        
        # Dashboard Statistics Cards (will be updated with filtered data)
        stats_config_row1 = [
            {'key': 'total_accounts_card', 'label': 'Total Accounts: Loading...'},
            {'key': 'active_accounts_card', 'label': 'Active Accounts: Loading...'},
            {'key': 'total_assets_card', 'label': 'Total Assets: Loading...'},
            {'key': 'total_liabilities_card', 'label': 'Total Liabilities: Loading...'}
        ]
        create_stats_cards(stats_config_row1, stats_cards)
        
        stats_config_row2 = [
            {'key': 'total_transactions_card', 'label': 'Total Transactions: Loading...'},
            {'key': 'total_debit_card', 'label': 'Total Debit: Loading...'}
        ]
        create_stats_cards(stats_config_row2, stats_cards)
        
        # UI5 Date Range Filter
        with ui.element('div').classes('box mb-4'):
            ui.label('Filter Data').classes('title is-5 mb-3')
            ui.label('Use the date range picker to filter transactions by date period').classes('subtitle is-6 has-text-grey mb-3')
            
            # Add professional date filter
            create_bulma_date_filter()
            
            # Quick filter buttons with enhanced styling
            ui.label('Quick Filters:').classes('label mt-4')
            with ui.element('div').classes('field is-grouped'):
                years = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
                for year in years:
                    create_enhanced_button(str(year), 'default')
            
            with ui.element('div').classes('field is-grouped mt-2'):
                create_enhanced_button('Apply Filters', 'emphasized', icon='filter')
                create_enhanced_button('Clear All', 'transparent', icon='clear')
        
        # Data tabs for accounts and transactions
        with ui.tabs().classes('w-full') as data_tabs:
            accounts_data_tab = ui.tab('Accounts')
            transactions_data_tab = ui.tab('Transactions')
        
        with ui.tab_panels(data_tabs, value=accounts_data_tab).classes('w-full'):
            # Accounts grid with UI5 Table
            with ui.tab_panel(accounts_data_tab):
                with ui.element('div').classes('box'):
                    ui.label('Account Summary').classes('title is-5 mb-3')
                    ui.label('Overview of all accounts with balances and activity').classes('subtitle is-6 has-text-grey mb-4')
                    
                    # Load initial accounts data and sort by account code
                    initial_accounts = get_sorted_accounts()
                    account_columns = ['account_code', 'account_name', 'total_transactions', 'total_debit', 'total_credit', 'net_balance']
                    create_bulma_table(initial_accounts, account_columns, "accounts-table")
                    
                    if initial_accounts:
                        ui.label(f'Showing {len(initial_accounts)} accounts (limited to first 20 rows for performance)').classes('help')
            
            # Transactions grid with UI5 Table
            with ui.tab_panel(transactions_data_tab):
                with ui.element('div').classes('box'):
                    ui.label('Transaction Details').classes('title is-5 mb-3')
                    ui.label('Detailed view of financial transactions').classes('subtitle is-6 has-text-grey mb-4')
                    
                    # Load initial transactions data
                    initial_transactions = get_limited_transactions(limit=20)
                    transaction_columns = ['account_code', 'account_name', 'transaction_date', 'description', 'debit_amount', 'credit_amount', 'net_amount']
                    create_bulma_table(initial_transactions, transaction_columns, "transactions-table")
                    
                    if initial_transactions:
                        ui.label(f'Showing {len(initial_transactions)} transactions (limited to first 20 rows for performance)').classes('help')

    # ANALYTICS TAB (No changes)
    with ui.tab_panel(analytics_tab):
        create_page_header('Financial Analytics', 'Advanced analytics and reporting features')
        
        # Lightdash Integration Section
        with ui.card().classes('w-full p-4 mb-4'):
            ui.label('Lightdash Integration').classes('text-h6 mb-2')
            ui.label('Business Intelligence Dashboard').classes('text-subtitle2 mb-4')
            
            # Mock Lightdash interface
            lightdash_cards = [
                {'title': '📊 Balance Sheet'},
                {'title': '📈 Income Statement'},
                {'title': '💰 Cash Flow Statement'},
                {'title': '📊 Metrics Over Time'}
            ]
            create_lightdash_cards(lightdash_cards)
        
        # Plotly Charts Section
        with ui.card().classes('w-full p-4'):
            ui.label('Interactive Charts (Plotly)').classes('text-h6 mb-4')
            
            # Create sample Plotly charts
            create_plotly_sample()

    # LINEAGE TAB (No changes)
    with ui.tab_panel(lineage_tab):
        create_page_header('Data Lineage', 'Understand data flow and dependencies')
        
        # Visual lineage diagram
        with ui.card().classes('w-full p-4 mb-4'):
            ui.label('DBT Model Lineage Visualization').classes('text-h6 mb-2')
            create_lineage_visualization()
        
        # Model details table  
        with ui.card().classes('w-full p-4 mb-4'):
            ui.label('Model Details & Dependencies').classes('text-h6 mb-2')
            # Get DBT models data and create table
            model_data = get_dbt_models_data()
            if model_data:
                model_columns = ['name', 'type', 'path', 'dependencies', 'sources']
                create_bulma_table(model_data, model_columns, "dbt-models-table")
            else:
                ui.html('<p class="has-text-grey">No models to display</p>', sanitize=False)
        
        # Generate full dbt docs option
        with ui.card().classes('w-full p-4'):
            ui.label('Full DBT Documentation').classes('text-h6 mb-2')
            ui.label('For complete lineage exploration with interactive graphs and detailed column lineage:').classes('mb-2')
            
            with ui.row().classes('gap-4'):
                create_enhanced_button('Generate DBT Docs', 'positive', 
                                     on_click=lambda: generate_and_embed_dbt_docs(), 
                                     icon='description')
                create_enhanced_button('Open DBT Docs (External)', 'default', 
                                     on_click=lambda: open_dbt_docs_external(),
                                     icon='open_in_new')
            
            ui.label('The external docs provide the full interactive DAG, column-level lineage, and model documentation.').classes('text-caption mt-2')

    # ADMIN TAB (No changes)
    with ui.tab_panel(admin_tab):
        create_page_header('Administration', 'System administration and configuration')
        
        with ui.card().classes('w-full p-4'):
            ui.label('No changes for now').classes('text-subtitle2')
            ui.label('Future admin features will be added here').classes('text-caption')

# Add version footer to every page
create_version_footer(APP_VERSION)

# Initialize dashboard stats on startup
def initialize_app():
    """Initialize the application with default data."""
    update_dashboard_stats(stats_cards, selected_years, selected_months, selected_quarters)

# Run initialization
ui.timer(0.1, initialize_app, once=True)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(port=PORT, title=TITLE)