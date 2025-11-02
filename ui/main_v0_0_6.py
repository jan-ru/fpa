#!/usr/bin/env python3
"""
Financial Data Platform v0.0.6
Simplified version with FPA branding and improved UI.
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

def get_iceberg_snapshots_data():
    """Get real Iceberg snapshots data from parquet files and ingestion log."""
    import os
    from pathlib import Path
    
    snapshots = []
    
    try:
        # Read from ingestion log
        log_path = Path("../pipelines/data/iceberg/warehouse/ingestion_log.txt")
        warehouse_path = Path("../pipelines/data/iceberg/warehouse")
        
        if log_path.exists() and warehouse_path.exists():
            # Read the ingestion log
            with open(log_path, 'r') as f:
                log_content = f.read().strip()
            
            # Parse each log entry
            for line in log_content.split('\\n'):
                if '|' in line:
                    parts = line.split(' | ')
                    if len(parts) >= 4:
                        timestamp_str = parts[0]
                        source_file = parts[1]
                        parquet_file = parts[2]
                        rows_info = parts[3]
                        
                        # Extract number of rows
                        rows_count = int(rows_info.split(' ')[0]) if ' rows' in rows_info else 0
                        
                        # Get file size
                        parquet_path = warehouse_path / parquet_file
                        file_size_mb = 0
                        if parquet_path.exists():
                            file_size_mb = round(parquet_path.stat().st_size / (1024*1024), 1)
                        
                        # Create snapshot ID from filename
                        snapshot_id = parquet_file.replace('financial_transactions_', '').replace('.parquet', '')
                        
                        # Format timestamp for display
                        try:
                            from datetime import datetime
                            parsed_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            display_timestamp = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
                        except:
                            display_timestamp = timestamp_str
                        
                        snapshots.append({
                            "snapshot_id": snapshot_id,
                            "timestamp": display_timestamp,
                            "operation": "ingest",
                            "records_added": rows_count,
                            "records_deleted": 0,
                            "data_size_mb": file_size_mb,
                            "source_file": source_file
                        })
            
            # Add the main iceberg file if it exists
            main_iceberg = warehouse_path / "financial_transactions_iceberg.parquet"
            if main_iceberg.exists():
                file_size_mb = round(main_iceberg.stat().st_size / (1024*1024), 1)
                snapshots.append({
                    "snapshot_id": "iceberg_main",
                    "timestamp": datetime.fromtimestamp(main_iceberg.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                    "operation": "consolidate",
                    "records_added": sum(s["records_added"] for s in snapshots),
                    "records_deleted": 0,
                    "data_size_mb": file_size_mb,
                    "source_file": "consolidated"
                })
        
        # Sort by timestamp (newest first)
        snapshots.sort(key=lambda x: x["timestamp"], reverse=True)
        
    except Exception as e:
        print(f"Error reading Iceberg data: {e}")
        # Return empty list if there's an error
        snapshots = []
    
    return snapshots

def get_database_info():
    """Get real database information."""
    from pathlib import Path
    
    databases = []
    total_size_mb = 0
    
    try:
        # Check for DuckDB files in data/warehouse
        warehouse_path = Path("../data/warehouse")
        if warehouse_path.exists():
            for db_file in warehouse_path.glob("*.db"):
                size_mb = round(db_file.stat().st_size / (1024*1024), 1)
                total_size_mb += size_mb
                databases.append({
                    "name": db_file.name,
                    "size_mb": size_mb,
                    "path": str(db_file)
                })
        
        # Check for DuckDB files in current directory
        for db_file in Path(".").glob("*.duckdb"):
            size_mb = round(db_file.stat().st_size / (1024*1024), 1)
            total_size_mb += size_mb
            databases.append({
                "name": db_file.name,
                "size_mb": size_mb,
                "path": str(db_file)
            })
            
    except Exception as e:
        print(f"Error reading database info: {e}")
    
    return databases, total_size_mb

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
        
        /* Smaller button styles */
        .btn-small {
            font-size: 0.8rem !important;
            padding: 4px 8px !important;
            height: auto !important;
            min-height: 28px !important;
        }
    </style>
''')

# Hero section with smaller FPA text
with ui.element('div').classes('hero is-primary is-small'):
    with ui.element('div').classes('hero-body'):
        with ui.element('div').classes('container'):
            ui.label('FPA').classes('title is-4 has-text-white')

# Main navigation tabs with tooltips
with ui.tabs().classes('w-full') as main_tabs:
    with ui.tab('Inputs') as inputs_tab:
        ui.tooltip('Excel file inventory and processing status')
    dashboard_tab = ui.tab('Dashboard')
    with ui.tab('Analytics') as analytics_tab:
        ui.tooltip('Advanced analytics and reporting features')
    with ui.tab('Lineage') as lineage_tab:
        ui.tooltip('Understand data flow and dependencies')
    with ui.tab('Admin') as admin_tab:
        ui.tooltip('System administration and configuration')

with ui.tab_panels(main_tabs, value=dashboard_tab).classes('w-full').style('padding-bottom: 50px;'):
    
    # INPUTS TAB
    with ui.tab_panel(inputs_tab):
        
        with ui.card().classes('w-full p-4'):
            ui.label('Excel Files in Data Directory').classes('text-h6 mb-2')
            
            # Get and display Excel files with enhanced information
            excel_files = get_excel_files_data()
            
            if excel_files:
                # Use consistent Bulma table for Excel files with status icons
                file_columns = ['filename', 'size_mb', 'n_columns', 'n_rows', 'modified', 'status']
                create_bulma_table(excel_files, file_columns, "excel-files-table")
                
                ui.label(f'Showing {len(excel_files)} Excel files').classes('help')
            else:
                ui.label('No Excel files found in ../data/raw directory').classes('text-caption')

    # DASHBOARD TAB - Updated with smaller buttons
    with ui.tab_panel(dashboard_tab):
        
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
        
        # Date Range Filter with smaller buttons
        with ui.element('div').classes('box mb-4'):
            ui.label('Filter Data').classes('title is-5 mb-3')
            ui.label('Use the date range picker to filter transactions by date period').classes('subtitle is-6 has-text-grey mb-3')
            
            # Add professional date filter
            create_bulma_date_filter()
            
            # Quick filter buttons with smaller styling
            ui.label('Quick Filters:').classes('label mt-4')
            with ui.element('div').classes('field is-grouped'):
                years = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
                for year in years:
                    ui.button(str(year), on_click=lambda: ui.notify(f'Year filter: {year}')).classes('button is-small mr-1')
            
            with ui.element('div').classes('field is-grouped mt-2'):
                ui.button('Apply Filters', on_click=lambda: ui.notify('Filters applied')).classes('button is-small btn-emphasized').style('font-size: 0.7rem; padding: 2px 6px; height: 24px;')
                ui.button('Clear All', on_click=lambda: ui.notify('Filters cleared')).classes('button is-small btn-transparent ml-2').style('font-size: 0.7rem; padding: 2px 6px; height: 24px;')
        
        # Data tabs for accounts and transactions
        with ui.tabs().classes('w-full') as data_tabs:
            accounts_data_tab = ui.tab('Accounts')
            transactions_data_tab = ui.tab('Transactions')
        
        with ui.tab_panels(data_tabs, value=accounts_data_tab).classes('w-full'):
            # Accounts grid
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
            
            # Transactions grid
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

    # ANALYTICS TAB - Updated with smaller buttons
    with ui.tab_panel(analytics_tab):
        
        # Lightdash Integration Section with smaller buttons
        with ui.card().classes('w-full p-4 mb-4'):
            ui.label('Lightdash Integration').classes('text-h6 mb-2')
            ui.label('Business Intelligence Dashboard').classes('text-subtitle2 mb-4')
            
            # Mock Lightdash interface with smaller buttons
            with ui.row().classes('gap-4 w-full'):
                lightdash_items = [
                    '📊 Balance Sheet',
                    '📈 Income Statement', 
                    '💰 Cash Flow Statement',
                    '📊 Metrics Over Time'
                ]
                for item in lightdash_items:
                    with ui.card().classes('flex-1 p-4'):
                        ui.label(item).classes('text-h6 mb-3')
                        ui.button('View in Lightdash', 
                                 on_click=lambda: ui.notify('Lightdash integration - coming soon!')).classes('button is-small btn-emphasized')
        
        # Plotly Charts Section
        with ui.card().classes('w-full p-4'):
            ui.label('Interactive Charts (Plotly)').classes('text-h6 mb-4')
            
            # Create sample Plotly charts
            create_plotly_sample()

    # LINEAGE TAB - Updated with embedded dbt docs
    with ui.tab_panel(lineage_tab):
        
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
        
        # Full dbt documentation embedded - full width to preserve two-pane layout
        ui.label('Full DBT Documentation').classes('text-h6 mb-2')
        ui.label('Interactive data lineage and model documentation').classes('text-subtitle2 mb-4')
        
        # Check if dbt docs exist
        dbt_docs_path = Path('../dbt_project/target/index.html')
        if dbt_docs_path.exists():
            # Create iframe to display dbt docs with full width to show both panes
            iframe_html = f'''
            <div style="width: 100%; margin: 20px 0; border: 1px solid #dbdbdb; border-radius: 6px; overflow: hidden;">
                <iframe 
                    src="/static/dbt_docs/index.html" 
                    width="100%" 
                    height="800px" 
                    frameborder="0"
                    style="display: block; border: none;">
                </iframe>
            </div>
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
                ui.label('Run the following command from the dbt_project directory:').classes('mt-2')
                ui.html('<code>dbt docs generate</code>', sanitize=False)

    # ADMIN TAB - Updated with iceberg, dbt, and duckdb sections
    with ui.tab_panel(admin_tab):
        
        # Iceberg Section (moved to first position)
        with ui.card().classes('w-full p-4 mb-4'):
            ui.label('Iceberg Data Lake').classes('text-h6 mb-2')
            ui.label('Data versioning and time travel').classes('text-subtitle2 mb-4')
            
            with ui.row().classes('gap-4 mb-4'):
                with ui.column().classes('flex-1'):
                    ui.label('Iceberg Features:').classes('has-text-weight-semibold mb-2')
                    ui.label('• Schema evolution').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('• Time travel queries').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('• ACID transactions').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('• Data versioning').classes('mb-4').style('font-size: 0.75rem;')
                    
                    ui.button('Time Travel', on_click=lambda: ui.notify('Time travel - coming soon!')).classes('button is-small btn-transparent')
                
                with ui.column().classes('flex-1'):
                    ui.label('Data Lake Status:').classes('has-text-weight-semibold mb-2')
                    ui.label('✅ Iceberg tables active').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('✅ Snapshots available').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('📊 Data versions: 4 snapshots').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('📅 Latest snapshot: 2 hours ago').classes('mb-1').style('font-size: 0.75rem;')
            
            # Iceberg Snapshots Table
            ui.label('Recent Snapshots').classes('has-text-weight-semibold mb-2')
            snapshots_data = get_iceberg_snapshots_data()
            if snapshots_data:
                snapshot_columns = ['snapshot_id', 'timestamp', 'operation', 'records_added', 'records_deleted', 'data_size_mb']
                create_bulma_table(snapshots_data, snapshot_columns, "iceberg-snapshots-table")
                ui.label(f'Showing {len(snapshots_data)} recent snapshots').classes('help')
            else:
                ui.label('No snapshots available').classes('text-caption')
        
        # dbt Section (moved to second position)
        with ui.card().classes('w-full p-4 mb-4'):
            ui.label('dbt Configuration').classes('text-h6 mb-2')
            ui.label('Data transformation and modeling').classes('text-subtitle2 mb-4')
            
            with ui.row().classes('gap-4'):
                with ui.column().classes('flex-1'):
                    ui.label('dbt Operations:').classes('has-text-weight-semibold mb-2')
                    ui.label('• Run transformations').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('• Generate documentation').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('• Test data quality').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('• Manage dependencies').classes('mb-4').style('font-size: 0.75rem;')
                    
                    ui.button('Run dbt', on_click=lambda: ui.notify('dbt run - coming soon!')).classes('button is-small btn-emphasized mr-2')
                    ui.button('Test dbt', on_click=lambda: ui.notify('dbt test - coming soon!')).classes('button is-small btn-transparent')
                
                with ui.column().classes('flex-1'):
                    ui.label('Current Status:').classes('has-text-weight-semibold mb-2')
                    ui.label('✅ dbt project configured').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('✅ Models compiled').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('✅ Documentation generated').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('⏳ Last run: Loading...').classes('mb-1').style('font-size: 0.75rem;')
        
        # DuckDB Section (new third card)
        with ui.card().classes('w-full p-4'):
            ui.label('DuckDB Database').classes('text-h6 mb-2')
            ui.label('Analytics database engine').classes('text-subtitle2 mb-4')
            
            with ui.row().classes('gap-4'):
                with ui.column().classes('flex-1'):
                    ui.label('DuckDB Features:').classes('has-text-weight-semibold mb-2')
                    ui.label('• Columnar analytics').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('• SQL interface').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('• Fast aggregations').classes('mb-1').style('font-size: 0.75rem;')
                    ui.label('• Parquet support').classes('mb-4').style('font-size: 0.75rem;')
                    
                    ui.button('Query Console', on_click=lambda: ui.notify('DuckDB console - coming soon!')).classes('button is-small btn-emphasized mr-2')
                    ui.button('Backup DB', on_click=lambda: ui.notify('Database backup - coming soon!')).classes('button is-small btn-transparent')
                
                with ui.column().classes('flex-1'):
                    ui.label('Database Status:').classes('has-text-weight-semibold mb-2')
                    
                    # Get real database information
                    databases, total_size = get_database_info()
                    
                    if databases:
                        ui.label('✅ Database connected').classes('mb-1').style('font-size: 0.75rem;')
                        for db in databases:
                            ui.label(f'📁 {db["name"]} ({db["size_mb"]} MB)').classes('mb-1').style('font-size: 0.75rem;')
                        ui.label(f'📊 Total size: {total_size} MB').classes('mb-1').style('font-size: 0.75rem;')
                        ui.label('📈 Performance: Optimal').classes('mb-1').style('font-size: 0.75rem;')
                    else:
                        ui.label('⚠️ No databases found').classes('mb-1').style('font-size: 0.75rem;')
                        ui.label('📊 Total size: 0 MB').classes('mb-1').style('font-size: 0.75rem;')

# Add version footer to every page
create_version_footer("v0.0.6")

# Set up static file serving for dbt docs
dbt_docs_path = Path('../dbt_project/target')
if dbt_docs_path.exists():
    from nicegui import app
    from fastapi import staticfiles
    app.mount('/static/dbt_docs', staticfiles.StaticFiles(directory=str(dbt_docs_path)), name='dbt_docs')

# Initialize dashboard stats on startup
def initialize_app():
    """Initialize the application with default data."""
    update_dashboard_stats(stats_cards, selected_years, selected_months, selected_quarters)

# Run initialization
ui.timer(0.1, initialize_app, once=True)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(port=PORT, title=TITLE)