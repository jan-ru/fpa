#!/usr/bin/env python3
"""
Financial Data Platform v0.0.3
Updated Inputs page with enhanced Excel file information and simplified grid.
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
APP_VERSION = "0.0.3"
APP_NAME = "FPA"

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
    <div style="
        position: fixed; 
        bottom: 0; 
        left: 0; 
        width: 100%; 
        background: rgba(248, 249, 250, 0.95); 
        border-top: 1px solid #e9ecef; 
        padding: 8px 16px; 
        font-size: 11px; 
        color: #6c757d; 
        text-align: center; 
        z-index: 1000;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    ">
        App: v{APP_VERSION} | Data: {data_timestamp} | Queries: {dbt_version} | {current_time}
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

def create_filter_buttons(items, selected_set, button_list, on_click_fn, style_override=None):
    """Create a row of filter buttons without labels."""
    default_style = 'flex: 1; min-width: 0; height: 32px; font-size: 14px'
    style = style_override or default_style
    
    with ui.row().classes('gap-1 mb-2').style('max-width: 100%'):
        for item in items:
            btn = ui.button(str(item), on_click=lambda x=item: on_click_fn(x)).classes('px-2 py-1').style(style).props('color=primary')
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
            btn.props('color=positive')
        else:
            btn.props('color=primary')

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
        update_dashboard_stats()
        
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

def clear_all_filters():
    """Clear all filter selections."""
    selected_years.clear()
    selected_months.clear()
    selected_quarters.clear()
    
    # Update all button colors
    for btn in year_buttons + month_buttons + quarter_buttons:
        btn.props('color=primary')
    
    update_grids_and_stats()

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

def create_dbt_model_details():
    """Create a detailed view of dbt models and their relationships."""
    lineage = get_dbt_lineage_info()
    
    if not lineage["models"]:
        return ui.label("No models to display")
    
    # Create AG-Grid with model information
    model_data = []
    for model in lineage["models"]:
        model_data.append({
            "name": model["name"],
            "type": model["type"].title(),
            "path": model["path"],
            "dependencies": ", ".join(model["dependencies"]) if model["dependencies"] else "None",
            "sources": ", ".join([f"{s[0]}.{s[1]}" for s in model["sources"]]) if model["sources"] else "None"
        })
    
    columns = [
        create_column_def('Model Name', 'name', 200),
        create_column_def('Type', 'type', 120),
        create_column_def('Path', 'path', 200),
        create_column_def('Dependencies', 'dependencies', 200),
        create_column_def('Sources', 'sources', 200),
    ]
    
    config = create_aggrid_config(columns, model_data, paginationPageSize=10)
    return ui.aggrid(config).classes('w-full').style('height: 400px')

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
ui.page_title('Financial Data Platform v0.0.3')

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
        ui.label('Data Inputs').classes('text-h4 mb-4')
        ui.label('Excel file inventory and processing status').classes('text-subtitle1 mb-4')
        
        with ui.card().classes('w-full p-4'):
            ui.label('Excel Files in Data Directory').classes('text-h6 mb-2')
            
            # Get and display Excel files with enhanced information
            excel_files = get_excel_files()
            
            if excel_files:
                # Create table of Excel files with updated columns
                file_columns = [
                    create_column_def('Filename', 'filename', 300),
                    create_column_def('Size (MB)', 'size_mb', 100, type='numericColumn'),
                    create_column_def('Columns', 'n_columns', 100, type='numericColumn'), 
                    create_column_def('Rows', 'n_rows', 100, type='numericColumn'),
                    create_column_def('Last Modified', 'modified', 150),
                    create_column_def('Status', 'status', 100),
                ]
                
                # Create AG-Grid config without filters
                file_config = create_aggrid_config(file_columns, excel_files, paginationPageSize=10)
                ui.aggrid(file_config).classes('w-full').style('height: 350px')
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
        with ui.row().classes('w-full gap-4 mb-4'):
            with ui.card().classes('flex-1 p-4 text-center'):
                stats_cards["total_accounts_card"] = ui.label('Total Accounts: Loading...').classes('text-h6')
            with ui.card().classes('flex-1 p-4 text-center'):
                stats_cards["active_accounts_card"] = ui.label('Active Accounts: Loading...').classes('text-h6')
            with ui.card().classes('flex-1 p-4 text-center'):
                stats_cards["total_assets_card"] = ui.label('Total Assets: Loading...').classes('text-h6')
            with ui.card().classes('flex-1 p-4 text-center'):
                stats_cards["total_liabilities_card"] = ui.label('Total Liabilities: Loading...').classes('text-h6')
        
        with ui.row().classes('w-full gap-4 mb-4'):
            with ui.card().classes('flex-1 p-4 text-center'):
                stats_cards["total_transactions_card"] = ui.label('Total Transactions: Loading...').classes('text-h6')
            with ui.card().classes('flex-1 p-4 text-center'):
                stats_cards["total_debit_card"] = ui.label('Total Debit: Loading...').classes('text-h6')
        
        # Filters (without labels, adjusted quarter width)
        ui.label('Filter Data').classes('text-h5 mb-2')
        
        # Years filter
        years = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
        create_filter_buttons(years, selected_years, year_buttons, toggle_year)
        
        # Quarters filter (same width as 3 months)
        quarters = ['Q1', 'Q2', 'Q3', 'Q4']
        quarter_style = 'flex: 3; min-width: 0; height: 32px; font-size: 14px'  # 3x width
        create_filter_buttons(quarters, selected_quarters, quarter_buttons, toggle_quarter, quarter_style)
        
        # Months filter
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        month_style = 'flex: 1; min-width: 0; height: 32px; font-size: 12px'
        create_filter_buttons(months, selected_months, month_buttons, toggle_month, month_style)
        
        # Action buttons
        with ui.row().classes('gap-4 mb-4'):
            ui.button('Apply Filters', on_click=update_grids_and_stats).classes('px-6 py-2').props('color=positive')
            ui.button('Clear All', on_click=clear_all_filters).classes('px-4 py-2')
        
        # Data tabs for accounts and transactions
        with ui.tabs().classes('w-full') as data_tabs:
            accounts_data_tab = ui.tab('Accounts')
            transactions_data_tab = ui.tab('Transactions')
        
        with ui.tab_panels(data_tabs, value=accounts_data_tab).classes('w-full'):
            # Accounts grid
            with ui.tab_panel(accounts_data_tab):
                ui.label('Account Summary').classes('text-h5 mb-2')
                
                # Load initial accounts data
                initial_accounts = data_access.get_account_summary()
                accounts_config = create_aggrid_config(ACCOUNT_COLUMNS, initial_accounts, paginationPageSize=25)
                accounts_grid = ui.aggrid(accounts_config).classes('w-full').style('height: 500px')
            
            # Transactions grid
            with ui.tab_panel(transactions_data_tab):
                ui.label('Transaction Details').classes('text-h5 mb-2')
                
                # Load initial transactions data
                initial_transactions = data_access.get_transaction_details(limit=1000)
                transaction_config = create_aggrid_config(
                    TRANSACTION_COLUMNS, 
                    initial_transactions,
                    rowSelection='multiple',
                    animateRows=True,
                    sideBar=True
                )
                grid = ui.aggrid(transaction_config).classes('w-full').style('height: 500px')

    # ANALYTICS TAB (No changes)
    with ui.tab_panel(analytics_tab):
        ui.label('Financial Analytics').classes('text-h4 mb-4')
        ui.label('Advanced analytics and reporting features').classes('text-subtitle1 mb-4')
        
        # Lightdash Integration Section
        with ui.card().classes('w-full p-4 mb-4'):
            ui.label('Lightdash Integration').classes('text-h6 mb-2')
            ui.label('Business Intelligence Dashboard').classes('text-subtitle2 mb-4')
            
            # Mock Lightdash interface
            with ui.row().classes('gap-4 w-full'):
                with ui.card().classes('flex-1 p-4'):
                    ui.label('📊 Balance Sheet').classes('text-h6 mb-2')
                    ui.label('Assets vs Liabilities over time').classes('mb-2')
                    ui.button('View in Lightdash', on_click=lambda: ui.notify('Lightdash integration - coming soon!'))
                
                with ui.card().classes('flex-1 p-4'):
                    ui.label('📈 Income Statement').classes('text-h6 mb-2')
                    ui.label('Revenue and expense analysis').classes('mb-2')
                    ui.button('View in Lightdash', on_click=lambda: ui.notify('Lightdash integration - coming soon!'))
                
                with ui.card().classes('flex-1 p-4'):
                    ui.label('💰 Cash Flow Statement').classes('text-h6 mb-2')
                    ui.label('Cash flow trends and projections').classes('mb-2')
                    ui.button('View in Lightdash', on_click=lambda: ui.notify('Lightdash integration - coming soon!'))
                
                with ui.card().classes('flex-1 p-4'):
                    ui.label('📊 Metrics Over Time').classes('text-h6 mb-2')
                    ui.label('Key performance indicators').classes('mb-2')
                    ui.button('View in Lightdash', on_click=lambda: ui.notify('Lightdash integration - coming soon!'))
        
        # Plotly Charts Section
        with ui.card().classes('w-full p-4'):
            ui.label('Interactive Charts (Plotly)').classes('text-h6 mb-4')
            
            # Create sample Plotly charts
            create_plotly_sample()

    # LINEAGE TAB (No changes)
    with ui.tab_panel(lineage_tab):
        ui.label('Data Lineage').classes('text-h4 mb-4')
        ui.label('Understand data flow and dependencies').classes('text-subtitle1 mb-4')
        
        # Visual lineage diagram
        with ui.card().classes('w-full p-4 mb-4'):
            ui.label('DBT Model Lineage Visualization').classes('text-h6 mb-2')
            create_lineage_visualization()
        
        # Model details table  
        with ui.card().classes('w-full p-4 mb-4'):
            ui.label('Model Details & Dependencies').classes('text-h6 mb-2')
            create_dbt_model_details()
        
        # Generate full dbt docs option
        with ui.card().classes('w-full p-4'):
            ui.label('Full DBT Documentation').classes('text-h6 mb-2')
            ui.label('For complete lineage exploration with interactive graphs and detailed column lineage:').classes('mb-2')
            
            with ui.row().classes('gap-4'):
                ui.button('Generate DBT Docs', 
                         on_click=lambda: generate_and_embed_dbt_docs()).classes('px-6 py-2').props('color=positive')
                ui.button('Open DBT Docs (External)', 
                         on_click=lambda: open_dbt_docs_external()).classes('px-6 py-2')
            
            ui.label('The external docs provide the full interactive DAG, column-level lineage, and model documentation.').classes('text-caption mt-2')

    # ADMIN TAB (No changes)
    with ui.tab_panel(admin_tab):
        ui.label('Administration').classes('text-h4 mb-4')
        ui.label('System administration and configuration').classes('text-subtitle1 mb-4')
        
        with ui.card().classes('w-full p-4'):
            ui.label('No changes for now').classes('text-subtitle2')
            ui.label('Future admin features will be added here').classes('text-caption')

# Add version footer to every page
create_version_footer()

# Initialize dashboard stats on startup
def initialize_app():
    """Initialize the application with default data."""
    update_dashboard_stats()

# Run initialization
ui.timer(0.1, initialize_app, once=True)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(port=8089, title='Financial Data Platform v0.0.3')