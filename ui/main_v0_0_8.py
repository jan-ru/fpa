#!/usr/bin/env python3
"""
Financial Data Platform v0.0.7
Refactored version with modular architecture, centralized configuration,
error handling, and reusable components.
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
from components.tab_manager import create_standard_tabs
from components.chart_components import create_financial_charts, create_plotly_sample
from services.data_factory import data_factory
from utils.dbt_utils import get_dbt_run_status, get_all_dbt_command_status
from components.lazy_loader import (
    lazy_loader, setup_lazy_loaders, create_lazy_data_card,
    create_data_refresh_button
)
from utils.error_boundaries import create_system_status_card
from utils.source_filter import (
    source_filter, create_filter_status_indicator, add_selection_javascript
)
from components import (
    create_stats_cards,
    create_page_header,
    create_lightdash_cards,
    create_enhanced_button,
    create_bulma_date_filter,
    create_bulma_table
)
from components.table_components import create_paginated_table
from components.pagination import create_page_size_selector
from services import (
    get_sorted_accounts,
    get_limited_transactions,
    get_excel_files_data,
    get_dbt_models_data
)
from services.data_service import get_accounts_paginated, get_transactions_paginated
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

# Import new modular components and configuration
from config.constants import (
    APP_VERSION, APP_NAME, APP_TITLE, DEFAULT_PORT,
    UIConfig, DataConfig, Paths, Icons, ErrorMessages, StatusMessages, Styles, Features,
    get_table_config, get_button_style
)
from utils.error_handling import (
    safe_data_fetch, safe_ui_operation, ErrorContext,
    validate_file_exists, validate_data_not_empty
)
from utils.async_loading import (
    async_data_loader, data_loader, LoadingState, create_loading_placeholder
)
from utils.caching import (
    smart_cache, cached, query_cache
)
from utils.state_management import (
    filter_manager, state_manager, ui_state_manager, data_state_manager,
    create_reactive_filter_buttons, get_current_filter_state, subscribe_to_filter_changes
)
from utils.validation import (
    DataValidator, FinancialDataValidator, FilterValidator, validate_data_batch
)
from components.cards import (
    create_admin_card, create_data_card, create_metrics_card, create_integration_card
)

# Legacy imports for compatibility
from config import (
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
    # Get current filter state
    filter_state = get_current_filter_state()
    
    # Get filtered data with validation
    years = list(filter_state.years) if filter_state.years else None
    quarters = list(filter_state.quarters) if filter_state.quarters else None
    months = list(filter_state.months) if filter_state.months else None
    
    # Validate filter inputs
    if years:
        is_valid, validated_years, error = FilterValidator.validate_year_filter(years)
        if not is_valid:
            ui.notify(f"Invalid year filter: {error}", type='negative')
            return
        years = validated_years
    
    if months:
        is_valid, validated_months, error = FilterValidator.validate_month_filter(months)
        if not is_valid:
            ui.notify(f"Invalid month filter: {error}", type='negative')
            return
        months = validated_months
    
    if quarters:
        is_valid, validated_quarters, error = FilterValidator.validate_quarter_filter(quarters)
        if not is_valid:
            ui.notify(f"Invalid quarter filter: {error}", type='negative')
            return
        quarters = validated_quarters
    
    # Get filtered data
    filtered_transactions = data_access.get_filtered_transactions(
        years=years,
        quarters=quarters,
        months=months,
        limit=DataConfig.DEFAULT_TRANSACTION_LIMIT
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



@cached('iceberg_snapshots', ttl=300)  # Cache for 5 minutes
def get_iceberg_snapshots_data():
    """Get real Iceberg snapshots data from parquet files and ingestion log."""
    snapshots = []
    
    if not validate_file_exists(str(Paths.ICEBERG_LOG), "Iceberg log"):
        return snapshots
    
    try:
        # Read the ingestion log
        with open(Paths.ICEBERG_LOG, 'r') as f:
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
                    parquet_path = Paths.ICEBERG_WAREHOUSE / parquet_file
                    file_size_mb = 0
                    if parquet_path.exists():
                        file_size_mb = round(parquet_path.stat().st_size / (1024*1024), 1)
                    
                    # Create snapshot ID from filename
                    snapshot_id = parquet_file.replace('financial_transactions_', '').replace('.parquet', '')
                    
                    # Format timestamp for display
                    try:
                        parsed_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        display_timestamp = parsed_time.strftime(DataConfig.DISPLAY_DATE_FORMAT)
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
        main_iceberg = Paths.ICEBERG_WAREHOUSE / "financial_transactions_iceberg.parquet"
        if main_iceberg.exists():
            file_size_mb = round(main_iceberg.stat().st_size / (1024*1024), 1)
            snapshots.append({
                "snapshot_id": "iceberg_main",
                "timestamp": datetime.fromtimestamp(main_iceberg.stat().st_mtime).strftime(DataConfig.DISPLAY_DATE_FORMAT),
                "operation": "consolidate",
                "records_added": sum(s["records_added"] for s in snapshots),
                "records_deleted": 0,
                "data_size_mb": file_size_mb,
                "source_file": "consolidated"
            })
    
    except Exception as e:
        ui.notify(f"Error reading Iceberg data: {e}", type='negative')
        return []
    
    # Sort by timestamp (newest first)
    snapshots.sort(key=lambda x: x["timestamp"], reverse=True)
    return snapshots


@cached('database_info', ttl=600)  # Cache for 10 minutes
def get_database_info():
    """Get real database information."""
    databases = []
    total_size_mb = 0
    
    try:
        # Check for DuckDB files in data/warehouse
        if Paths.DATA_WAREHOUSE.exists():
            for db_file in Paths.DATA_WAREHOUSE.glob("*.db"):
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
        ui.notify(f"Error reading database info: {e}", type='negative')
    
    return databases, total_size_mb


@cached('dbt_lineage', ttl=600)  # Cache for 10 minutes
def get_dbt_lineage_info():
    """Get dbt lineage information from the project files."""
    lineage_info = {
        "models": [],
        "dependencies": [],
        "sources": []
    }
    
    if not Paths.DBT_PROJECT.exists():
        return lineage_info
    
    try:
        # Get models from the models directory
        models_dir = Paths.DBT_PROJECT / "models"
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
        ui.notify(f"Error reading dbt project: {str(e)}", type='negative')
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
ui.page_title(APP_TITLE)

# Setup Bulma CSS and FontAwesome with enhanced button styling
ui.add_head_html(f'''
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.9.4/css/bulma.min.css">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <style>
        /* Enhanced Bulma buttons with professional styling */
        .btn-emphasized {{
            background: linear-gradient(135deg, {UIConfig.PRIMARY_COLOR} 0%, #2366d1 100%);
            border: none;
            color: white !important;
            font-weight: 600;
            box-shadow: 0 2px 4px rgba(50, 115, 220, 0.2);
            transition: all 0.2s ease;
        }}
        .btn-emphasized:hover {{
            transform: translateY(-1px);
            box-shadow: 0 4px 8px rgba(50, 115, 220, 0.3);
        }}
        .btn-positive {{
            background: linear-gradient(135deg, {UIConfig.SUCCESS_COLOR} 0%, #3ec46d 100%);
            border: none;
            color: white !important;
            font-weight: 600;
            box-shadow: 0 2px 4px rgba(72, 199, 116, 0.2);
        }}
        .btn-transparent {{
            background: transparent;
            border: 1px solid #dbdbdb;
            color: #363636 !important;
            font-weight: 500;
        }}
        .btn-transparent:hover {{
            background: #f5f5f5;
            border-color: #b5b5b5;
        }}
        
        /* Smaller button styles */
        .btn-small {{
            font-size: {UIConfig.BUTTON_SMALL_FONT_SIZE} !important;
            padding: {UIConfig.BUTTON_SMALL_PADDING} !important;
            height: auto !important;
            min-height: {UIConfig.BUTTON_SMALL_HEIGHT} !important;
        }}
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
        # Source file filter status indicator
        create_filter_status_indicator()
        
        # Lazy-loaded Excel files data with selection checkboxes
        file_columns = ['filename', 'size_mb', 'n_columns', 'n_rows', 'total_debit', 'total_credit', 'modified', 'status', 'source']
        create_lazy_data_card(
            title='Excel Files in Data Directory',
            subtitle='Select files to filter data across all tabs',
            data_key='excel_files',
            table_columns=file_columns,
            table_id="excel-files-table",
            show_selection=True
        )
        
        # Add refresh button and selection controls
        with ui.row().classes('gap-2'):
            create_data_refresh_button(['excel_files'], 'Refresh File Inventory')
            ui.button('Select All', on_click=lambda: ui.run_javascript('selectAllFiles()')).classes('bg-blue-500 text-white')
            ui.button('Select None', on_click=lambda: ui.run_javascript('selectNoFiles()')).classes('bg-gray-500 text-white')
        
        # Add selection handling JavaScript (will be loaded when tab is accessed)
        ui.add_body_html('''
        <script>
        function updateSourceFilter() {
            const checkboxes = document.querySelectorAll('.row-selector');
            const checkedBoxes = document.querySelectorAll('.row-selector:checked');
            const selectedFiles = Array.from(checkedBoxes).map(cb => cb.dataset.key);
            
            fetch('/update_source_filter', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({selectedFiles: selectedFiles})
            });
        }
        
        function selectAllFiles() {
            document.querySelectorAll('.row-selector').forEach(cb => cb.checked = true);
            updateSourceFilter();
        }
        
        function selectNoFiles() {
            document.querySelectorAll('.row-selector').forEach(cb => cb.checked = false);
            updateSourceFilter();
        }
        
        // Add event listeners when page loads
        document.addEventListener('DOMContentLoaded', function() {
            document.addEventListener('change', function(e) {
                if (e.target.classList.contains('row-selector')) {
                    updateSourceFilter();
                }
            });
        });
        </script>
        ''')

    # DASHBOARD TAB
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
        
        # Date Range Filter
        with ui.element('div').classes('box mb-4'):
            ui.label('Filter Data').classes('title is-5 mb-3')
            ui.label('Use the date range picker to filter transactions by date period').classes('subtitle is-6 has-text-grey mb-3')
            
            # Add professional date filter
            create_bulma_date_filter()
            
            # Quick filter buttons with reactive state management
            ui.label('Quick Filters:').classes('label mt-4')
            with ui.element('div').classes('field is-grouped'):
                years = [2020, 2021, 2022, 2023, 2024, 2025, 2026]
                year_buttons = create_reactive_filter_buttons(years, 'year', callback=update_grids_and_stats)
            
            with ui.element('div').classes('field is-grouped mt-2'):
                def apply_filters_and_refresh():
                    """Apply filters and refresh lazy-loaded data."""
                    update_grids_and_stats()
                    # Clear and reload lazy data to reflect filters
                    from components.lazy_loader import lazy_loader
                    lazy_loader.clear_cache('accounts')
                    lazy_loader.clear_cache('transactions')
                    ui.notify('Filters applied and data refreshed', type='positive')
                
                def clear_filters_and_refresh():
                    """Clear all filters and refresh data."""
                    filter_manager.clear_all_filters()
                    from components.lazy_loader import lazy_loader
                    lazy_loader.clear_cache('accounts')
                    lazy_loader.clear_cache('transactions')
                    ui.notify('Filters cleared and data refreshed', type='positive')
                
                ui.button('Apply Filters', on_click=apply_filters_and_refresh).classes(Styles.EMPHASIZED_BUTTON_CLASSES).style(get_button_style('compact'))
                ui.button('Clear All', on_click=clear_filters_and_refresh).classes(Styles.TRANSPARENT_BUTTON_CLASSES + ' ml-2').style(get_button_style('compact'))
        
        # Data tabs for accounts and transactions
        with ui.tabs().classes('w-full') as data_tabs:
            accounts_data_tab = ui.tab('Accounts')
            transactions_data_tab = ui.tab('Transactions')
        
        with ui.tab_panels(data_tabs, value=accounts_data_tab).classes('w-full'):
            # Accounts grid with pagination
            with ui.tab_panel(accounts_data_tab):
                account_columns = ['account_code', 'account_name', 'total_transactions', 'total_debit', 'total_credit', 'net_balance']
                
                # Create paginated table
                create_paginated_table(
                    data_func=get_accounts_paginated,
                    columns=account_columns,
                    table_id="accounts-table",
                    page_size=20,
                    title='Account Summary',
                    subtitle='Overview of all accounts with balances and activity (Source: mart_account_summary)'
                )
                
                # Add refresh button
                create_data_refresh_button(['accounts'], 'Refresh Accounts')
            
            # Transactions grid with pagination
            with ui.tab_panel(transactions_data_tab):
                transaction_columns = ['account_code', 'account_name', 'transaction_date', 'description', 'debit_amount', 'credit_amount', 'net_amount']
                
                # Create paginated table
                create_paginated_table(
                    data_func=get_transactions_paginated,
                    columns=transaction_columns,
                    table_id="transactions-table",
                    page_size=20,
                    title='Transaction Details',
                    subtitle='Detailed view of financial transactions (Source: mart_transaction_details)'
                )
                
                # Add refresh button
                create_data_refresh_button(['transactions'], 'Refresh Transactions')

    # ANALYTICS TAB
    with ui.tab_panel(analytics_tab):
        
        # Lightdash Integration using IntegrationCard
        lightdash_items = [
            {'title': '📊 Balance Sheet'},
            {'title': '📈 Income Statement'}, 
            {'title': '💰 Cash Flow Statement'},
            {'title': '📊 Metrics Over Time'}
        ]
        
        create_integration_card(
            title='Lightdash Integration',
            subtitle='Business Intelligence Dashboard',
            integration_items=lightdash_items,
            button_text='View in Lightdash',
            button_action=lambda: ui.notify(StatusMessages.FEATURE_COMING_SOON)
        )
        
        # Plotly Charts Section
        with ui.card().classes(Styles.CARD_CLASSES):
            ui.label('Interactive Charts (Plotly)').classes(Styles.CARD_TITLE_CLASSES)
            create_plotly_sample()

    # LINEAGE TAB
    with ui.tab_panel(lineage_tab):
        
        # Model details table using DataCard
        model_data = get_dbt_models_data()
        if validate_data_not_empty(model_data, "DBT models"):
            model_columns = ['name', 'type', 'path', 'dependencies', 'sources']
            create_data_card(
                title='Model Details & Dependencies',
                subtitle='DBT model configuration and relationships',
                data_func=lambda: model_data,
                table_columns=model_columns,
                table_id="dbt-models-table"
            )
        
        # Full dbt documentation embedded
        with ui.card().classes(Styles.CARD_CLASSES):
            ui.label('Full DBT Documentation').classes(Styles.CARD_TITLE_CLASSES)
            ui.label('Interactive data lineage and model documentation').classes(Styles.CARD_SUBTITLE_CLASSES)
            
            # Check if dbt docs exist
            if validate_file_exists(str(Paths.DBT_DOCS), "DBT documentation"):
                # Create iframe to display dbt docs
                iframe_html = f'''
                <div style="width: 100%; margin: 10px 0; border: 1px solid #dbdbdb; border-radius: 6px; overflow-x: auto; overflow-y: hidden;">
                    <iframe 
                        src="/static/dbt_docs/index.html" 
                        width="{UIConfig.IFRAME_WIDTH}" 
                        height="{UIConfig.IFRAME_HEIGHT}" 
                        frameborder="0"
                        style="display: block; border: none; min-width: {UIConfig.IFRAME_MIN_WIDTH};">
                    </iframe>
                </div>
                '''
                ui.html(iframe_html, sanitize=False)
                
            else:
                with ui.element('div').classes('notification is-warning'):
                    ui.label('dbt documentation not found. Please run "dbt docs generate" first.').classes('has-text-weight-semibold')
                    ui.label('Run the following command from the dbt_project directory:').classes('mt-2')
                    ui.html('<code>dbt docs generate</code>', sanitize=False)

    # ADMIN TAB - Using new card components
    with ui.tab_panel(admin_tab):
        
        # Iceberg Section using AdminCard
        iceberg_features = [
            'Schema evolution',
            'Time travel queries', 
            'ACID transactions',
            'Data versioning'
        ]
        
        # Get real Iceberg status
        snapshots_data = get_iceberg_snapshots_data()
        snapshot_count = len(snapshots_data) if snapshots_data else 0
        
        # Calculate time since latest snapshot
        latest_time = "Never"
        if snapshots_data:
            try:
                from datetime import datetime
                latest_snapshot = snapshots_data[0]  # First item is most recent
                timestamp_str = latest_snapshot.get('timestamp', '')
                if timestamp_str:
                    # Parse timestamp and calculate time difference
                    snapshot_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    current_time = datetime.now(snapshot_time.tzinfo)
                    time_diff = current_time - snapshot_time
                    
                    if time_diff.days > 0:
                        latest_time = f"{time_diff.days} days ago"
                    elif time_diff.seconds > 3600:
                        hours = time_diff.seconds // 3600
                        latest_time = f"{hours} hours ago"
                    elif time_diff.seconds > 60:
                        minutes = time_diff.seconds // 60
                        latest_time = f"{minutes} minutes ago"
                    else:
                        latest_time = "Just now"
            except Exception:
                latest_time = "Unknown"
        
        iceberg_status = [
            {'icon': Icons.SUCCESS if snapshot_count > 0 else Icons.WARNING, 'text': 'Iceberg tables active' if snapshot_count > 0 else 'No Iceberg tables'},
            {'icon': Icons.SUCCESS if snapshot_count > 0 else Icons.WARNING, 'text': 'Snapshots available' if snapshot_count > 0 else 'No snapshots found'},
            {'icon': Icons.INFO, 'text': f'Data versions: {snapshot_count} snapshots'},
            {'icon': Icons.INFO, 'text': f'Latest snapshot: {latest_time}'}
        ]
        
        iceberg_buttons = [
            {'text': 'Time Travel', 'on_click': lambda: ui.notify(StatusMessages.FEATURE_COMING_SOON), 'type': 'transparent'}
        ]
        
        iceberg_card = create_admin_card(
            title='Data Lake',
            subtitle='',
            features=iceberg_features,
            status_items=iceberg_status,
            buttons=iceberg_buttons,
            tooltip='Iceberg data versioning and time travel'
        )
        
        # Add snapshots table to the iceberg card
        with iceberg_card:
            ui.label('Recent Snapshots').classes(Styles.SECTION_TITLE_CLASSES)
            snapshots_data = get_iceberg_snapshots_data()
            if validate_data_not_empty(snapshots_data, "Iceberg snapshots"):
                snapshot_columns = ['snapshot_id', 'timestamp', 'operation', 'records_added', 'records_deleted', 'data_size_mb', 'source_file']
                create_bulma_table(snapshots_data, snapshot_columns, "iceberg-snapshots-table")
                ui.label(f'Showing {len(snapshots_data)} recent snapshots').classes(Styles.HELP_TEXT_CLASSES)
        
        # dbt Section using AdminCard
        dbt_features = [
            'Run transformations',
            'Generate documentation',
            'Test data quality',
            'Manage dependencies'
        ]
        
        # Get real dbt status for all commands
        all_dbt_status = get_all_dbt_command_status()
        
        dbt_status = [
            {'icon': Icons.SUCCESS, 'text': 'dbt project configured'},
            {'icon': Icons.SUCCESS, 'text': 'Models compiled'},
            {'icon': Icons.SUCCESS, 'text': 'Documentation generated'},
            {'icon': all_dbt_status['debug']['icon'], 'text': all_dbt_status['debug']['last_run']},
            {'icon': all_dbt_status['run']['icon'], 'text': all_dbt_status['run']['last_run']},
            {'icon': all_dbt_status['test']['icon'], 'text': all_dbt_status['test']['last_run']}
        ]
        
        dbt_buttons = [
            {'text': 'dbt debug', 'on_click': lambda: ui.notify(StatusMessages.FEATURE_COMING_SOON), 'type': 'transparent'},
            {'text': 'dbt run', 'on_click': lambda: ui.notify(StatusMessages.FEATURE_COMING_SOON), 'type': 'emphasized'},
            {'text': 'dbt test', 'on_click': lambda: ui.notify(StatusMessages.FEATURE_COMING_SOON), 'type': 'transparent'}
        ]
        
        create_admin_card(
            title='Data Build Tool',
            subtitle='',
            features=dbt_features,
            status_items=dbt_status,
            buttons=dbt_buttons,
            tooltip='Data transformation and modeling'
        )
        
        # DuckDB Section using AdminCard with database info
        duckdb_features = [
            'Columnar analytics',
            'SQL interface',
            'Fast aggregations',
            'Parquet support'
        ]
        
        # Get real database information
        databases, total_size = get_database_info()
        
        duckdb_status = []
        if databases:
            duckdb_status.append({'icon': Icons.SUCCESS, 'text': 'Database connected'})
            for db in databases:
                duckdb_status.append({'icon': Icons.DATABASE, 'text': f'{db["name"]} ({db["size_mb"]} MB)'})
            duckdb_status.append({'icon': Icons.INFO, 'text': f'Total size: {total_size} MB'})
            duckdb_status.append({'icon': Icons.CHART, 'text': 'Performance: Optimal'})
        else:
            duckdb_status.extend([
                {'icon': Icons.WARNING, 'text': 'No databases found'},
                {'icon': Icons.INFO, 'text': 'Total size: 0 MB'}
            ])
        
        duckdb_buttons = [
            {'text': 'Query Console', 'on_click': lambda: ui.notify(StatusMessages.FEATURE_COMING_SOON), 'type': 'emphasized'},
            {'text': 'Backup DB', 'on_click': lambda: ui.notify(StatusMessages.FEATURE_COMING_SOON), 'type': 'transparent'}
        ]
        
        create_admin_card(
            title='Database',
            subtitle='',
            features=duckdb_features,
            status_items=duckdb_status,
            buttons=duckdb_buttons,
            tooltip='DuckDB analytics database engine'
        )
        
        # Cache Management Section
        cache_features = [
            'Memory cache for fast access',
            'Persistent cache for sessions',
            'Automatic cache invalidation',
            'Performance optimization'
        ]
        
        # Get cache statistics
        cache_stats = smart_cache.get_stats()
        memory_stats = cache_stats.get('memory', {})
        
        cache_status = [
            {'icon': Icons.INFO, 'text': f'Memory cache: {memory_stats.get("size", 0)} entries'},
            {'icon': Icons.INFO, 'text': f'Hit rate: {memory_stats.get("hit_rate", 0):.1%}'},
            {'icon': Icons.INFO, 'text': f'Cache hits: {memory_stats.get("hits", 0)}'},
            {'icon': Icons.INFO, 'text': f'Cache misses: {memory_stats.get("misses", 0)}'}
        ]
        
        def clear_cache_and_refresh():
            """Clear cache and refresh the display."""
            smart_cache.invalidate()
            ui.notify('Cache cleared successfully', type='positive')
            # Force page refresh to show updated stats
            ui.run_javascript('window.location.reload()')
        
        def show_cache_details():
            """Show detailed cache statistics."""
            current_stats = smart_cache.get_stats()
            stats_text = f"""
Memory Cache: {current_stats.get('memory', {}).get('size', 0)} entries
Hit Rate: {current_stats.get('memory', {}).get('hit_rate', 0):.1%}
Cache Hits: {current_stats.get('memory', {}).get('hits', 0)}
Cache Misses: {current_stats.get('memory', {}).get('misses', 0)}
Total Cache Size: {current_stats.get('persistent', {}).get('size_mb', 0):.1f} MB
            """.strip()
            ui.notify(stats_text, type='info', timeout=10000)
        
        cache_buttons = [
            {'text': 'Clear Cache', 'on_click': clear_cache_and_refresh, 'type': 'transparent'},
            {'text': 'Cache Stats', 'on_click': show_cache_details, 'type': 'transparent'}
        ]
        
        create_admin_card(
            title='Cache Management',
            subtitle='',
            features=cache_features,
            status_items=cache_status,
            buttons=cache_buttons
        )

# Add version footer to every page
create_version_footer(APP_VERSION)

# Set up static file serving for dbt docs
if Paths.DBT_TARGET.exists():
    from nicegui import app
    from fastapi import staticfiles
    app.mount('/static/dbt_docs', staticfiles.StaticFiles(directory=str(Paths.DBT_TARGET)), name='dbt_docs')

# Add endpoint for source filter updates
from nicegui import app
from fastapi import Request
import json

@app.post('/update_source_filter')
async def update_source_filter_endpoint(request: Request):
    """Handle source filter updates from JavaScript."""
    try:
        data = await request.json()
        selected_files = data.get('selectedFiles', [])
        source_filter.select_files(selected_files)
        return {'status': 'success', 'selected_count': len(selected_files)}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

@app.post('/apply_date_filter')
async def apply_date_filter_endpoint(request: Request):
    """Handle date filter application from JavaScript."""
    try:
        data = await request.json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        # Convert dates to years for the filter system
        from datetime import datetime
        start_year = datetime.strptime(start_date, '%Y-%m-%d').year
        end_year = datetime.strptime(end_date, '%Y-%m-%d').year
        
        # Add years to filter state
        years_to_filter = list(range(start_year, end_year + 1))
        for year in years_to_filter:
            filter_manager.add_filter('year', year)
        
        # Clear cache to refresh data
        from components.lazy_loader import lazy_loader
        lazy_loader.clear_cache('accounts')
        lazy_loader.clear_cache('transactions')
        
        return {
            'status': 'success', 
            'start_date': start_date, 
            'end_date': end_date,
            'years_filtered': years_to_filter
        }
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

# Initialize dashboard stats on startup
@safe_ui_operation
def initialize_app():
    """Initialize the application with default data."""
    update_dashboard_stats(stats_cards, selected_years, selected_months, selected_quarters)

# Initialize lazy loading system
setup_lazy_loaders()

# Run initialization
ui.timer(0.1, initialize_app, once=True)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(port=DEFAULT_PORT, title=APP_TITLE)