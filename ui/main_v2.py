#!/usr/bin/env python3
"""
Financial Data Platform - Enhanced UI with dbt Integration

Modern financial data analysis application with version control,
time travel, and advanced analytics powered by dbt and Iceberg.
"""

from nicegui import ui
from data_access import data_access
from data_refresh import refresh_manager
from datetime import datetime, date
from typing import List, Optional
import asyncio

# Global state
selected_years = set()
selected_months = set()
selected_quarters = set()
selected_accounts = set()
selected_version = None
current_grid = None
current_stats = {}

# Initialize data on startup
try:
    current_stats = data_access.get_dashboard_stats()
    available_versions = data_access.get_available_versions()
    print(f"✅ Loaded dashboard with {current_stats['accounts']['total']} accounts")
    print(f"📊 {len(available_versions)} data versions available")
except Exception as e:
    print(f"⚠️ Warning: Could not load initial data: {e}")
    current_stats = {"accounts": {"total": 0, "active": 0}, "transactions": {"total_transactions": 0}}
    available_versions = []

# Enhanced filter management
def create_enhanced_filter_buttons(label: str, items: List, selected_set: set, 
                                 button_list: List, on_click_fn, **kwargs):
    """Create enhanced filter buttons with better styling."""
    default_style = 'min-width: 60px; height: 36px; margin: 2px;'
    style = kwargs.get('style', default_style)
    
    with ui.row().classes('gap-2 mb-3 items-center'):
        ui.label(f'{label}:').classes('text-subtitle2 font-medium').style('min-width: 80px')
        with ui.row().classes('gap-1 flex-wrap'):
            for item in items:
                btn = ui.button(
                    str(item), 
                    on_click=lambda x=item: on_click_fn(x)
                ).classes('transition-colors duration-200').style(style)
                btn.props('color=primary outline')
                button_list.append(btn)

def update_filter_button_colors(button_list: List, selected_set: set, value_converter=None):
    """Update button colors based on selection state with enhanced styling."""
    for btn in button_list:
        if value_converter:
            button_value = value_converter(btn.text)
        else:
            button_value = int(btn.text) if btn.text.isdigit() else btn.text
        
        if button_value in selected_set:
            btn.props('color=positive unelevated')
        else:
            btn.props('color=primary outline')

def toggle_filter_selection(value, selected_set: set, button_list: List, 
                          value_converter=None, update_callback=None):
    """Enhanced toggle function with callbacks."""
    if value is None:
        return
    
    actual_value = value_converter(value) if value_converter else value
    
    if actual_value in selected_set:
        selected_set.remove(actual_value)
    else:
        selected_set.add(actual_value)
    
    update_filter_button_colors(button_list, selected_set, value_converter)
    
    if update_callback:
        update_callback()

# Version selector component
def create_version_selector():
    """Create version selector for time travel."""
    global selected_version
    
    with ui.card().classes('p-4 mb-4'):
        ui.label('🕐 Time Travel & Data Versions').classes('text-h6 mb-2')
        
        versions = data_access.get_available_versions()
        if not versions:
            ui.label('No versions available').classes('text-caption')
            return
        
        version_options = {}
        for v in versions[:10]:  # Show last 10 versions
            created_str = v['created'].strftime('%Y-%m-%d %H:%M')
            label = f"{v['file'][:30]}... ({v['size_mb']} MB, {created_str})"
            version_options[v['file']] = label
        
        version_select = ui.select(
            options=version_options,
            label='Select Data Version',
            value=None
        ).classes('w-full mb-2')
        
        def on_version_change():
            global selected_version
            selected_version = version_select.value
            if selected_version:
                ui.notify(f'Selected version: {selected_version}', type='info')
        
        version_select.on('update:model-value', on_version_change)
        
        with ui.row().classes('gap-2'):
            ui.button(
                'Use Current (dbt marts)', 
                on_click=lambda: setattr(globals(), 'selected_version', None)
            ).props('color=positive outline')
            
            ui.button(
                'Compare Versions', 
                on_click=lambda: ui.notify('Version comparison - coming soon!')
            ).props('color=info outline')

# Enhanced statistics display
def create_stats_display():
    """Create enhanced statistics display."""
    stats = current_stats
    
    with ui.row().classes('gap-4 mb-4 w-full'):
        # Accounts card
        with ui.card().classes('p-4 flex-1'):
            ui.label('👥 Accounts').classes('text-h6 text-primary')
            with ui.column().classes('gap-1'):
                ui.label(f"Total: {stats['accounts']['total']:,}").classes('text-subtitle1')
                ui.label(f"Active: {stats['accounts']['active']:,}").classes('text-body2')
                
        # Transactions card  
        with ui.card().classes('p-4 flex-1'):
            ui.label('💰 Transactions').classes('text-h6 text-primary')
            trans_stats = stats['transactions']
            with ui.column().classes('gap-1'):
                ui.label(f"Total: {trans_stats.get('total_transactions', 0):,}").classes('text-subtitle1')
                ui.label(f"Accounts: {trans_stats.get('unique_accounts', 0):,}").classes('text-body2')
        
        # Balance card
        with ui.card().classes('p-4 flex-1'):
            ui.label('📊 Balances').classes('text-h6 text-primary')
            with ui.column().classes('gap-1'):
                assets = stats['accounts'].get('assets', 0)
                liabilities = stats['accounts'].get('liabilities', 0)
                ui.label(f"Assets: €{assets:,.2f}").classes('text-subtitle1 text-positive')
                ui.label(f"Liabilities: €{liabilities:,.2f}").classes('text-body2 text-negative')

# Enhanced AG-Grid configurations
def create_account_grid_config():
    """Create enhanced account grid configuration."""
    accounts_data = data_access.get_account_summary()
    
    columns = [
        {'headerName': 'Account Code', 'field': 'account_code', 'width': 140, 'pinned': 'left'},
        {'headerName': 'Account Name', 'field': 'account_name', 'width': 250, 'pinned': 'left'},
        {'headerName': 'Transactions', 'field': 'total_transactions', 'width': 120, 'type': 'numericColumn'},
        {'headerName': 'Net Balance', 'field': 'net_balance', 'width': 150, 'type': 'numericColumn',
         'valueFormatter': {'function': 'params.value.toLocaleString("en-US", {style: "currency", currency: "EUR"})'}},
        {'headerName': 'Total Debit', 'field': 'total_debit', 'width': 150, 'type': 'numericColumn',
         'valueFormatter': {'function': 'params.value.toLocaleString("en-US", {style: "currency", currency: "EUR"})'}},
        {'headerName': 'Total Credit', 'field': 'total_credit', 'width': 150, 'type': 'numericColumn',
         'valueFormatter': {'function': 'params.value.toLocaleString("en-US", {style: "currency", currency: "EUR"})'}},
        {'headerName': 'Activity', 'field': 'activity_status', 'width': 120},
        {'headerName': 'Balance Type', 'field': 'account_balance_type', 'width': 130},
        {'headerName': 'Volume Category', 'field': 'transaction_volume_category', 'width': 140},
        {'headerName': 'Last 12M', 'field': 'net_amount_last_12_months', 'width': 150, 'type': 'numericColumn',
         'valueFormatter': {'function': 'params.value.toLocaleString("en-US", {style: "currency", currency: "EUR"})'}},
        {'headerName': 'Years Active', 'field': 'years_active', 'width': 110, 'type': 'numericColumn'},
        {'headerName': 'Last Transaction', 'field': 'last_transaction_date', 'width': 140}
    ]
    
    return {
        'columnDefs': columns,
        'rowData': accounts_data,
        'defaultColDef': {
            'resizable': True,
            'sortable': True,
            'filter': True,
            'minWidth': 80,
        },
        'theme': 'alpine-dark',
        'pagination': True,
        'paginationPageSize': 25,
        'rowSelection': 'multiple',
        'animateRows': True,
        'enableCellTextSelection': True,
        'sideBar': {
            'toolPanels': ['filters', 'columns']
        }
    }

def create_transaction_grid_config():
    """Create enhanced transaction grid configuration."""
    if selected_version:
        # Use versioned data
        transactions_data = data_access.get_data_at_version(selected_version, limit=2000)
    else:
        # Use current mart data with filters
        years_list = list(selected_years) if selected_years else None
        quarters_list = list(selected_quarters) if selected_quarters else None  
        months_list = list(selected_months) if selected_months else None
        
        transactions_data = data_access.get_filtered_transactions(
            years=years_list,
            quarters=quarters_list,
            months=months_list,
            limit=2000
        )
    
    columns = [
        {'headerName': 'Account Code', 'field': 'account_code', 'width': 140, 'pinned': 'left'},
        {'headerName': 'Account Name', 'field': 'account_name', 'width': 200},
        {'headerName': 'Date', 'field': 'transaction_date', 'width': 120, 'pinned': 'left'},
        {'headerName': 'Description', 'field': 'description', 'width': 250},
        {'headerName': 'Debit', 'field': 'debit_amount', 'width': 120, 'type': 'numericColumn',
         'valueFormatter': {'function': 'params.value.toLocaleString("en-US", {style: "currency", currency: "EUR"})'}},
        {'headerName': 'Credit', 'field': 'credit_amount', 'width': 120, 'type': 'numericColumn',
         'valueFormatter': {'function': 'params.value.toLocaleString("en-US", {style: "currency", currency: "EUR"})'}},
        {'headerName': 'Net Amount', 'field': 'net_amount', 'width': 130, 'type': 'numericColumn',
         'valueFormatter': {'function': 'params.value.toLocaleString("en-US", {style: "currency", currency: "EUR"})'}},
        {'headerName': 'Type', 'field': 'transaction_type', 'width': 100},
        {'headerName': 'Amount Category', 'field': 'amount_category', 'width': 140},
        {'headerName': 'Recency', 'field': 'recency_category', 'width': 130},
        {'headerName': 'Booking #', 'field': 'booking_number', 'width': 120},
        {'headerName': 'Source File', 'field': 'source_file', 'width': 200}
    ]
    
    return {
        'columnDefs': columns,
        'rowData': transactions_data,
        'defaultColDef': {
            'resizable': True,
            'sortable': True,
            'filter': True,
            'minWidth': 80,
        },
        'theme': 'alpine-dark',
        'pagination': True,
        'paginationPageSize': 50,
        'rowSelection': 'multiple',
        'animateRows': True,
        'enableCellTextSelection': True,
        'sideBar': {
            'toolPanels': ['filters', 'columns']
        }
    }

# Enhanced filtering and querying
def submit_enhanced_query():
    """Execute enhanced query with current filters."""
    global current_grid
    
    if current_grid is None:
        ui.notify('Grid not ready yet!', type='warning')
        return
    
    try:
        # Get filtered data
        config = create_transaction_grid_config()
        current_grid.options.update(config)
        current_grid.update()
        
        # Show notification
        filter_parts = []
        if selected_years:
            filter_parts.append(f"Years: {', '.join(map(str, sorted(selected_years)))}")
        if selected_quarters:
            filter_parts.append(f"Quarters: {', '.join([f'Q{q}' for q in sorted(selected_quarters)])}")
        if selected_months:
            filter_parts.append(f"Months: {', '.join(map(str, sorted(selected_months)))}")
        
        if filter_parts:
            filter_desc = " | ".join(filter_parts)
            ui.notify(f'✅ Filtered by: {filter_desc}', type='positive')
        else:
            ui.notify('✅ Showing all data', type='info')
            
    except Exception as e:
        ui.notify(f'❌ Error: {str(e)}', type='negative')

# Main application UI
with ui.header().classes('items-center justify-between px-6 py-3'):
    ui.label('🏦 Financial Data Platform').classes('text-h5 font-bold')
    with ui.row().classes('gap-4 items-center'):
        refresh_time = data_access.get_last_refresh_time()
        if refresh_time:
            ui.label(f'Last updated: {refresh_time.strftime("%Y-%m-%d %H:%M")}').classes('text-caption')
        
        # Enhanced refresh button with progress
        async def start_refresh():
            progress_dialog = ui.dialog()
            with progress_dialog:
                with ui.card().classes('p-6'):
                    ui.label('🔄 Refreshing Data...').classes('text-h6 mb-4')
                    progress_label = ui.label('Initializing...').classes('mb-2')
                    progress_bar = ui.linear_progress(value=0.0).classes('w-full')
                    
                    async def update_progress(message: str):
                        progress_label.text = message
                        await asyncio.sleep(0.1)  # Allow UI to update
                    
                    progress_dialog.open()
                    
                    try:
                        result = await refresh_manager.refresh_all_data(update_progress)
                        
                        progress_dialog.close()
                        
                        if result["success"]:
                            ui.notify('✅ Data refresh completed successfully!', type='positive')
                            # Reload the page to show updated data
                            ui.run_javascript('window.location.reload()')
                        else:
                            ui.notify(f'❌ Refresh failed: {result.get("error", "Unknown error")}', type='negative')
                    
                    except Exception as e:
                        progress_dialog.close()
                        ui.notify(f'❌ Error during refresh: {str(e)}', type='negative')
        
        ui.button('🔄 Refresh Data', on_click=start_refresh).props('flat')

# Main navigation tabs
with ui.tabs().classes('w-full') as main_tabs:
    dashboard_tab = ui.tab('📊 Dashboard')
    data_tab = ui.tab('📋 Data')
    analytics_tab = ui.tab('📈 Analytics')
    admin_tab = ui.tab('⚙️ Admin')

with ui.tab_panels(main_tabs, value=dashboard_tab).classes('w-full'):
    # Dashboard tab
    with ui.tab_panel(dashboard_tab):
        ui.label('Financial Dashboard').classes('text-h4 mb-4')
        
        # Statistics overview
        create_stats_display()
        
        # Quick insights
        with ui.row().classes('gap-4 w-full mb-4'):
            with ui.card().classes('p-4 flex-1'):
                ui.label('🔥 Top Accounts by Balance').classes('text-h6 mb-2')
                top_accounts = data_access.get_top_accounts_by_balance(5)
                for acc in top_accounts:
                    with ui.row().classes('justify-between w-full'):
                        ui.label(f"{acc['account_code']} - {acc['account_name'][:30]}...").classes('text-body2')
                        ui.label(f"€{acc['net_balance']:,.2f}").classes('text-body2 font-mono')
            
            with ui.card().classes('p-4 flex-1'):
                ui.label('📊 Account Activity Status').classes('text-h6 mb-2')
                activity_data = data_access.get_account_activity_breakdown()
                for activity in activity_data:
                    with ui.row().classes('justify-between w-full'):
                        ui.label(activity['activity_status']).classes('text-body2')
                        ui.label(f"{activity['account_count']} accounts").classes('text-body2')
    
    # Data tab with enhanced features
    with ui.tab_panel(data_tab):
        ui.label('Data Explorer').classes('text-h4 mb-4')
        
        # Version selector
        create_version_selector()
        
        # Sub-tabs for accounts and transactions
        with ui.tabs().classes('w-full') as data_tabs:
            accounts_tab = ui.tab('👥 Accounts')
            transactions_tab = ui.tab('💳 Transactions')
        
        with ui.tab_panels(data_tabs, value=transactions_tab).classes('w-full'):
            # Enhanced Accounts tab
            with ui.tab_panel(accounts_tab):
                ui.label('Account Analysis').classes('text-h5 mb-4')
                
                # Account analytics summary
                with ui.row().classes('gap-4 mb-4 w-full'):
                    total_accounts = current_stats['accounts']['total']
                    active_accounts = current_stats['accounts']['active']
                    
                    with ui.card().classes('p-3 text-center'):
                        ui.label(str(total_accounts)).classes('text-h4 text-primary')
                        ui.label('Total Accounts').classes('text-caption')
                    
                    with ui.card().classes('p-3 text-center'):
                        ui.label(str(active_accounts)).classes('text-h4 text-positive')
                        ui.label('Active Accounts').classes('text-caption')
                    
                    with ui.card().classes('p-3 text-center'):
                        inactive = total_accounts - active_accounts
                        ui.label(str(inactive)).classes('text-h4 text-orange')
                        ui.label('Inactive Accounts').classes('text-caption')
                
                # Enhanced accounts grid
                accounts_config = create_account_grid_config()
                ui.aggrid(accounts_config).classes('w-full').style('height: 700px')
            
            # Enhanced Transactions tab
            with ui.tab_panel(transactions_tab):
                ui.label('Transaction Analysis').classes('text-h5 mb-4')
                
                # Enhanced filter interface
                with ui.card().classes('p-4 mb-4'):
                    ui.label('🔍 Advanced Filters').classes('text-h6 mb-3')
                    
                    # Filter buttons
                    years = [2020, 2021, 2022, 2023, 2024, 2025]
                    year_buttons = []
                    create_enhanced_filter_buttons('Years', years, selected_years, year_buttons,
                                                 lambda y: toggle_filter_selection(y, selected_years, year_buttons))

                    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
                    quarter_buttons = []
                    create_enhanced_filter_buttons('Quarters', quarters, selected_quarters, quarter_buttons,
                                                 lambda q: toggle_filter_selection(q, selected_quarters, quarter_buttons,
                                                                                  lambda x: quarters.index(x) + 1))

                    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                    month_buttons = []
                    create_enhanced_filter_buttons('Months', months, selected_months, month_buttons,
                                                 lambda m: toggle_filter_selection(m, selected_months, month_buttons,
                                                                                  lambda x: months.index(x) + 1),
                                                 style='min-width: 50px; height: 32px; margin: 1px;')

                    # Action buttons
                    with ui.row().classes('gap-4 mt-4'):
                        ui.button('🔍 Apply Filters', on_click=submit_enhanced_query).props('color=positive unelevated')
                        
                        def clear_all_filters():
                            selected_years.clear()
                            selected_months.clear()
                            selected_quarters.clear()
                            update_filter_button_colors(year_buttons, selected_years)
                            update_filter_button_colors(month_buttons, selected_months, lambda x: months.index(x) + 1)
                            update_filter_button_colors(quarter_buttons, selected_quarters, lambda x: quarters.index(x) + 1)
                            ui.notify('Filters cleared', type='info')
                        
                        ui.button('🗑️ Clear All', on_click=clear_all_filters).props('color=grey outline')
                        ui.button('📊 Export Results', on_click=lambda: ui.notify('Export - coming soon!')).props('color=info outline')
                
                # Enhanced transactions grid
                transaction_config = create_transaction_grid_config()
                current_grid = ui.aggrid(transaction_config).classes('w-full').style('height: 700px')
    
    # Analytics tab (placeholder)
    with ui.tab_panel(analytics_tab):
        ui.label('Advanced Analytics').classes('text-h4 mb-4')
        ui.label('📈 Advanced analytics and reporting features coming soon!').classes('text-subtitle1')
        
        with ui.card().classes('p-4'):
            ui.label('Planned Features:').classes('text-h6 mb-2')
            with ui.column().classes('gap-1'):
                ui.label('• Monthly/Quarterly trend analysis')
                ui.label('• Account balance forecasting')
                ui.label('• Anomaly detection')
                ui.label('• Custom report builder')
                ui.label('• Data export and visualization')
    
    # Admin tab
    with ui.tab_panel(admin_tab):
        ui.label('System Administration').classes('text-h4 mb-4')
        
        with ui.row().classes('gap-4 w-full'):
            with ui.card().classes('p-4 flex-1'):
                ui.label('📊 Data Management').classes('text-h6 mb-2')
                
                async def quick_dbt_refresh():
                    ui.notify('🔄 Starting dbt refresh...', type='info')
                    try:
                        result = await refresh_manager.quick_refresh_dbt_only()
                        if result["success"]:
                            ui.notify('✅ dbt models refreshed successfully!', type='positive')
                        else:
                            ui.notify(f'❌ dbt refresh failed: {result.get("message", "Unknown error")}', type='negative')
                    except Exception as e:
                        ui.notify(f'❌ Error: {str(e)}', type='negative')
                
                with ui.column().classes('gap-2'):
                    ui.button('🔄 Quick dbt Refresh', on_click=quick_dbt_refresh).classes('w-full')
                    ui.button('📁 Import New Excel File', on_click=lambda: ui.notify('Import - coming soon!')).classes('w-full')
                    ui.button('🗂️ Manage Data Versions', on_click=lambda: ui.notify('Version management - coming soon!')).classes('w-full')
                    
                ui.separator().classes('my-3')
                
                # Refresh status
                refresh_status = refresh_manager.get_refresh_status()
                ui.label('Last Operations:').classes('text-subtitle2 mb-1')
                
                async def get_status():
                    status = await refresh_manager.get_refresh_status()
                    return status
                
                with ui.column().classes('gap-1'):
                    ui.label('• Data ingestion: Ready').classes('text-body2')
                    ui.label('• dbt models: Ready').classes('text-body2')
                    ui.label('• Quality tests: Ready').classes('text-body2')
            
            with ui.card().classes('p-4 flex-1'):
                ui.label('⚙️ System Info').classes('text-h6 mb-2')
                versions = data_access.get_available_versions()
                with ui.column().classes('gap-1'):
                    ui.label(f'Data Versions: {len(versions)}').classes('text-body2')
                    ui.label(f'Last Refresh: {data_access.get_last_refresh_time() or "Unknown"}').classes('text-body2')
                    ui.label('Status: ✅ Operational').classes('text-body2')

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(port=8081, title="Financial Data Platform")