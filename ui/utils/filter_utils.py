"""
Filter Utilities
Functions for handling UI filters and button interactions.
"""

from typing import Set, List, Callable, Optional
from nicegui import ui
from data_access import data_access
import polars as pl


def create_filter_buttons(items: List, selected_set: Set, button_list: List, on_click_fn: Callable, style_override: str = None):
    """Create a row of filter buttons without labels."""
    default_style = 'flex: 1; min-width: 0; height: 32px; font-size: 14px'
    style = style_override or default_style
    
    with ui.row().classes('gap-1 mb-2').style('max-width: 100%'):
        for item in items:
            btn = ui.button(str(item), on_click=lambda x=item: on_click_fn(x)).classes('px-2 py-1').style(style).props('color=primary')
            button_list.append(btn)


def toggle_selection(value, selected_set: Set, button_list: List, value_converter: Callable = None, 
                    update_callback: Callable = None):
    """Generic toggle function for filter selections."""
    if value is None:
        return
        
    actual_value = value_converter(value) if value_converter else value
    
    if actual_value in selected_set:
        selected_set.remove(actual_value)
    else:
        selected_set.add(actual_value)
    
    update_button_colors(button_list, selected_set, value_converter)
    if update_callback:
        update_callback()


def update_button_colors(button_list: List, selected_set: Set, value_converter: Callable = None):
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


def toggle_year(year: int, selected_years: Set, year_buttons: List, update_callback: Callable):
    """Toggle year selection."""
    toggle_selection(year, selected_years, year_buttons, update_callback=update_callback)


def toggle_month(month_name: str, selected_months: Set, month_buttons: List, update_callback: Callable):
    """Toggle month selection."""
    month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    month_num = month_names.index(month_name) + 1
    toggle_selection(month_num, selected_months, month_buttons, update_callback=update_callback)


def toggle_quarter(quarter_name: str, selected_quarters: Set, quarter_buttons: List, update_callback: Callable):
    """Toggle quarter selection."""
    quarter_num = int(quarter_name[1])  # Extract number from "Q1", "Q2", etc.
    toggle_selection(quarter_num, selected_quarters, quarter_buttons, update_callback=update_callback)


def clear_all_filters(selected_years: Set, selected_months: Set, selected_quarters: Set,
                     year_buttons: List, month_buttons: List, quarter_buttons: List,
                     update_callback: Callable):
    """Clear all filter selections."""
    selected_years.clear()
    selected_months.clear()
    selected_quarters.clear()
    
    # Update all button colors
    for btn in year_buttons + month_buttons + quarter_buttons:
        btn.props('color=primary')
    
    if update_callback:
        update_callback()