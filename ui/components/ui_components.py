"""
UI Components for Financial Data Platform
Reusable UI components with consistent styling.
"""

from typing import Dict, List
from nicegui import ui


def create_stats_cards(stats_config: List[Dict], stats_cards: Dict):
    """Create a row of statistics cards with consistent styling."""
    with ui.row().classes('w-full gap-4 mb-4'):
        for config in stats_config:
            with ui.card().classes('flex-1 p-4 text-center'):
                stats_cards[config['key']] = ui.label(config['label']).classes('text-subtitle2').style('font-size: 0.8rem;')


def create_page_header(title: str, subtitle: str = None, classes: str = 'text-h4 mb-4'):
    """Create consistent page headers."""
    ui.label(title).classes(classes)
    if subtitle:
        ui.label(subtitle).classes('text-subtitle1 mb-4')


def create_enhanced_button(text: str, style: str = "default", on_click=None, icon: str = None):
    """Create enhanced Bulma button with professional styling."""
    # Map styles to Bulma classes
    style_map = {
        "emphasized": "button is-medium btn-emphasized",
        "positive": "button is-medium btn-positive", 
        "transparent": "button is-medium btn-transparent",
        "default": "button is-medium is-primary"
    }
    
    button_class = style_map.get(style, style_map["default"])
    
    if icon:
        return ui.button(text, icon=icon, on_click=on_click).classes(button_class)
    else:
        return ui.button(text, on_click=on_click).classes(button_class)


def create_lightdash_cards(card_configs: List[Dict]):
    """Create Lightdash integration cards with enhanced buttons."""
    with ui.row().classes('gap-4 w-full'):
        for config in card_configs:
            with ui.card().classes('flex-1 p-4'):
                ui.label(config['title']).classes('text-h6 mb-3')
                create_enhanced_button('View in Lightdash', 'default', 
                                     on_click=lambda: ui.notify('Lightdash integration - coming soon!'))


def create_bulma_date_filter():
    """Create professional date filter with Bulma styling."""
    date_picker_html = '''
    <div class="field is-grouped" style="margin: 1rem 0;">
        <div class="control">
            <label class="label is-small">From Date:</label>
            <input class="input is-small" type="date" id="startDate" style="width: 150px;">
        </div>
        <div class="control">
            <label class="label is-small">To Date:</label>
            <input class="input is-small" type="date" id="endDate" style="width: 150px;">
        </div>
        <div class="control">
            <button class="button is-small btn-emphasized" id="applyDateFilter" style="margin-left: 8px; font-size: 0.7rem; padding: 4px 8px; height: 28px;">
                <span class="icon is-small">
                    <i class="fas fa-filter"></i>
                </span>
                <span>Apply Filter</span>
            </button>
        </div>
        <div class="control">
            <button class="button is-small btn-transparent" id="clearDateFilter" style="margin-left: 4px; font-size: 0.7rem; padding: 4px 8px; height: 28px;">
                <span class="icon is-small">
                    <i class="fas fa-times"></i>
                </span>
                <span>Clear</span>
            </button>
        </div>
    </div>
    '''
    
    # JavaScript part
    date_picker_js = '''
        document.addEventListener('DOMContentLoaded', function() {
            const applyBtn = document.getElementById('applyDateFilter');
            const clearBtn = document.getElementById('clearDateFilter');
            const startDate = document.getElementById('startDate');
            const endDate = document.getElementById('endDate');
            
            if (applyBtn && startDate && endDate) {
                applyBtn.addEventListener('click', function() {
                    const start = startDate.value;
                    const end = endDate.value;
                    if (start && end) {
                        // Send date filter to backend
                        fetch('/apply_date_filter', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({start_date: start, end_date: end})
                        }).then(response => response.json())
                          .then(data => {
                              if (data.status === 'success') {
                                  // Notify user and trigger refresh
                                  alert('Date filter applied: ' + start + ' to ' + end);
                                  window.location.reload(); // Refresh to show filtered data
                              }
                          });
                        console.log('Date filter applied:', start, 'to', end);
                    } else {
                        alert('Please select both start and end dates');
                    }
                });
            }
            
            if (clearBtn && startDate && endDate) {
                clearBtn.addEventListener('click', function() {
                    startDate.value = '';
                    endDate.value = '';
                    window.location.hash = 'date-filter:clear';
                    console.log('Date filter cleared');
                });
            }
        });
    '''
    
    # Add JavaScript to body
    ui.add_body_html(f'<script>{date_picker_js}</script>')
    
    # Return HTML
    return ui.html(date_picker_html, sanitize=False)