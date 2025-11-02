"""
Reusable card components for the FPA application.
Provides consistent card layouts and reduces code duplication.
"""

from typing import List, Dict, Optional, Callable, Any
from nicegui import ui

from config.constants import Styles, UIConfig, ErrorMessages, get_button_style
from utils.error_handling import safe_ui_operation


class BaseCard:
    """Base class for all card components."""
    
    def __init__(self, title: str, subtitle: Optional[str] = None, tooltip: Optional[str] = None):
        self.title = title
        self.subtitle = subtitle
        self.tooltip = tooltip
        self.card = None
    
    def create(self) -> ui.card:
        """Create the card with title and subtitle."""
        self.card = ui.card().classes(Styles.CARD_CLASSES)
        with self.card:
            title_element = ui.label(self.title).classes(Styles.CARD_TITLE_CLASSES)
            if self.tooltip:
                title_element.tooltip(self.tooltip)
            if self.subtitle:
                ui.label(self.subtitle).classes(Styles.CARD_SUBTITLE_CLASSES)
        return self.card


class AdminCard(BaseCard):
    """Standardized admin card with features, status, and buttons."""
    
    def __init__(
        self, 
        title: str, 
        subtitle: str,
        features: List[str],
        status_items: List[Dict[str, str]],
        buttons: List[Dict[str, Any]],
        tooltip: Optional[str] = None
    ):
        super().__init__(title, subtitle, tooltip)
        self.features = features
        self.status_items = status_items
        self.buttons = buttons
    
    def create(self) -> ui.card:
        """Create admin card with standardized layout."""
        super().create()
        
        with self.card:
            # Only show status and buttons
            self._create_status_section()
            self._create_buttons_section()
        
        return self.card
    
    def _create_features_section(self):
        """Create the features list section."""
        ui.label('Features:').classes(Styles.SECTION_TITLE_CLASSES)
        for feature in self.features:
            ui.label(f'• {feature}').style(f'{Styles.SMALL_TEXT_STYLE} line-height: 1.2; margin-bottom: 0.25rem;')
        ui.html('<div style="margin-bottom: 0.75rem;"></div>', sanitize=False)  # Reduced spacing
    
    def _create_status_section(self):
        """Create the status items section."""
        ui.label('Status:').classes(Styles.SECTION_TITLE_CLASSES)
        for item in self.status_items:
            icon = item.get('icon', '')
            text = item.get('text', '')
            with ui.row().classes('gap-2 items-start mb-0.5 w-full'):
                if icon:
                    # Apply colors based on icon type
                    icon_color = ''
                    if 'fa-check-circle' in icon:
                        icon_color = 'color: #28a745;'  # Green for success
                    elif 'fa-exclamation-triangle' in icon:
                        icon_color = 'color: #ffc107;'  # Yellow/orange for warning
                    elif 'fa-times-circle' in icon:
                        icon_color = 'color: #dc3545;'  # Red for error
                    elif 'fa-info-circle' in icon:
                        icon_color = 'color: #17a2b8;'  # Blue for info
                    
                    ui.html(f'<i class="{icon}"></i>', sanitize=False).style(f'font-size: 0.75rem; flex-shrink: 0; margin-top: 2px; {icon_color}')
                ui.label(text).style(f'{Styles.SMALL_TEXT_STYLE} word-wrap: break-word; line-height: 1.3;').classes('flex-grow')
    
    def _create_buttons_section(self):
        """Create the buttons section."""
        if not self.buttons:
            return
            
        with ui.row().classes('gap-2'):
            for button_config in self.buttons:
                self._create_button(button_config)
    
    @safe_ui_operation
    def _create_button(self, config: Dict[str, Any]):
        """Create a single button from configuration."""
        text = config.get('text', 'Button')
        on_click = config.get('on_click', lambda: ui.notify('Not implemented'))
        button_type = config.get('type', 'emphasized')
        
        button_class = Styles.EMPHASIZED_BUTTON_CLASSES if button_type == 'emphasized' else Styles.TRANSPARENT_BUTTON_CLASSES
        style = get_button_style('small')
        
        if 'icon' in config:
            ui.button(text, icon=config['icon'], on_click=on_click).classes(button_class).style(style)
        else:
            ui.button(text, on_click=on_click).classes(button_class).style(style)


class DataCard(BaseCard):
    """Card for displaying data tables."""
    
    def __init__(
        self, 
        title: str, 
        subtitle: str,
        data_func: Callable,
        table_columns: List[str],
        table_id: str,
        show_count: bool = True,
        empty_message: str = ErrorMessages.NO_DATA_AVAILABLE,
        show_selection: bool = False
    ):
        super().__init__(title, subtitle)
        self.data_func = data_func
        self.table_columns = table_columns
        self.table_id = table_id
        self.show_count = show_count
        self.empty_message = empty_message
        self.show_selection = show_selection
    
    def create(self) -> ui.card:
        """Create data card with table."""
        super().create()
        
        with self.card:
            self._create_data_table()
        
        return self.card
    
    @safe_ui_operation
    def _create_data_table(self):
        """Create the data table with error handling."""
        from components.table_components import create_bulma_table
        
        try:
            data = self.data_func()
            
            if data:
                create_bulma_table(data, self.table_columns, self.table_id, self.show_selection)
                if self.show_count:
                    ui.label(f'Showing {len(data)} items').classes(Styles.HELP_TEXT_CLASSES)
            else:
                ui.html(f'<p class="has-text-grey">{self.empty_message}</p>', sanitize=False)
                
        except Exception as e:
            ui.html(f'<p class="has-text-danger">Error loading data: {str(e)}</p>', sanitize=False)


class MetricsCard(BaseCard):
    """Card for displaying key metrics and statistics."""
    
    def __init__(
        self, 
        title: str,
        metrics: List[Dict[str, Any]],
        layout: str = 'grid'  # 'grid' or 'list'
    ):
        super().__init__(title)
        self.metrics = metrics
        self.layout = layout
    
    def create(self) -> ui.card:
        """Create metrics card."""
        super().create()
        
        with self.card:
            if self.layout == 'grid':
                self._create_grid_layout()
            else:
                self._create_list_layout()
        
        return self.card
    
    def _create_grid_layout(self):
        """Create grid layout for metrics."""
        with ui.row().classes('w-full gap-4'):
            for metric in self.metrics:
                with ui.card().classes('flex-1 p-4 text-center'):
                    value = metric.get('value', 'N/A')
                    label = metric.get('label', 'Metric')
                    ui.label(str(value)).classes('text-h5 mb-1')
                    ui.label(label).classes('text-caption')
    
    def _create_list_layout(self):
        """Create list layout for metrics."""
        for metric in self.metrics:
            icon = metric.get('icon', '')
            label = metric.get('label', 'Metric')
            value = metric.get('value', 'N/A')
            
            with ui.row().classes('w-full justify-between items-center mb-2'):
                ui.label(f'{icon} {label}').classes('text-subtitle2')
                ui.label(str(value)).classes('has-text-weight-semibold')


class IntegrationCard(BaseCard):
    """Card for external integrations like Lightdash."""
    
    def __init__(
        self, 
        title: str,
        subtitle: str,
        integration_items: List[Dict[str, str]],
        button_text: str = "Open Integration",
        button_action: Optional[Callable] = None
    ):
        super().__init__(title, subtitle)
        self.integration_items = integration_items
        self.button_text = button_text
        self.button_action = button_action or (lambda: ui.notify('Integration coming soon!'))
    
    def create(self) -> ui.card:
        """Create integration card."""
        super().create()
        
        with self.card:
            with ui.row().classes('gap-4 w-full'):
                for item in self.integration_items:
                    self._create_integration_item(item)
        
        return self.card
    
    @safe_ui_operation
    def _create_integration_item(self, item: Dict[str, str]):
        """Create a single integration item."""
        with ui.card().classes('flex-1 p-4'):
            title = item.get('title', 'Integration Item')
            ui.label(title).classes('text-h6 mb-3')
            ui.button(
                self.button_text, 
                on_click=self.button_action
            ).classes(Styles.SMALL_BUTTON_CLASSES).style(get_button_style('small'))


def create_admin_card(
    title: str,
    subtitle: str,
    features: List[str],
    status_items: List[Dict[str, str]],
    buttons: List[Dict[str, Any]],
    tooltip: Optional[str] = None
) -> ui.card:
    """
    Convenience function to create an admin card.
    
    Args:
        title: Card title
        subtitle: Card subtitle
        features: List of feature descriptions
        status_items: List of status items with 'icon' and 'text' keys
        buttons: List of button configurations
        
    Returns:
        Created card component
    """
    card = AdminCard(title, subtitle, features, status_items, buttons, tooltip)
    return card.create()


def create_data_card(
    title: str,
    subtitle: str,
    data_func: Callable,
    table_columns: List[str],
    table_id: str,
    show_count: bool = True,
    show_selection: bool = False
) -> ui.card:
    """
    Convenience function to create a data card.
    
    Args:
        title: Card title
        subtitle: Card subtitle  
        data_func: Function that returns data for the table
        table_columns: List of column names
        table_id: Unique identifier for the table
        show_count: Whether to show row count
        
    Returns:
        Created card component
    """
    card = DataCard(title, subtitle, data_func, table_columns, table_id, show_count, show_selection=show_selection)
    return card.create()


def create_metrics_card(
    title: str,
    metrics: List[Dict[str, Any]],
    layout: str = 'grid'
) -> ui.card:
    """
    Convenience function to create a metrics card.
    
    Args:
        title: Card title
        metrics: List of metric dictionaries with 'label', 'value', 'icon' keys
        layout: Layout type ('grid' or 'list')
        
    Returns:
        Created card component
    """
    card = MetricsCard(title, metrics, layout)
    return card.create()


def create_integration_card(
    title: str,
    subtitle: str,
    integration_items: List[Dict[str, str]],
    button_text: str = "View Integration",
    button_action: Optional[Callable] = None
) -> ui.card:
    """
    Convenience function to create an integration card.
    
    Args:
        title: Card title
        subtitle: Card subtitle
        integration_items: List of integration items
        button_text: Text for action button
        button_action: Action to perform when button clicked
        
    Returns:
        Created card component
    """
    card = IntegrationCard(title, subtitle, integration_items, button_text, button_action)
    return card.create()