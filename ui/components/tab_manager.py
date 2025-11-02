"""
Tab management components for the Financial Data Platform.
Provides centralized tab creation and content management.
"""

from typing import Callable, Dict, Any
from nicegui import ui


class TabManager:
    """Manages tab creation and content organization."""
    
    def __init__(self):
        self.tabs = {}
        self.tab_panels = {}
    
    def create_tabs(self, tab_configs: Dict[str, Dict[str, Any]]) -> Dict[str, ui.tab]:
        """
        Create multiple tabs from configuration.
        
        Args:
            tab_configs: Dictionary with tab configurations
                Format: {
                    'tab_key': {
                        'label': 'Tab Label',
                        'icon': 'icon_name' (optional)
                    }
                }
        
        Returns:
            Dictionary of created tab objects
        """
        for tab_key, config in tab_configs.items():
            label = config.get('label', tab_key.title())
            icon = config.get('icon')
            
            if icon:
                self.tabs[tab_key] = ui.tab(label, icon=icon)
            else:
                self.tabs[tab_key] = ui.tab(label)
        
        return self.tabs
    
    def create_tab_panel(self, tab_key: str, content_func: Callable[[], None]) -> ui.tab_panel:
        """
        Create a tab panel with content.
        
        Args:
            tab_key: Key identifying the tab
            content_func: Function that creates the tab content
            
        Returns:
            Created tab panel
        """
        if tab_key not in self.tabs:
            raise ValueError(f"Tab '{tab_key}' not found. Create tabs first.")
        
        tab_panel = ui.tab_panel(self.tabs[tab_key])
        self.tab_panels[tab_key] = tab_panel
        
        with tab_panel:
            content_func()
        
        return tab_panel
    
    def get_tab(self, tab_key: str) -> ui.tab:
        """Get a specific tab by key."""
        return self.tabs.get(tab_key)
    
    def get_tab_panel(self, tab_key: str) -> ui.tab_panel:
        """Get a specific tab panel by key."""
        return self.tab_panels.get(tab_key)


def create_standard_tabs() -> TabManager:
    """
    Create the standard set of tabs for the Financial Data Platform.
    
    Returns:
        Configured TabManager instance
    """
    tab_manager = TabManager()
    
    tab_configs = {
        'overview': {
            'label': 'Financial Overview',
            'icon': 'analytics'
        },
        'inputs': {
            'label': 'Data Inputs',
            'icon': 'upload_file'
        },
        'lineage': {
            'label': 'Data Lineage',
            'icon': 'account_tree'
        },
        'admin': {
            'label': 'Administration',
            'icon': 'settings'
        }
    }
    
    tab_manager.create_tabs(tab_configs)
    return tab_manager