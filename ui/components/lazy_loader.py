"""
Lazy loading system for tab content and data.
Only loads data when tabs are accessed for the first time.
"""

from typing import Dict, Callable, Any, Optional
from functools import wraps
from nicegui import ui

from utils.error_boundaries import ErrorBoundary, create_data_error_card, create_loading_placeholder


class LazyDataLoader:
    """Manages lazy loading of data for UI components."""
    
    def __init__(self):
        self.loaded_data: Dict[str, Any] = {}
        self.loading_states: Dict[str, bool] = {}
        self.error_states: Dict[str, str] = {}
        self.loaders: Dict[str, Callable] = {}
    
    def register_loader(self, key: str, loader_func: Callable, dependencies: list = None):
        """Register a data loader function for lazy loading."""
        self.loaders[key] = {
            'func': loader_func,
            'dependencies': dependencies or [],
            'loaded': False
        }
    
    def get_data(self, key: str, force_reload: bool = False) -> Any:
        """Get data, loading it lazily if not already loaded."""
        
        # Return cached data if available and not forcing reload
        if key in self.loaded_data and not force_reload:
            return self.loaded_data[key]
        
        # Check if loader is registered
        if key not in self.loaders:
            raise ValueError(f"No loader registered for key: {key}")
        
        # Set loading state
        self.loading_states[key] = True
        self.error_states.pop(key, None)  # Clear previous errors
        
        try:
            # Load dependencies first
            loader_config = self.loaders[key]
            for dep_key in loader_config['dependencies']:
                if dep_key not in self.loaded_data:
                    self.get_data(dep_key)
            
            # Execute the loader function
            with ErrorBoundary('LazyLoader', f'loading {key}', fallback_data=None) as boundary:
                data = loader_config['func']()
                
            if boundary.error:
                self.error_states[key] = str(boundary.error)
                return None
            
            # Cache the loaded data
            self.loaded_data[key] = data
            loader_config['loaded'] = True
            
            return data
            
        finally:
            self.loading_states[key] = False
    
    def is_loading(self, key: str) -> bool:
        """Check if data is currently being loaded."""
        return self.loading_states.get(key, False)
    
    def has_error(self, key: str) -> bool:
        """Check if data loading failed."""
        return key in self.error_states
    
    def get_error(self, key: str) -> Optional[str]:
        """Get error message for failed data loading."""
        return self.error_states.get(key)
    
    def clear_cache(self, key: str = None):
        """Clear cached data for specific key or all data."""
        if key:
            self.loaded_data.pop(key, None)
            self.error_states.pop(key, None)
            if key in self.loaders:
                self.loaders[key]['loaded'] = False
        else:
            self.loaded_data.clear()
            self.error_states.clear()
            for loader in self.loaders.values():
                loader['loaded'] = False
    
    def reload_data(self, key: str):
        """Force reload data for specific key."""
        return self.get_data(key, force_reload=True)
    
    def get_lazy_data_status(self) -> Dict[str, Any]:
        """Get status information about the lazy loader."""
        loaded_keys = [key for key, data in self.loaded_data.items() if data is not None]
        error_keys = list(self.error_states.keys())
        
        details = {}
        for key in self.loaders:
            details[key] = {
                'loaded': key in loaded_keys,
                'has_error': key in error_keys,
                'error_message': self.error_states.get(key),
                'is_loading': self.loading_states.get(key, False)
            }
        
        return {
            'total_loaders': len(self.loaders),
            'loaded_count': len(loaded_keys),
            'error_count': len(error_keys),
            'details': details
        }


class LazyTabContent:
    """Manages lazy loading of tab content."""
    
    def __init__(self, tab_id: str, content_loader: Callable):
        self.tab_id = tab_id
        self.content_loader = content_loader
        self.is_loaded = False
        self.container = None
        self.error_message = None
    
    def load_content(self):
        """Load tab content if not already loaded."""
        if self.is_loaded:
            return
        
        try:
            with ErrorBoundary('TabContent', f'loading {self.tab_id}') as boundary:
                # Clear container and load content
                if self.container:
                    self.container.clear()
                
                self.content_loader()
                
            if boundary.error:
                self.error_message = str(boundary.error)
                self._show_error_content()
            else:
                self.is_loaded = True
                
        except Exception as e:
            self.error_message = str(e)
            self._show_error_content()
    
    def _show_error_content(self):
        """Show error content in the tab."""
        if self.container:
            with self.container:
                create_data_error_card(
                    source=f"Tab: {self.tab_id}",
                    error_message=self.error_message,
                    retry_callback=lambda: self.reload_content()
                )
    
    def reload_content(self):
        """Force reload tab content."""
        self.is_loaded = False
        self.error_message = None
        self.load_content()


# Global lazy data loader instance
lazy_loader = LazyDataLoader()


def lazy_data(key: str, dependencies: list = None):
    """Decorator for registering lazy data loaders."""
    
    def decorator(func: Callable):
        lazy_loader.register_loader(key, func, dependencies)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            return lazy_loader.get_data(key)
        
        return wrapper
    return decorator


def create_lazy_tab_panel(tab: ui.tab, content_func: Callable) -> ui.tab_panel:
    """Create a tab panel with lazy content loading."""
    
    panel = ui.tab_panel(tab)
    lazy_content = LazyTabContent(tab.text, content_func)
    lazy_content.container = panel
    
    # Load content when tab becomes active
    def on_tab_change():
        # Check if this tab is active
        if hasattr(tab, 'value') and tab.value:
            lazy_content.load_content()
    
    # Set up tab change listener (this would need to be connected to the actual tab system)
    panel.on('click', on_tab_change)
    
    return panel


def create_lazy_data_card(
    title: str,
    data_key: str,
    table_columns: list,
    table_id: str,
    subtitle: str = "",
    show_count: bool = True,
    show_selection: bool = False
):
    """Create a data card with lazy loading."""
    
    from components.cards import create_data_card
    
    def get_lazy_data():
        """Wrapper function for lazy data loading."""
        if lazy_loader.is_loading(data_key):
            return []  # Return empty while loading
        
        if lazy_loader.has_error(data_key):
            return []  # Return empty on error
        
        return lazy_loader.get_data(data_key) or []
    
    # Create the card
    card = create_data_card(
        title=title,
        subtitle=subtitle,
        data_func=get_lazy_data,
        table_columns=table_columns,
        table_id=table_id,
        show_count=show_count,
        show_selection=show_selection
    )
    
    # Add loading/error indicators
    with card:
        if lazy_loader.is_loading(data_key):
            create_loading_placeholder(f"Loading {title.lower()}...")
        elif lazy_loader.has_error(data_key):
            create_data_error_card(
                source=title,
                error_message=lazy_loader.get_error(data_key),
                retry_callback=lambda: lazy_loader.reload_data(data_key)
            )
    
    return card


# Pre-configured lazy loaders for common data
def setup_lazy_loaders():
    """Set up lazy loaders for all application data."""
    
    from services.data_service import (
        get_sorted_accounts, get_limited_transactions, 
        get_excel_files_data, get_dbt_models_data
    )
    from utils.error_boundaries import database_boundary, excel_boundary, file_boundary
    
    # Register data loaders with error boundaries
    lazy_loader.register_loader(
        'accounts', 
        database_boundary('get_accounts', [])(get_sorted_accounts)
    )
    
    lazy_loader.register_loader(
        'transactions', 
        database_boundary('get_transactions', [])(get_limited_transactions)
    )
    
    lazy_loader.register_loader(
        'excel_files', 
        excel_boundary('scan_files', [])(get_excel_files_data)
    )
    
    lazy_loader.register_loader(
        'dbt_models', 
        file_boundary('scan_dbt', [])(get_dbt_models_data)
    )


def create_data_refresh_button(data_keys: list, label: str = "Refresh Data"):
    """Create a button to refresh specific data sources."""
    
    def refresh_data():
        for key in data_keys:
            lazy_loader.reload_data(key)
        ui.notify(f"Refreshed {len(data_keys)} data sources", type='positive')
    
    return ui.button(label, on_click=refresh_data).classes('bg-blue-500 text-white')


def get_lazy_data_status() -> Dict[str, Any]:
    """Get status of all lazy-loaded data."""
    
    status = {
        'loaded_count': len(lazy_loader.loaded_data),
        'error_count': len(lazy_loader.error_states),
        'loading_count': sum(1 for loading in lazy_loader.loading_states.values() if loading),
        'details': {}
    }
    
    for key, loader_config in lazy_loader.loaders.items():
        status['details'][key] = {
            'loaded': loader_config['loaded'],
            'loading': lazy_loader.is_loading(key),
            'error': lazy_loader.has_error(key),
            'error_message': lazy_loader.get_error(key)
        }
    
    return status