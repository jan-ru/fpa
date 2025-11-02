"""
State management utilities for application state and filters.
Provides reactive state management with automatic UI updates.
"""

import json
from typing import Any, Dict, List, Set, Optional, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path

from nicegui import ui

from config.constants import StatusMessages, ErrorMessages


@dataclass
class FilterState:
    """Represents the current filter state."""
    years: Set[int] = field(default_factory=set)
    months: Set[int] = field(default_factory=set)
    quarters: Set[int] = field(default_factory=set)
    account_codes: Set[str] = field(default_factory=set)
    date_range: Optional[Dict[str, date]] = None
    search_term: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert filter state to dictionary."""
        return {
            'years': list(self.years),
            'months': list(self.months),
            'quarters': list(self.quarters),
            'account_codes': list(self.account_codes),
            'date_range': {
                'start': self.date_range['start'].isoformat() if self.date_range and 'start' in self.date_range else None,
                'end': self.date_range['end'].isoformat() if self.date_range and 'end' in self.date_range else None
            } if self.date_range else None,
            'search_term': self.search_term
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FilterState':
        """Create filter state from dictionary."""
        state = cls()
        state.years = set(data.get('years', []))
        state.months = set(data.get('months', []))
        state.quarters = set(data.get('quarters', []))
        state.account_codes = set(data.get('account_codes', []))
        state.search_term = data.get('search_term', '')
        
        date_range = data.get('date_range')
        if date_range and date_range.get('start') and date_range.get('end'):
            state.date_range = {
                'start': datetime.fromisoformat(date_range['start']).date(),
                'end': datetime.fromisoformat(date_range['end']).date()
            }
        
        return state
    
    def is_empty(self) -> bool:
        """Check if filter state is empty."""
        return (
            not self.years and 
            not self.months and 
            not self.quarters and 
            not self.account_codes and 
            not self.date_range and 
            not self.search_term.strip()
        )
    
    def clear(self):
        """Clear all filters."""
        self.years.clear()
        self.months.clear()
        self.quarters.clear()
        self.account_codes.clear()
        self.date_range = None
        self.search_term = ""
    
    def get_summary(self) -> str:
        """Get human-readable summary of filters."""
        parts = []
        
        if self.years:
            parts.append(f"Years: {', '.join(map(str, sorted(self.years)))}")
        
        if self.quarters:
            parts.append(f"Quarters: {', '.join([f'Q{q}' for q in sorted(self.quarters)])}")
        
        if self.months:
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                          'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            parts.append(f"Months: {', '.join([month_names[m-1] for m in sorted(self.months)])}")
        
        if self.account_codes:
            codes = sorted(list(self.account_codes))
            if len(codes) > 3:
                parts.append(f"Accounts: {', '.join(codes[:3])} + {len(codes)-3} more")
            else:
                parts.append(f"Accounts: {', '.join(codes)}")
        
        if self.date_range:
            start = self.date_range['start'].strftime('%Y-%m-%d')
            end = self.date_range['end'].strftime('%Y-%m-%d')
            parts.append(f"Date Range: {start} to {end}")
        
        if self.search_term.strip():
            parts.append(f"Search: '{self.search_term.strip()}'")
        
        return " | ".join(parts) if parts else "No filters applied"


class StateManager:
    """Manages application state with reactive updates."""
    
    def __init__(self):
        self.state: Dict[str, Any] = {}
        self.listeners: Dict[str, List[Callable]] = {}
        self.filter_state = FilterState()
        self.ui_components: Dict[str, Any] = {}
    
    def set(self, key: str, value: Any, notify: bool = True):
        """Set state value and notify listeners."""
        old_value = self.state.get(key)
        self.state[key] = value
        
        if notify and old_value != value:
            self._notify_listeners(key, value, old_value)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get state value."""
        return self.state.get(key, default)
    
    def update(self, updates: Dict[str, Any], notify: bool = True):
        """Update multiple state values."""
        for key, value in updates.items():
            self.set(key, value, notify=False)
        
        if notify:
            for key in updates.keys():
                self._notify_listeners(key, self.state[key], None)
    
    def subscribe(self, key: str, callback: Callable):
        """Subscribe to state changes."""
        if key not in self.listeners:
            self.listeners[key] = []
        self.listeners[key].append(callback)
    
    def unsubscribe(self, key: str, callback: Callable):
        """Unsubscribe from state changes."""
        if key in self.listeners and callback in self.listeners[key]:
            self.listeners[key].remove(callback)
    
    def _notify_listeners(self, key: str, new_value: Any, old_value: Any):
        """Notify all listeners for a key."""
        if key in self.listeners:
            for callback in self.listeners[key]:
                try:
                    callback(new_value, old_value)
                except Exception as e:
                    print(f"Error in state listener: {e}")
    
    def register_ui_component(self, key: str, component: Any):
        """Register UI component for state updates."""
        self.ui_components[key] = component
    
    def update_ui_component(self, key: str, **kwargs):
        """Update registered UI component."""
        if key in self.ui_components:
            component = self.ui_components[key]
            if hasattr(component, 'update'):
                component.update(**kwargs)


class FilterManager:
    """Manages filter state with automatic persistence and UI updates."""
    
    def __init__(self, state_manager: StateManager, persistence_file: Path = None):
        self.state_manager = state_manager
        self.persistence_file = persistence_file or Path('.filter_state.json')
        self.filter_callbacks: List[Callable[[FilterState], None]] = []
        
        # Load persisted filter state
        self._load_state()
        
        # Subscribe to filter changes
        self.state_manager.subscribe('filters_changed', self._on_filters_changed)
    
    def get_filter_state(self) -> FilterState:
        """Get current filter state."""
        return self.state_manager.filter_state
    
    def update_filters(self, **kwargs):
        """Update filter state with new values."""
        filter_state = self.state_manager.filter_state
        
        for key, value in kwargs.items():
            if hasattr(filter_state, key):
                setattr(filter_state, key, value)
        
        self.state_manager.set('filters_changed', filter_state)
        self._save_state()
    
    def toggle_year(self, year: int):
        """Toggle year in filter state."""
        filter_state = self.state_manager.filter_state
        if year in filter_state.years:
            filter_state.years.remove(year)
        else:
            filter_state.years.add(year)
        
        self.state_manager.set('filters_changed', filter_state)
        self._save_state()
    
    def toggle_month(self, month: int):
        """Toggle month in filter state."""
        filter_state = self.state_manager.filter_state
        if month in filter_state.months:
            filter_state.months.remove(month)
        else:
            filter_state.months.add(month)
        
        self.state_manager.set('filters_changed', filter_state)
        self._save_state()
    
    def toggle_quarter(self, quarter: int):
        """Toggle quarter in filter state."""
        filter_state = self.state_manager.filter_state
        if quarter in filter_state.quarters:
            filter_state.quarters.remove(quarter)
        else:
            filter_state.quarters.add(quarter)
        
        self.state_manager.set('filters_changed', filter_state)
        self._save_state()
    
    def toggle_account(self, account_code: str):
        """Toggle account in filter state."""
        filter_state = self.state_manager.filter_state
        if account_code in filter_state.account_codes:
            filter_state.account_codes.remove(account_code)
        else:
            filter_state.account_codes.add(account_code)
        
        self.state_manager.set('filters_changed', filter_state)
        self._save_state()
    
    def set_date_range(self, start_date: date, end_date: date):
        """Set date range filter."""
        filter_state = self.state_manager.filter_state
        filter_state.date_range = {'start': start_date, 'end': end_date}
        
        self.state_manager.set('filters_changed', filter_state)
        self._save_state()
    
    def set_search_term(self, term: str):
        """Set search term filter."""
        filter_state = self.state_manager.filter_state
        filter_state.search_term = term
        
        self.state_manager.set('filters_changed', filter_state)
        self._save_state()
    
    def clear_all_filters(self):
        """Clear all filters."""
        filter_state = self.state_manager.filter_state
        filter_state.clear()
        
        self.state_manager.set('filters_changed', filter_state)
        self._save_state()
        ui.notify(StatusMessages.FILTERS_CLEARED)
    
    def add_filter_callback(self, callback: Callable[[FilterState], None]):
        """Add callback to be called when filters change."""
        self.filter_callbacks.append(callback)
    
    def remove_filter_callback(self, callback: Callable[[FilterState], None]):
        """Remove filter callback."""
        if callback in self.filter_callbacks:
            self.filter_callbacks.remove(callback)
    
    def _on_filters_changed(self, filter_state: FilterState, old_state: FilterState):
        """Handle filter state changes."""
        # Notify all callbacks
        for callback in self.filter_callbacks:
            try:
                callback(filter_state)
            except Exception as e:
                print(f"Error in filter callback: {e}")
        
        # Show filter summary notification
        if not filter_state.is_empty():
            ui.notify(f"Filters: {filter_state.get_summary()}", type='info')
    
    def _save_state(self):
        """Save filter state to disk."""
        try:
            with open(self.persistence_file, 'w') as f:
                json.dump(self.state_manager.filter_state.to_dict(), f)
        except Exception:
            pass
    
    def _load_state(self):
        """Load filter state from disk."""
        if self.persistence_file.exists():
            try:
                with open(self.persistence_file, 'r') as f:
                    data = json.load(f)
                self.state_manager.filter_state = FilterState.from_dict(data)
            except Exception:
                pass


class UIStateManager:
    """Manages UI component states and interactions."""
    
    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager
        self.component_states: Dict[str, Dict[str, Any]] = {}
        self.loading_states: Dict[str, bool] = {}
    
    def set_loading(self, component_id: str, loading: bool, message: str = StatusMessages.LOADING):
        """Set loading state for a component."""
        self.loading_states[component_id] = loading
        self.state_manager.set(f'loading_{component_id}', {'loading': loading, 'message': message})
    
    def is_loading(self, component_id: str) -> bool:
        """Check if component is in loading state."""
        return self.loading_states.get(component_id, False)
    
    def set_component_state(self, component_id: str, state: Dict[str, Any]):
        """Set state for a UI component."""
        self.component_states[component_id] = state
        self.state_manager.set(f'component_{component_id}', state)
    
    def get_component_state(self, component_id: str) -> Dict[str, Any]:
        """Get state for a UI component."""
        return self.component_states.get(component_id, {})
    
    def update_component_state(self, component_id: str, updates: Dict[str, Any]):
        """Update component state with new values."""
        current_state = self.get_component_state(component_id)
        current_state.update(updates)
        self.set_component_state(component_id, current_state)


class DataStateManager:
    """Manages data loading and caching state."""
    
    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager
        self.data_cache: Dict[str, Any] = {}
        self.cache_timestamps: Dict[str, datetime] = {}
        self.loading_promises: Dict[str, Any] = {}
    
    def get_data(self, key: str, loader: Callable = None, force_refresh: bool = False) -> Optional[Any]:
        """Get data with caching."""
        if not force_refresh and key in self.data_cache:
            return self.data_cache[key]
        
        if loader and key not in self.loading_promises:
            # Start loading
            self.state_manager.set(f'data_loading_{key}', True)
            
            try:
                data = loader()
                self.data_cache[key] = data
                self.cache_timestamps[key] = datetime.now()
                self.state_manager.set(f'data_loading_{key}', False)
                self.state_manager.set(f'data_{key}', data)
                return data
            except Exception as e:
                self.state_manager.set(f'data_loading_{key}', False)
                self.state_manager.set(f'data_error_{key}', str(e))
                return None
        
        return self.data_cache.get(key)
    
    def invalidate_data(self, key: str = None):
        """Invalidate cached data."""
        if key:
            self.data_cache.pop(key, None)
            self.cache_timestamps.pop(key, None)
        else:
            self.data_cache.clear()
            self.cache_timestamps.clear()
    
    def is_data_stale(self, key: str, max_age_minutes: int = 5) -> bool:
        """Check if cached data is stale."""
        if key not in self.cache_timestamps:
            return True
        
        age = datetime.now() - self.cache_timestamps[key]
        return age.total_seconds() > (max_age_minutes * 60)


# Global state managers
state_manager = StateManager()
filter_manager = FilterManager(state_manager)
ui_state_manager = UIStateManager(state_manager)
data_state_manager = DataStateManager(state_manager)


def create_reactive_filter_buttons(
    items: List[Union[int, str]], 
    filter_type: str,
    callback: Callable = None
) -> List[Any]:
    """Create reactive filter buttons that update state."""
    buttons = []
    filter_state = filter_manager.get_filter_state()
    
    for item in items:
        is_selected = False
        
        if filter_type == 'year':
            is_selected = item in filter_state.years
        elif filter_type == 'month':
            is_selected = item in filter_state.months
        elif filter_type == 'quarter':
            is_selected = item in filter_state.quarters
        elif filter_type == 'account':
            is_selected = item in filter_state.account_codes
        
        def make_click_handler(item_val, filter_type_val):
            def handler():
                if filter_type_val == 'year':
                    filter_manager.toggle_year(item_val)
                elif filter_type_val == 'month':
                    filter_manager.toggle_month(item_val)
                elif filter_type_val == 'quarter':
                    filter_manager.toggle_quarter(item_val)
                elif filter_type_val == 'account':
                    filter_manager.toggle_account(item_val)
                
                if callback:
                    callback()
            return handler
        
        button_classes = 'button is-small mr-1'
        if is_selected:
            button_classes += ' is-primary'
        
        button = ui.button(
            str(item), 
            on_click=make_click_handler(item, filter_type)
        ).classes(button_classes)
        
        buttons.append(button)
    
    return buttons


def get_current_filter_state() -> FilterState:
    """Get the current filter state."""
    return filter_manager.get_filter_state()


def subscribe_to_filter_changes(callback: Callable[[FilterState], None]):
    """Subscribe to filter state changes."""
    filter_manager.add_filter_callback(callback)