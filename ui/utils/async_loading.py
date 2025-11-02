"""
Async data loading utilities with loading states and progress indicators.
Provides smooth user experience with loading spinners and progress feedback.
"""

import asyncio
import time
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, Dict, List
from nicegui import ui

from config.constants import StatusMessages, ErrorMessages

T = TypeVar('T')


class LoadingState:
    """Manages loading state for UI components."""
    
    def __init__(self, component_id: str):
        self.component_id = component_id
        self.is_loading = False
        self.loading_element = None
        self.content_element = None
    
    def start_loading(self, message: str = StatusMessages.LOADING):
        """Start loading state with spinner."""
        self.is_loading = True
        if self.loading_element:
            self.loading_element.delete()
        
        # Create loading spinner
        with ui.element('div').classes('has-text-centered p-4') as loading_container:
            ui.html(f'''
            <div class="is-flex is-justify-content-center is-align-items-center">
                <div class="loader mr-3"></div>
                <span class="has-text-grey">{message}</span>
            </div>
            <style>
            .loader {{
                border: 3px solid #f3f3f3;
                border-top: 3px solid #3273dc;
                border-radius: 50%;
                width: 20px;
                height: 20px;
                animation: spin 1s linear infinite;
            }}
            @keyframes spin {{
                0% {{ transform: rotate(0deg); }}
                100% {{ transform: rotate(360deg); }}
            }}
            </style>
            ''')
        
        self.loading_element = loading_container
    
    def stop_loading(self):
        """Stop loading state and remove spinner."""
        self.is_loading = False
        if self.loading_element:
            self.loading_element.delete()
            self.loading_element = None


def async_data_loader(
    loading_message: str = StatusMessages.LOADING,
    success_message: Optional[str] = None,
    error_message: str = ErrorMessages.DATA_PROCESSING,
    show_progress: bool = False
):
    """
    Decorator for async data loading with loading states.
    
    Args:
        loading_message: Message to show during loading
        success_message: Optional success message
        error_message: Error message on failure
        show_progress: Whether to show progress indication
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Optional[T]:
            loading_state = LoadingState(f"{func.__name__}_loader")
            
            try:
                # Start loading
                loading_state.start_loading(loading_message)
                
                # Execute the async function
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    # Run sync function in thread pool
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: func(*args, **kwargs)
                    )
                
                # Stop loading
                loading_state.stop_loading()
                
                # Show success message if specified
                if success_message:
                    ui.notify(success_message, type='positive')
                
                return result
                
            except Exception as e:
                loading_state.stop_loading()
                ui.notify(f"{error_message}: {str(e)}", type='negative')
                return None
        
        return wrapper
    return decorator


class DataLoader:
    """Centralized data loader with caching and loading states."""
    
    def __init__(self):
        self.cache: Dict[str, Any] = {}
        self.loading_states: Dict[str, LoadingState] = {}
        self.cache_ttl: Dict[str, float] = {}
        self.default_ttl = 300  # 5 minutes
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cached data is still valid."""
        if key not in self.cache:
            return False
        
        if key in self.cache_ttl:
            return time.time() - self.cache_ttl[key] < self.default_ttl
        
        return True
    
    async def load_data(
        self,
        key: str,
        loader_func: Callable[[], Any],
        force_refresh: bool = False,
        loading_message: str = StatusMessages.LOADING
    ) -> Optional[Any]:
        """
        Load data with caching and loading state management.
        
        Args:
            key: Cache key for the data
            loader_func: Function to load the data
            force_refresh: Whether to bypass cache
            loading_message: Message to show during loading
        """
        
        # Return cached data if valid and not forcing refresh
        if not force_refresh and self._is_cache_valid(key):
            return self.cache[key]
        
        # Create or get loading state
        if key not in self.loading_states:
            self.loading_states[key] = LoadingState(key)
        
        loading_state = self.loading_states[key]
        
        try:
            # Start loading
            loading_state.start_loading(loading_message)
            
            # Load data
            if asyncio.iscoroutinefunction(loader_func):
                data = await loader_func()
            else:
                data = await asyncio.get_event_loop().run_in_executor(
                    None, loader_func
                )
            
            # Cache the data
            self.cache[key] = data
            self.cache_ttl[key] = time.time()
            
            # Stop loading
            loading_state.stop_loading()
            
            return data
            
        except Exception as e:
            loading_state.stop_loading()
            ui.notify(f"Error loading {key}: {str(e)}", type='negative')
            return None
    
    def invalidate_cache(self, key: Optional[str] = None):
        """Invalidate cache for specific key or all keys."""
        if key:
            self.cache.pop(key, None)
            self.cache_ttl.pop(key, None)
        else:
            self.cache.clear()
            self.cache_ttl.clear()
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get information about current cache state."""
        return {
            'cached_keys': list(self.cache.keys()),
            'cache_sizes': {k: len(str(v)) for k, v in self.cache.items()},
            'cache_ages': {k: time.time() - t for k, t in self.cache_ttl.items()}
        }


# Global data loader instance
data_loader = DataLoader()


class ProgressiveLoader:
    """Progressive data loader for large datasets."""
    
    def __init__(self, batch_size: int = 100):
        self.batch_size = batch_size
        self.loaded_items = 0
        self.total_items = 0
        self.progress_element = None
    
    def start_progressive_load(self, total_items: int, container_element):
        """Start progressive loading with progress bar."""
        self.total_items = total_items
        self.loaded_items = 0
        
        with container_element:
            with ui.element('div').classes('progress-container p-4') as progress_container:
                ui.label(f'Loading {total_items:,} items...').classes('has-text-grey mb-2')
                
                # Progress bar
                ui.html(f'''
                <progress class="progress is-primary" value="0" max="{total_items}">0%</progress>
                <div class="has-text-centered">
                    <small class="has-text-grey">
                        <span id="progress-text">0 / {total_items:,} items loaded</span>
                    </small>
                </div>
                ''')
        
        self.progress_element = progress_container
    
    def update_progress(self, items_loaded: int):
        """Update progress bar."""
        self.loaded_items = items_loaded
        
        if self.progress_element:
            progress_percent = (items_loaded / self.total_items) * 100 if self.total_items > 0 else 0
            
            # Update progress bar via JavaScript
            ui.run_javascript(f'''
                const progress = document.querySelector('.progress');
                const progressText = document.querySelector('#progress-text');
                if (progress) {{
                    progress.value = {items_loaded};
                    progress.textContent = "{progress_percent:.1f}%";
                }}
                if (progressText) {{
                    progressText.textContent = "{items_loaded:,} / {self.total_items:,} items loaded";
                }}
            ''')
    
    def finish_loading(self):
        """Complete loading and remove progress indicator."""
        if self.progress_element:
            self.progress_element.delete()
            self.progress_element = None


async def load_data_with_retry(
    loader_func: Callable[[], T],
    max_retries: int = 3,
    retry_delay: float = 1.0,
    exponential_backoff: bool = True
) -> Optional[T]:
    """
    Load data with retry logic and exponential backoff.
    
    Args:
        loader_func: Function to load data
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries
        exponential_backoff: Whether to use exponential backoff
    """
    
    for attempt in range(max_retries + 1):
        try:
            if asyncio.iscoroutinefunction(loader_func):
                return await loader_func()
            else:
                return await asyncio.get_event_loop().run_in_executor(
                    None, loader_func
                )
        
        except Exception as e:
            if attempt == max_retries:
                # Last attempt failed
                ui.notify(f"Failed to load data after {max_retries} attempts: {str(e)}", type='negative')
                return None
            
            # Calculate delay for next attempt
            delay = retry_delay
            if exponential_backoff:
                delay = retry_delay * (2 ** attempt)
            
            ui.notify(f"Attempt {attempt + 1} failed, retrying in {delay:.1f}s...", type='warning')
            await asyncio.sleep(delay)
    
    return None


def create_loading_placeholder(message: str = StatusMessages.LOADING) -> ui.element:
    """Create a loading placeholder with spinner."""
    with ui.element('div').classes('has-text-centered p-6') as container:
        ui.html(f'''
        <div class="is-flex is-flex-direction-column is-align-items-center">
            <div class="loader mb-4"></div>
            <p class="has-text-grey">{message}</p>
        </div>
        <style>
        .loader {{
            border: 4px solid #f3f3f3;
            border-top: 4px solid #3273dc;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
        }}
        @keyframes spin {{
            0% {{ transform: rotate(0deg); }}
            100% {{ transform: rotate(360deg); }}
        }}
        </style>
        ''')
    
    return container