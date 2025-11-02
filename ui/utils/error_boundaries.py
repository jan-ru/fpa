"""
Error boundaries for data source isolation and user-friendly error handling.
"""

import logging
from functools import wraps
from typing import Any, Callable, Optional, Dict, List
from nicegui import ui

from config.constants import ErrorMessages, StatusMessages

logger = logging.getLogger(__name__)


class DataSourceError(Exception):
    """Custom exception for data source errors."""
    
    def __init__(self, source: str, operation: str, message: str, original_error: Exception = None):
        self.source = source
        self.operation = operation
        self.message = message
        self.original_error = original_error
        super().__init__(f"{source} {operation}: {message}")


class ErrorBoundary:
    """Context manager for isolating data source errors."""
    
    def __init__(self, source: str, operation: str, fallback_data: Any = None, 
                 show_notification: bool = True, ui_fallback: Optional[Callable] = None):
        self.source = source
        self.operation = operation
        self.fallback_data = fallback_data
        self.show_notification = show_notification
        self.ui_fallback = ui_fallback
        self.error = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.error = DataSourceError(
                source=self.source,
                operation=self.operation,
                message=str(exc_val),
                original_error=exc_val
            )
            
            # Log the error
            logger.error(f"Error boundary caught: {self.error}")
            
            # Show user notification
            if self.show_notification:
                self._show_user_notification()
            
            # Suppress the exception (return True)
            return True
        return False
    
    def _show_user_notification(self):
        """Show user-friendly error notification."""
        error_messages = {
            'database': f"Database connection issue. Using cached data if available.",
            'excel': f"Error reading Excel file. File may be corrupted or in use.",
            'dbt': f"DBT operation failed. Check dbt configuration.",
            'file_system': f"File access error. Check permissions.",
            'iceberg': f"Data lake access error. Using available data.",
        }
        
        message = error_messages.get(self.source.lower(), f"{self.source} error occurred")
        ui.notify(message, type='warning', timeout=5000)
    
    def get_fallback_data(self):
        """Get fallback data if operation failed."""
        return self.fallback_data if self.error else None
    
    def create_error_ui(self) -> ui.card:
        """Create error UI component for display."""
        if not self.error:
            return None
        
        with ui.card().classes('border-red-500 bg-red-50'):
            ui.label(f"Error: {self.source}").classes('text-red-700 font-semibold')
            ui.label(f"Operation: {self.operation}").classes('text-red-600 text-sm')
            if self.ui_fallback:
                ui.button('Retry', on_click=self.ui_fallback).classes('bg-red-500 text-white mt-2')


def data_source_boundary(source: str, operation: str, fallback_data: Any = None):
    """Decorator for wrapping data source operations with error boundaries."""
    
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with ErrorBoundary(source, operation, fallback_data) as boundary:
                return func(*args, **kwargs)
            
            # If error occurred, return fallback
            if boundary.error:
                logger.warning(f"Using fallback data for {source} {operation}")
                return boundary.get_fallback_data()
        
        return wrapper
    return decorator


def create_data_error_card(source: str, error_message: str, retry_callback: Optional[Callable] = None) -> ui.card:
    """Create a standardized error card for data loading failures."""
    
    with ui.card().classes('border-orange-500 bg-orange-50 p-4'):
        with ui.row().classes('items-center gap-3'):
            ui.html('<i class="fas fa-exclamation-triangle text-orange-500"></i>', sanitize=False)
            ui.label(f"{source} Unavailable").classes('text-orange-700 font-semibold')
        
        ui.label(error_message).classes('text-orange-600 text-sm mt-2')
        
        if retry_callback:
            ui.button('Retry', on_click=retry_callback).classes('bg-orange-500 text-white mt-3 text-sm')
        else:
            ui.label('Check logs for details').classes('text-orange-500 text-xs mt-2')


def create_loading_placeholder(message: str = "Loading data...") -> ui.card:
    """Create a loading placeholder card."""
    
    with ui.card().classes('bg-blue-50 p-4'):
        with ui.row().classes('items-center gap-3'):
            ui.spinner(size='sm')
            ui.label(message).classes('text-blue-600')


class DataSourceValidator:
    """Validates data sources and provides user-friendly error messages."""
    
    @staticmethod
    def validate_database_connection() -> Dict[str, Any]:
        """Validate database connectivity."""
        try:
            from data_access import data_access
            # Simple connectivity test
            test_query = data_access.get_account_summary()
            return {
                'status': 'success',
                'message': 'Database connected successfully',
                'data_available': bool(test_query)
            }
        except Exception as e:
            return {
                'status': 'error',
                'message': f'Database connection failed: {str(e)[:100]}...',
                'data_available': False
            }
    
    @staticmethod
    def validate_excel_directory() -> Dict[str, Any]:
        """Validate Excel data directory."""
        from pathlib import Path
        
        data_dir = Path("../data/raw")
        
        if not data_dir.exists():
            return {
                'status': 'error',
                'message': 'Excel data directory not found',
                'files_count': 0
            }
        
        excel_files = list(data_dir.glob("*.xlsx"))
        
        return {
            'status': 'success' if excel_files else 'warning',
            'message': f'Found {len(excel_files)} Excel files' if excel_files else 'No Excel files found',
            'files_count': len(excel_files)
        }
    
    @staticmethod
    def validate_dbt_setup() -> Dict[str, Any]:
        """Validate DBT configuration."""
        from config.constants import Paths
        
        issues = []
        
        if not Paths.DBT_PROJECT.exists():
            issues.append("DBT project directory not found")
        
        if not (Paths.DBT_PROJECT / "dbt_project.yml").exists():
            issues.append("DBT configuration file missing")
        
        if not Paths.DBT_DOCS.exists():
            issues.append("DBT documentation not generated")
        
        if issues:
            return {
                'status': 'error',
                'message': '; '.join(issues),
                'configured': False
            }
        
        return {
            'status': 'success',
            'message': 'DBT properly configured',
            'configured': True
        }


def create_system_status_card() -> ui.card:
    """Create a system status overview card."""
    
    validator = DataSourceValidator()
    
    with ui.card().classes('w-full p-4'):
        ui.label('System Status').classes('text-lg font-semibold mb-3')
        
        # Check each data source
        sources = [
            ('Database', validator.validate_database_connection()),
            ('Excel Files', validator.validate_excel_directory()),
            ('DBT', validator.validate_dbt_setup()),
        ]
        
        for source_name, status in sources:
            with ui.row().classes('items-center gap-2 mb-2'):
                # Status icon
                if status['status'] == 'success':
                    ui.html('<i class="fas fa-check-circle text-green-500"></i>', sanitize=False)
                elif status['status'] == 'warning':
                    ui.html('<i class="fas fa-exclamation-triangle text-yellow-500"></i>', sanitize=False)
                else:
                    ui.html('<i class="fas fa-times-circle text-red-500"></i>', sanitize=False)
                
                # Source name and message
                ui.label(source_name).classes('font-medium min-w-24')
                ui.label(status['message']).classes('text-sm text-gray-600')


# Pre-configured error boundaries for common data sources
excel_boundary = lambda operation, fallback=None: data_source_boundary('Excel', operation, fallback or [])
database_boundary = lambda operation, fallback=None: data_source_boundary('Database', operation, fallback or [])
dbt_boundary = lambda operation, fallback=None: data_source_boundary('DBT', operation, fallback or {})
file_boundary = lambda operation, fallback=None: data_source_boundary('FileSystem', operation, fallback or [])