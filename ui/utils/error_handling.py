"""
Error handling utilities for the FPA application.
Provides consistent error handling patterns and user-friendly messages.
"""

import logging
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, Union
from nicegui import ui

from config.constants import ErrorMessages, StatusMessages

# Set up logging
logger = logging.getLogger(__name__)

T = TypeVar('T')

def safe_data_fetch(
    func: Callable[..., T], 
    fallback: Optional[T] = None,
    error_message: str = ErrorMessages.DATA_PROCESSING,
    show_notification: bool = True,
    log_error: bool = True
) -> Callable[..., Optional[T]]:
    """
    Decorator to safely execute data fetching functions with error handling.
    
    Args:
        func: Function to execute safely
        fallback: Value to return if function fails
        error_message: Message to show user on error
        show_notification: Whether to show UI notification
        log_error: Whether to log the error
        
    Returns:
        Wrapped function that handles errors gracefully
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Optional[T]:
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            if log_error:
                logger.warning(f"File not found in {func.__name__}: {args}")
            if show_notification:
                ui.notify(ErrorMessages.FILE_NOT_FOUND, type='warning')
            return fallback
        except PermissionError:
            if log_error:
                logger.error(f"Permission denied in {func.__name__}: {args}")
            if show_notification:
                ui.notify(ErrorMessages.PERMISSION_DENIED, type='negative')
            return fallback
        except Exception as e:
            if log_error:
                logger.error(f"Error in {func.__name__}: {str(e)}")
            if show_notification:
                ui.notify(error_message, type='negative')
            return fallback
    
    return wrapper

def safe_ui_operation(
    func: Callable[..., T],
    error_message: str = "Operation failed",
    show_success: bool = False,
    success_message: str = StatusMessages.SUCCESS
) -> Callable[..., Optional[T]]:
    """
    Decorator for UI operations that may fail.
    
    Args:
        func: UI function to execute safely
        error_message: Message to show on error
        show_success: Whether to show success notification
        success_message: Success message to display
        
    Returns:
        Wrapped function with error handling
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Optional[T]:
        try:
            result = func(*args, **kwargs)
            if show_success:
                ui.notify(success_message, type='positive')
            return result
        except Exception as e:
            logger.error(f"UI operation failed in {func.__name__}: {str(e)}")
            ui.notify(error_message, type='negative')
            return None
    
    return wrapper

def validate_file_exists(file_path: str, friendly_name: str = "File") -> bool:
    """
    Validate that a file exists and show appropriate message if not.
    
    Args:
        file_path: Path to check
        friendly_name: Human-readable name for the file
        
    Returns:
        True if file exists, False otherwise
    """
    from pathlib import Path
    
    if not Path(file_path).exists():
        ui.notify(f"{friendly_name} not found: {file_path}", type='warning')
        logger.warning(f"File not found: {file_path}")
        return False
    return True

def validate_data_not_empty(data: Any, data_name: str = "Data") -> bool:
    """
    Validate that data is not empty and show message if it is.
    
    Args:
        data: Data to check
        data_name: Human-readable name for the data
        
    Returns:
        True if data is not empty, False otherwise
    """
    if not data or (hasattr(data, '__len__') and len(data) == 0):
        ui.notify(f"{data_name} is empty or unavailable", type='info')
        logger.info(f"Empty data encountered: {data_name}")
        return False
    return True

def handle_database_error(operation_name: str = "Database operation"):
    """
    Decorator specifically for database operations.
    
    Args:
        operation_name: Name of the database operation for logging
    """
    def decorator(func: Callable[..., T]) -> Callable[..., Optional[T]]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Optional[T]:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_msg = f"{operation_name} failed: {str(e)}"
                logger.error(error_msg)
                ui.notify(ErrorMessages.DATABASE_CONNECTION, type='negative')
                return None
        return wrapper
    return decorator

class ErrorContext:
    """Context manager for handling errors in a block of code."""
    
    def __init__(
        self, 
        operation_name: str,
        show_loading: bool = True,
        success_message: Optional[str] = None
    ):
        self.operation_name = operation_name
        self.show_loading = show_loading
        self.success_message = success_message
        self.loading_spinner = None
    
    def __enter__(self):
        if self.show_loading:
            # Could add loading spinner here if needed
            pass
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.show_loading and self.loading_spinner:
            # Remove loading spinner
            pass
            
        if exc_type is not None:
            error_msg = f"{self.operation_name} failed: {str(exc_val)}"
            logger.error(error_msg)
            ui.notify(f"Error: {self.operation_name}", type='negative')
            return False  # Don't suppress the exception
        
        if self.success_message:
            ui.notify(self.success_message, type='positive')
        
        return True

def create_error_placeholder(message: str = ErrorMessages.NO_DATA_AVAILABLE) -> str:
    """
    Create HTML for error placeholder content.
    
    Args:
        message: Error message to display
        
    Returns:
        HTML string for error display
    """
    return f'<div class="has-text-grey-light has-text-centered p-4"><p>{message}</p></div>'

def log_performance(operation_name: str):
    """
    Decorator to log performance metrics for operations.
    
    Args:
        operation_name: Name of operation for logging
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            import time
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.info(f"{operation_name} completed in {execution_time:.2f}s")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"{operation_name} failed after {execution_time:.2f}s: {str(e)}")
                raise
        return wrapper
    return decorator

# Convenience functions for common error scenarios

def safe_file_read(file_path: str, encoding: str = 'utf-8') -> Optional[str]:
    """Safely read a file with error handling."""
    from pathlib import Path
    
    try:
        return Path(file_path).read_text(encoding=encoding)
    except FileNotFoundError:
        logger.warning(f"File not found: {file_path}")
        return None
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {str(e)}")
        return None

def safe_json_parse(json_string: str) -> Optional[dict]:
    """Safely parse JSON with error handling."""
    import json
    
    try:
        return json.loads(json_string)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing JSON: {str(e)}")
        return None

def safe_path_check(path: str) -> bool:
    """Safely check if a path exists."""
    try:
        from pathlib import Path
        return Path(path).exists()
    except Exception as e:
        logger.error(f"Error checking path {path}: {str(e)}")
        return False