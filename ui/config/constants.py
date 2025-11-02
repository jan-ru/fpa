"""
Configuration constants for the FPA application.
Centralized configuration to improve maintainability.
"""

from pathlib import Path
from typing import Dict, Any

# Application Constants
APP_VERSION = "v0.0.7"
APP_NAME = "FPA Financial Platform"
APP_TITLE = "Financial Platform Analytics"
DEFAULT_PORT = 8091

# File Paths
class Paths:
    """Centralized path configuration."""
    # Data directories
    DATA_WAREHOUSE = Path("../data/warehouse")
    DATA_RAW = Path("../data/raw")
    
    # Iceberg and pipelines
    ICEBERG_WAREHOUSE = Path("../pipelines/data/iceberg/warehouse")
    ICEBERG_LOG = ICEBERG_WAREHOUSE / "ingestion_log.txt"
    
    # dbt
    DBT_PROJECT = Path("../dbt_project")
    DBT_TARGET = DBT_PROJECT / "target"
    DBT_DOCS = DBT_TARGET / "index.html"

# UI Configuration
class UIConfig:
    """UI-related configuration constants."""
    # Table settings
    TABLE_PAGE_SIZE = 20
    TABLE_HEIGHT = "600px"
    SNAPSHOTS_DISPLAY_LIMIT = 10
    
    # Chart settings
    CHART_HEIGHT = 600
    CHART_MONTHLY_TRENDS_LIMIT = 12
    
    # Iframe settings
    IFRAME_WIDTH = "1200px"
    IFRAME_HEIGHT = "600px"
    IFRAME_MIN_WIDTH = "1200px"
    
    # Data limits
    TRANSACTIONS_DISPLAY_LIMIT = 20
    EXCEL_FILES_DISPLAY_LIMIT = 20
    ACCOUNTS_DISPLAY_LIMIT = 50
    
    # Button sizes and styles
    BUTTON_SMALL_HEIGHT = "28px"
    BUTTON_SMALL_FONT_SIZE = "0.7rem"
    BUTTON_SMALL_PADDING = "4px 8px"
    
    # Text sizes
    SMALL_TEXT_SIZE = "0.75rem"
    CARD_TEXT_SIZE = "0.8rem"
    
    # Colors
    SUCCESS_COLOR = "#28a745"
    WARNING_COLOR = "#ffc107"
    ERROR_COLOR = "#dc3545"
    PRIMARY_COLOR = "#3273dc"

# Data Processing Constants
class DataConfig:
    """Data processing configuration."""
    # Query limits
    DEFAULT_TRANSACTION_LIMIT = 1000
    DEFAULT_ACCOUNT_LIMIT = 50
    EXCEL_PREVIEW_LIMIT = 20
    
    # File size limits (MB)
    MAX_FILE_SIZE_MB = 500
    
    # Database connection
    DB_TIMEOUT_SECONDS = 30
    
    # Date formats
    DISPLAY_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Icon Configuration
class Icons:
    """Icon mappings for consistent usage."""
    SUCCESS = "fas fa-check-circle"
    WARNING = "fas fa-exclamation-triangle"
    ERROR = "fas fa-times-circle"
    INFO = "fas fa-info-circle"
    
    FILTER = "fas fa-filter"
    CLEAR = "fas fa-times"
    DOWNLOAD = "fas fa-download"
    UPLOAD = "fas fa-upload"
    
    DATABASE = "fas fa-database"
    TABLE = "fas fa-table"
    CHART = "fas fa-chart-bar"

# Error Messages
class ErrorMessages:
    """Centralized error message configuration."""
    DATABASE_CONNECTION = "Failed to connect to database"
    FILE_NOT_FOUND = "Required file not found"
    DATA_PROCESSING = "Error processing data"
    INVALID_INPUT = "Invalid input provided"
    PERMISSION_DENIED = "Permission denied"
    TIMEOUT = "Operation timed out"
    
    # User-friendly messages
    NO_DATA_AVAILABLE = "No data available to display"
    LOADING_ERROR = "Unable to load data. Please try again."
    FILTER_ERROR = "Error applying filters. Please check your selection."

# Status Messages
class StatusMessages:
    """Status and notification messages."""
    LOADING = "Loading..."
    SUCCESS = "Operation completed successfully"
    FILTERS_APPLIED = "Filters applied successfully"
    FILTERS_CLEARED = "All filters cleared"
    
    # Feature status
    FEATURE_COMING_SOON = "Feature coming soon!"
    INTEGRATION_PENDING = "Integration in progress"

# Styling Constants
class Styles:
    """CSS styling constants."""
    
    # Card styles
    CARD_CLASSES = "w-full p-4 mb-4"
    CARD_TITLE_CLASSES = "text-h6 mb-2"
    CARD_SUBTITLE_CLASSES = "text-subtitle2 mb-4"
    
    # Button styles
    SMALL_BUTTON_CLASSES = "button is-small"
    EMPHASIZED_BUTTON_CLASSES = "button is-small btn-emphasized"
    TRANSPARENT_BUTTON_CLASSES = "button is-small btn-transparent"
    
    # Text styles
    SECTION_TITLE_CLASSES = "has-text-weight-semibold mb-2"
    HELP_TEXT_CLASSES = "help"
    SMALL_TEXT_STYLE = f"font-size: {UIConfig.SMALL_TEXT_SIZE};"
    
    # Layout
    ROW_GAP_CLASSES = "gap-4"
    COLUMN_FLEX_CLASSES = "flex-1"

# Feature Flags
class Features:
    """Feature flag configuration."""
    ENABLE_PLOTLY_CHARTS = True
    ENABLE_DBT_DOCS = True
    ENABLE_ICEBERG_SNAPSHOTS = True
    ENABLE_TIME_TRAVEL = False  # Not yet implemented
    ENABLE_EXPORT_FEATURES = False  # Not yet implemented
    ENABLE_LIGHTDASH = False  # Not yet implemented

# Development Settings
class DevConfig:
    """Development and debugging configuration."""
    DEBUG_MODE = False
    SHOW_PERFORMANCE_METRICS = False
    LOG_LEVEL = "INFO"
    
    # Mock data settings
    USE_MOCK_DATA = False
    MOCK_SNAPSHOT_COUNT = 4

def get_table_config(table_type: str = "default") -> Dict[str, Any]:
    """Get standardized table configuration."""
    base_config = {
        'pagination': True,
        'paginationPageSize': UIConfig.TABLE_PAGE_SIZE,
        'defaultColDef': {
            'resizable': True,
            'sortable': True,
            'filter': True,
            'minWidth': 80,
        },
        'theme': 'alpine',
    }
    
    table_specific = {
        'transactions': {
            'rowSelection': 'multiple',
            'animateRows': True,
            'sideBar': True
        },
        'accounts': {
            'paginationPageSize': 25,
            'animateRows': True
        },
        'snapshots': {
            'paginationPageSize': 15,
        }
    }
    
    if table_type in table_specific:
        base_config.update(table_specific[table_type])
    
    return base_config

def get_button_style(button_type: str = "default") -> str:
    """Get standardized button styling."""
    styles = {
        'small': f"font-size: {UIConfig.BUTTON_SMALL_FONT_SIZE}; padding: {UIConfig.BUTTON_SMALL_PADDING}; height: {UIConfig.BUTTON_SMALL_HEIGHT};",
        'compact': f"font-size: 0.6rem; padding: 2px 4px; height: 24px;",
        'default': ""
    }
    return styles.get(button_type, styles['default'])