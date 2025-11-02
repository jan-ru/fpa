"""
Version and Footer Utilities
Version management and footer creation functions.
"""

import yaml
from pathlib import Path
from datetime import datetime
from nicegui import ui


def get_dbt_version() -> str:
    """Get dbt project version from dbt_project.yml."""
    try:
        dbt_project_path = Path("../dbt_project/dbt_project.yml")
        if dbt_project_path.exists():
            with open(dbt_project_path, 'r') as f:
                dbt_config = yaml.safe_load(f)
                return f"v{dbt_config.get('version', '1.0.0')}"
    except Exception:
        pass
    return "v1.0.0"


def get_data_timestamp() -> str:
    """Get latest data timestamp from available Iceberg data."""
    try:
        # Look for the most recent parquet file in the iceberg warehouse
        iceberg_dir = Path("../pipelines/data/iceberg/warehouse")
        if iceberg_dir.exists():
            parquet_files = list(iceberg_dir.glob("financial_transactions_*.parquet"))
            if parquet_files:
                # Get the most recent file
                latest_file = max(parquet_files, key=lambda x: x.stat().st_mtime)
                # Extract full timestamp from filename
                filename = latest_file.stem
                if "_" in filename:
                    # Split and get the last two parts (date_time)
                    parts = filename.split("_")
                    if len(parts) >= 2:
                        # Get the last two parts: date and time
                        date_part = parts[-2]  # 20251101
                        time_part = parts[-1]  # 091325
                        return f"v{date_part}_{time_part}"
    except Exception:
        pass
    return "v20251101_091324"


def create_version_footer(app_version: str = "0.0.4") -> ui.html:
    """Create footer with comprehensive version information."""
    dbt_version = get_dbt_version()
    data_timestamp = get_data_timestamp()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M UTC")
    
    footer_html = f"""
    <div style="
        position: fixed; 
        bottom: 0; 
        left: 0; 
        width: 100%; 
        background: rgba(248, 249, 250, 0.95); 
        border-top: 1px solid #e9ecef; 
        padding: 8px 16px; 
        font-size: 11px; 
        color: #6c757d; 
        text-align: center; 
        z-index: 1000;
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    ">
        App: v{app_version} | Data: {data_timestamp} | Queries: {dbt_version} | {current_time}
    </div>
    """
    
    return ui.html(footer_html, sanitize=False)