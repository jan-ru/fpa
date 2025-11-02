"""
Data Service Module
Handles data loading, processing, and preparation for UI components.
"""

from typing import List, Dict
from data_access import data_access


def get_sorted_accounts() -> List[Dict]:
    """Get account summary data sorted by account code."""
    accounts = data_access.get_account_summary()
    if accounts:
        return sorted(accounts, key=lambda x: x.get('account_code', ''))
    return []


def get_limited_transactions(limit: int = 20) -> List[Dict]:
    """Get transaction details with specified limit."""
    return data_access.get_transaction_details(limit=limit)


def get_excel_files_data() -> List[Dict]:
    """Get Excel files data with processing status."""
    # Import here to avoid circular dependencies
    import os
    import polars as pl
    from pathlib import Path
    from datetime import datetime
    
    data_dir = Path("../data/raw")
    excel_files = []
    
    if data_dir.exists():
        for file_path in data_dir.glob("*.xlsx"):
            stat = file_path.stat()
            
            # Get Excel file dimensions using polars
            try:
                # Read just the first few rows to get dimensions efficiently
                df_sample = pl.read_excel(file_path, read_options={"n_rows": 1})
                n_columns = len(df_sample.columns)
                
                # For row count, we need to read the full file (but efficiently)
                df_full = pl.read_excel(file_path)
                n_rows = len(df_full)
                
            except Exception as e:
                # If we can't read the file, set defaults
                n_columns = 0
                n_rows = 0
            
            excel_files.append({
                "filename": file_path.name,
                "path": str(file_path),
                "size_mb": round(stat.st_size / (1024*1024), 2),
                "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                "n_columns": n_columns,
                "n_rows": n_rows,
                "status": "Skipped" if 'DUMP2024' in file_path.name else "Processed"
            })
    
    return sorted(excel_files, key=lambda x: x["modified"], reverse=True)


def get_dbt_models_data() -> List[Dict]:
    """Get DBT model information for lineage display."""
    # Import here to avoid circular dependencies
    import re
    from pathlib import Path
    
    dbt_dir = Path("../dbt_project")
    model_data = []
    
    if not dbt_dir.exists():
        return model_data
    
    try:
        # Get models from the models directory
        models_dir = dbt_dir / "models"
        if models_dir.exists():
            for sql_file in models_dir.rglob("*.sql"):
                if not sql_file.name.startswith("my_"):  # Skip example files
                    model_name = sql_file.stem
                    relative_path = sql_file.relative_to(models_dir)
                    
                    # Read the file to find dependencies
                    content = sql_file.read_text()
                    dependencies = []
                    
                    # Find ref() calls
                    refs = re.findall(r"ref\(['\"]([^'\"]+)['\"]\)", content)
                    dependencies.extend(refs)
                    
                    # Find source() calls
                    sources = re.findall(r"source\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)", content)
                    
                    model_data.append({
                        "name": model_name,
                        "type": "staging" if "staging" in str(relative_path) else 
                               "intermediate" if "intermediate" in str(relative_path) else "mart",
                        "path": str(relative_path),
                        "dependencies": ", ".join(dependencies) if dependencies else "None",
                        "sources": ", ".join([f"{s[0]}.{s[1]}" for s in sources]) if sources else "None"
                    })
        
        return model_data
        
    except Exception as e:
        return model_data