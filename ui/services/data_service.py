"""
Data Service Module
Handles data loading, processing, and preparation for UI components.
"""

from typing import List, Dict
from data_access import data_access
from config.data_schemas import (
    standardize_financial_data, 
    read_excel_with_schema,
    EXCEL_FILE_METADATA_SCHEMA,
    validate_schema
)
from utils.error_boundaries import (
    database_boundary, excel_boundary, file_boundary, 
    DataSourceError
)
from components.lazy_loader import lazy_data


@lazy_data('accounts')
@database_boundary('get_account_summary', fallback=[])
def get_sorted_accounts() -> List[Dict]:
    """Get account summary data sorted by account code with standardized types."""
    accounts = data_access.get_account_summary()
    if accounts:
        # Standardize data types (convert Decimals to floats)
        accounts = standardize_financial_data(accounts)
        return sorted(accounts, key=lambda x: x.get('account_code', ''))
    return []


@lazy_data('transactions')
@database_boundary('get_transaction_details', fallback=[])
def get_limited_transactions(limit: int = 20) -> List[Dict]:
    """Get transaction details with specified limit and standardized types."""
    transactions = data_access.get_transaction_details(limit=limit)
    if transactions:
        # Standardize data types
        transactions = standardize_financial_data(transactions)
    return transactions or []


@lazy_data('excel_files')
@excel_boundary('scan_directory', fallback=[])
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
            
            # Get Excel file dimensions and financial totals using polars with schema
            try:
                # Read the full file to calculate totals with proper data types
                df_full = read_excel_with_schema(str(file_path))
                n_columns = len(df_full.columns)
                n_rows = len(df_full)
                
                # Calculate debit and credit totals
                total_debit = 0.0
                total_credit = 0.0
                
                # Look for common column names for debit/credit amounts
                debit_columns = [col for col in df_full.columns if 'debit' in col.lower()]
                credit_columns = [col for col in df_full.columns if 'credit' in col.lower()]
                
                # Sum debit amounts
                for col in debit_columns:
                    try:
                        # Convert to numeric and sum, handling any non-numeric values
                        debit_sum = df_full.select(pl.col(col).cast(pl.Float64, strict=False).sum()).item()
                        if debit_sum is not None:
                            total_debit += debit_sum
                    except:
                        pass
                
                # Sum credit amounts  
                for col in credit_columns:
                    try:
                        # Convert to numeric and sum, handling any non-numeric values
                        credit_sum = df_full.select(pl.col(col).cast(pl.Float64, strict=False).sum()).item()
                        if credit_sum is not None:
                            total_credit += credit_sum
                    except:
                        pass
                
            except Exception as e:
                # If we can't read the file, set defaults
                n_columns = 0
                n_rows = 0
                total_debit = 0.0
                total_credit = 0.0
            
            excel_files.append({
                "filename": file_path.name,
                "path": str(file_path),
                "size_mb": round(stat.st_size / (1024*1024), 2),
                "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                "n_columns": n_columns,
                "n_rows": n_rows,
                "total_debit": round(total_debit, 2),
                "total_credit": round(total_credit, 2),
                "status": "Skipped" if 'DUMP2024' in file_path.name else "Processed"
            })
    
    return sorted(excel_files, key=lambda x: x["modified"], reverse=True)


@lazy_data('dbt_models')
@file_boundary('scan_dbt_models', fallback=[])
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