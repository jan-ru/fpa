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
    # Check if there are active filters
    try:
        from utils.state_management import get_current_filter_state
        filter_state = get_current_filter_state()
        
        # If filters are active, get filtered data
        if filter_state.years or filter_state.months or filter_state.quarters:
            # Get filtered transactions first
            transactions = data_access.get_filtered_transactions(
                years=list(filter_state.years) if filter_state.years else None,
                quarters=list(filter_state.quarters) if filter_state.quarters else None,
                months=list(filter_state.months) if filter_state.months else None,
                limit=50000  # Large limit to get all for aggregation
            )
            
            if transactions:
                # Aggregate to account level
                import polars as pl
                df = pl.DataFrame(transactions)
                accounts = df.group_by(["account_code", "account_name"]).agg([
                    pl.len().alias("total_transactions"),
                    pl.col("debit_amount").sum().alias("total_debit"),
                    pl.col("credit_amount").sum().alias("total_credit"),
                    pl.col("net_amount").sum().alias("net_balance"),
                ]).with_columns([
                    pl.when(pl.col("net_balance") > 0).then(pl.lit("Net Debit")).otherwise(pl.lit("Net Credit")).alias("account_balance_type"),
                    pl.lit("Active").alias("activity_status")
                ]).to_dicts()
            else:
                accounts = []
        else:
            # No filters, get all accounts
            accounts = data_access.get_account_summary()
    except:
        # Fallback to unfiltered data if filter state is not available
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
    # Check if there are active filters
    try:
        from utils.state_management import get_current_filter_state
        filter_state = get_current_filter_state()
        
        # If filters are active, get filtered data
        if filter_state.years or filter_state.months or filter_state.quarters:
            transactions = data_access.get_filtered_transactions(
                years=list(filter_state.years) if filter_state.years else None,
                quarters=list(filter_state.quarters) if filter_state.quarters else None,
                months=list(filter_state.months) if filter_state.months else None,
                limit=limit
            )
        else:
            # No filters, get regular transaction details
            transactions = data_access.get_transaction_details(limit=limit)
    except:
        # Fallback to unfiltered data if filter state is not available
        transactions = data_access.get_transaction_details(limit=limit)
    
    if transactions:
        # Standardize data types
        transactions = standardize_financial_data(transactions)
        # Sort by Account Code, then by Transaction Date
        transactions = sorted(transactions, key=lambda x: (
            x.get('account_code', ''), 
            x.get('transaction_date', '')
        ))
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
                
                # Get column names as list for positional access
                column_names = df_full.columns
                
                # Method 1: Try specific column positions (L = column 12, 0-indexed = 11)
                # Method 2: Look for columns with 'debit' or 'credit' in name
                # Method 3: Look for numeric columns that might contain amounts
                
                # First try positional approach for column L (index 11)
                if len(column_names) > 11:  # Column L exists
                    try:
                        col_l = column_names[11]  # Column L (0-indexed)
                        debit_sum = df_full.select(pl.col(col_l).cast(pl.Float64, strict=False).sum()).item()
                        if debit_sum is not None:
                            total_debit = debit_sum
                    except:
                        pass
                
                # Try to find debit/credit columns by name if positional failed
                if total_debit == 0.0:
                    debit_columns = [col for col in column_names if any(term in col.lower() for term in ['debit', 'soll', 'amount']) and 'credit' not in col.lower()]
                    for col in debit_columns:
                        try:
                            debit_sum = df_full.select(pl.col(col).cast(pl.Float64, strict=False).sum()).item()
                            if debit_sum is not None and debit_sum > 0:
                                total_debit += debit_sum
                        except:
                            pass
                
                # Find credit columns
                credit_columns = [col for col in column_names if any(term in col.lower() for term in ['credit', 'haben', 'kredit'])]
                for col in credit_columns:
                    try:
                        credit_sum = df_full.select(pl.col(col).cast(pl.Float64, strict=False).sum()).item()
                        if credit_sum is not None and credit_sum > 0:
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
                "status": "Skipped" if 'DUMP2024' in file_path.name else "Processed",
                "source": "File System Scan"
            })
    
    # Sort by filename and update source filter
    sorted_files = sorted(excel_files, key=lambda x: x["filename"])
    
    # Update source filter with available files
    try:
        from utils.source_filter import source_filter
        filenames = [f["filename"] for f in sorted_files]
        source_filter.set_available_files(filenames)
    except ImportError:
        pass  # Source filtering not available
    
    return sorted_files


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


# Pagination wrapper functions for existing lazy-loaded data
def get_accounts_paginated(offset: int, limit: int) -> tuple[list, int]:
    """
    Get paginated account data from cached lazy-loaded data.
    
    Args:
        offset: Starting record number (0-based)
        limit: Number of records to return
        
    Returns:
        Tuple of (page_data, total_count)
    """
    # Get all cached account data
    all_accounts = get_sorted_accounts()
    
    # Calculate pagination
    total_count = len(all_accounts)
    start_idx = offset
    end_idx = offset + limit
    
    # Slice the data for current page
    page_data = all_accounts[start_idx:end_idx]
    
    return page_data, total_count


def get_transactions_paginated(offset: int, limit: int) -> tuple[list, int]:
    """
    Get paginated transaction data from cached lazy-loaded data.
    
    Args:
        offset: Starting record number (0-based)
        limit: Number of records to return
        
    Returns:
        Tuple of (page_data, total_count)
    """
    # Get all cached transaction data (use a larger limit to get more data)
    all_transactions = get_limited_transactions(limit=1000)  # Get more than the default 20
    
    # Calculate pagination
    total_count = len(all_transactions)
    start_idx = offset
    end_idx = offset + limit
    
    # Slice the data for current page
    page_data = all_transactions[start_idx:end_idx]
    
    return page_data, total_count