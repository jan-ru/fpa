"""
Pagination service functions for handling paginated data requests.
"""

from typing import List, Dict, Tuple, Optional
from data_access import data_access
from config.data_schemas import standardize_financial_data
from utils.error_boundaries import database_boundary


@database_boundary('get_paginated_accounts', fallback=([], 0))
def get_paginated_accounts(offset: int, limit: int) -> Tuple[List[Dict], int]:
    """
    Get paginated account data with total count.
    
    Args:
        offset: Number of records to skip
        limit: Maximum number of records to return
        
    Returns:
        Tuple of (account_data, total_count)
    """
    try:
        # Get paginated accounts
        accounts = data_access.get_account_summary(limit=limit, offset=offset)
        
        # Get total count
        total_count = data_access.get_account_summary_count()
        
        # Standardize data types
        if accounts:
            accounts = standardize_financial_data(accounts)
            # Sort by account code
            accounts = sorted(accounts, key=lambda x: x.get('account_code', ''))
        
        return accounts, total_count
        
    except Exception as e:
        print(f"Error getting paginated accounts: {e}")
        return [], 0


@database_boundary('get_paginated_transactions', fallback=([], 0))
def get_paginated_transactions(offset: int, limit: int) -> Tuple[List[Dict], int]:
    """
    Get paginated transaction data with total count.
    
    Args:
        offset: Number of records to skip
        limit: Maximum number of records to return
        
    Returns:
        Tuple of (transaction_data, total_count)
    """
    try:
        # Get paginated transactions
        transactions = data_access.get_transaction_details(limit=limit, offset=offset)
        
        # Get total count
        total_count = data_access.get_transaction_details_count()
        
        # Standardize data types
        if transactions:
            transactions = standardize_financial_data(transactions)
        
        return transactions, total_count
        
    except Exception as e:
        print(f"Error getting paginated transactions: {e}")
        return [], 0


@database_boundary('get_filtered_paginated_accounts', fallback=([], 0))
def get_filtered_paginated_accounts(offset: int, limit: int) -> Tuple[List[Dict], int]:
    """
    Get paginated account data with filtering applied.
    
    Args:
        offset: Number of records to skip
        limit: Maximum number of records to return
        
    Returns:
        Tuple of (filtered_account_data, total_count)
    """
    try:
        # Check for active filters
        from utils.state_management import get_current_filter_state
        filter_state = get_current_filter_state()
        
        # If filters are active, aggregate from filtered transactions
        if filter_state.years or filter_state.months or filter_state.quarters:
            # Get all filtered transactions for aggregation
            filtered_transactions = data_access.get_filtered_transactions(
                years=list(filter_state.years) if filter_state.years else None,
                quarters=list(filter_state.quarters) if filter_state.quarters else None,
                months=list(filter_state.months) if filter_state.months else None,
                limit=50000  # Large limit to get all for aggregation
            )
            
            if filtered_transactions:
                import polars as pl
                df = pl.DataFrame(filtered_transactions)
                
                # Aggregate to account level
                accounts_df = df.group_by(["account_code", "account_name"]).agg([
                    pl.len().alias("total_transactions"),
                    pl.col("debit_amount").sum().alias("total_debit"),
                    pl.col("credit_amount").sum().alias("total_credit"),
                    pl.col("net_amount").sum().alias("net_balance"),
                ]).with_columns([
                    pl.when(pl.col("net_balance") > 0).then(pl.lit("Net Debit")).otherwise(pl.lit("Net Credit")).alias("account_balance_type"),
                    pl.lit("Active").alias("activity_status")
                ]).sort("account_code")
                
                # Get total count
                total_count = len(accounts_df)
                
                # Apply pagination to the aggregated data
                paginated_df = accounts_df.slice(offset, limit)
                accounts = paginated_df.to_dicts()
                
                # Standardize data types
                accounts = standardize_financial_data(accounts)
                
                return accounts, total_count
            else:
                return [], 0
        else:
            # No filters, use regular pagination
            return get_paginated_accounts(offset, limit)
            
    except Exception as e:
        print(f"Error getting filtered paginated accounts: {e}")
        # Fallback to regular pagination
        return get_paginated_accounts(offset, limit)


@database_boundary('get_filtered_paginated_transactions', fallback=([], 0))
def get_filtered_paginated_transactions(offset: int, limit: int) -> Tuple[List[Dict], int]:
    """
    Get paginated transaction data with filtering applied.
    
    Args:
        offset: Number of records to skip
        limit: Maximum number of records to return
        
    Returns:
        Tuple of (filtered_transaction_data, total_count)
    """
    try:
        # Check for active filters
        from utils.state_management import get_current_filter_state
        filter_state = get_current_filter_state()
        
        # If filters are active, use filtered query
        if filter_state.years or filter_state.months or filter_state.quarters:
            # First get total count of filtered transactions
            all_filtered = data_access.get_filtered_transactions(
                years=list(filter_state.years) if filter_state.years else None,
                quarters=list(filter_state.quarters) if filter_state.quarters else None,
                months=list(filter_state.months) if filter_state.months else None,
                limit=None  # No limit to get count
            )
            total_count = len(all_filtered) if all_filtered else 0
            
            # Now get paginated filtered data
            paginated_transactions = data_access.get_filtered_transactions(
                years=list(filter_state.years) if filter_state.years else None,
                quarters=list(filter_state.quarters) if filter_state.quarters else None,
                months=list(filter_state.months) if filter_state.months else None,
                limit=limit,
                offset=offset
            )
            
            # Standardize data types
            if paginated_transactions:
                paginated_transactions = standardize_financial_data(paginated_transactions)
            
            return paginated_transactions, total_count
        else:
            # No filters, use regular pagination
            return get_paginated_transactions(offset, limit)
            
    except Exception as e:
        print(f"Error getting filtered paginated transactions: {e}")
        # Fallback to regular pagination
        return get_paginated_transactions(offset, limit)