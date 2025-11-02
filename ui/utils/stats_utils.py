"""
Statistics Utilities
Functions for calculating and updating dashboard statistics.
"""

from typing import Dict, Set
import polars as pl
from data_access import data_access


def get_filtered_stats(selected_years: Set, selected_months: Set, selected_quarters: Set) -> Dict:
    """Get statistics for currently filtered data."""
    if not any([selected_years, selected_months, selected_quarters]):
        # No filters, return overall stats
        return data_access.get_dashboard_stats()
    
    # Get filtered transactions
    filtered_transactions = data_access.get_filtered_transactions(
        years=list(selected_years) if selected_years else None,
        quarters=list(selected_quarters) if selected_quarters else None,
        months=list(selected_months) if selected_months else None,
        limit=50000  # High limit to get all for stats
    )
    
    if not filtered_transactions:
        return {
            "accounts": {"total": 0, "active": 0, "assets": 0, "liabilities": 0},
            "transactions": {"total_transactions": 0, "unique_accounts": 0, 
                           "total_debit": 0, "total_credit": 0, "net_total": 0}
        }
    
    # Calculate filtered stats
    df = pl.DataFrame(filtered_transactions)
    unique_accounts = df["account_code"].n_unique()
    total_transactions = len(df)
    total_debit = df["debit_amount"].sum()
    total_credit = df["credit_amount"].sum()
    net_total = df["net_amount"].sum()
    
    # Get unique accounts with their balances
    account_balances = df.group_by("account_code").agg([
        pl.col("net_amount").sum().alias("balance")
    ])
    
    total_assets = account_balances.filter(pl.col("balance") > 0)["balance"].sum() or 0
    total_liabilities = abs(account_balances.filter(pl.col("balance") < 0)["balance"].sum() or 0)
    
    return {
        "accounts": {
            "total": unique_accounts,
            "active": unique_accounts,  # All filtered accounts considered active
            "assets": total_assets,
            "liabilities": total_liabilities
        },
        "transactions": {
            "total_transactions": total_transactions,
            "unique_accounts": unique_accounts,
            "total_debit": total_debit,
            "total_credit": total_credit,
            "net_total": net_total
        }
    }


def update_dashboard_stats(stats_cards: Dict, selected_years: Set, selected_months: Set, selected_quarters: Set):
    """Update dashboard statistics cards."""
    stats = get_filtered_stats(selected_years, selected_months, selected_quarters)
    
    if "total_accounts_card" in stats_cards:
        stats_cards["total_accounts_card"].text = f"Total Accounts: {stats['accounts']['total']:,}"
    if "active_accounts_card" in stats_cards:
        stats_cards["active_accounts_card"].text = f"Active Accounts: {stats['accounts']['active']:,}"
    if "total_assets_card" in stats_cards:
        stats_cards["total_assets_card"].text = f"Total Assets: €{stats['accounts']['assets']:,.2f}"
    if "total_liabilities_card" in stats_cards:
        stats_cards["total_liabilities_card"].text = f"Total Liabilities: €{stats['accounts']['liabilities']:,.2f}"
    if "total_transactions_card" in stats_cards:
        stats_cards["total_transactions_card"].text = f"Total Transactions: {stats['transactions']['total_transactions']:,}"
    if "total_debit_card" in stats_cards:
        stats_cards["total_debit_card"].text = f"Total Debit: €{stats['transactions']['total_debit']:,.2f}"