#!/usr/bin/env python3
"""
Data Access Layer for Financial Data Platform

Connects NiceGUI application to dbt mart tables and Iceberg versioned data.
"""

import duckdb
import polars as pl
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, date
import os

class DataAccessLayer:
    """Data access layer for the financial data platform."""
    
    def __init__(self):
        """Initialize data access layer."""
        self.dbt_warehouse_path = Path("../data/warehouse/dev.duckdb")
        self.iceberg_warehouse_path = Path("../pipelines/data/iceberg/warehouse")
        
    def get_dbt_connection(self) -> duckdb.DuckDBPyConnection:
        """Get connection to dbt warehouse."""
        return duckdb.connect(str(self.dbt_warehouse_path))
    
    def execute_dbt_query(self, query: str, fetch_all: bool = True) -> Any:
        """Execute query against dbt warehouse."""
        with self.get_dbt_connection() as conn:
            result = conn.execute(query)
            return result.fetchall() if fetch_all else result.fetchone()
    
    def query_to_dict_list(self, query: str, apply_source_filter: bool = True) -> List[Dict]:
        """Execute query and convert results to list of dictionaries."""
        # Apply source file filtering if enabled
        if apply_source_filter:
            try:
                from utils.source_filter import source_filter
                query = source_filter.apply_filter_to_query(query)
            except ImportError:
                pass  # Source filtering not available
        
        with self.get_dbt_connection() as conn:
            result = conn.execute(query).fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in result]
    
    # Account Summary Methods
    def get_account_summary(self, limit: int = None, offset: int = 0) -> List[Dict]:
        """Get account summary from dbt mart."""
        query = """
        SELECT 
            account_code,
            account_name,
            total_transactions,
            total_debit,
            total_credit,
            net_balance,
            total_vat,
            net_amount_last_12_months,
            transactions_last_12_months,
            first_transaction_date,
            last_transaction_date,
            years_active,
            account_balance_type,
            activity_status,
            transaction_volume_category,
            balance_value_category,
            last_updated
        FROM mart_account_summary
        ORDER BY abs(net_balance) DESC
        """
        
        if limit is not None:
            query += f" LIMIT {limit} OFFSET {offset}"
            
        return self.query_to_dict_list(query)
    
    def get_account_summary_count(self) -> int:
        """Get total count of accounts."""
        query = "SELECT COUNT(*) as count FROM mart_account_summary"
        result = self.execute_dbt_query(query, fetch_all=False)
        return result[0] if result else 0
    
    def get_top_accounts_by_balance(self, limit: int = 10) -> List[Dict]:
        """Get top accounts by balance."""
        query = f"""
        SELECT 
            account_code,
            account_name,
            net_balance,
            account_balance_type,
            activity_status
        FROM mart_account_summary
        ORDER BY abs(net_balance) DESC
        LIMIT {limit}
        """
        return self.query_to_dict_list(query)
    
    def get_account_activity_breakdown(self) -> List[Dict]:
        """Get breakdown of accounts by activity status."""
        query = """
        SELECT 
            activity_status,
            COUNT(*) as account_count,
            SUM(net_balance) as total_balance,
            AVG(net_balance) as avg_balance
        FROM mart_account_summary
        GROUP BY activity_status
        ORDER BY account_count DESC
        """
        return self.query_to_dict_list(query)
    
    # Transaction Methods
    def get_transaction_details(self, limit: int = 1000, offset: int = 0) -> List[Dict]:
        """Get transaction details from dbt mart."""
        query = f"""
        SELECT 
            transaction_id,
            account_code,
            account_name,
            transaction_date,
            booking_number,
            description,
            debit_amount,
            credit_amount,
            net_amount,
            balance_amount,
            vat_amount,
            transaction_type,
            transaction_year,
            transaction_quarter,
            transaction_month,
            amount_category,
            recency_category,
            running_balance,
            data_quality_flag,
            source_file
        FROM mart_transaction_details
        ORDER BY transaction_date DESC, booking_number DESC
        LIMIT {limit} OFFSET {offset}
        """
        return self.query_to_dict_list(query)
    
    def get_transaction_details_count(self) -> int:
        """Get total count of transactions."""
        query = "SELECT COUNT(*) as count FROM mart_transaction_details"
        result = self.execute_dbt_query(query, fetch_all=False)
        return result[0] if result else 0
    
    def get_filtered_transactions(self, 
                                 years: Optional[List[int]] = None,
                                 quarters: Optional[List[int]] = None, 
                                 months: Optional[List[int]] = None,
                                 account_codes: Optional[List[str]] = None,
                                 amount_categories: Optional[List[str]] = None,
                                 limit: int = 1000,
                                 offset: int = 0) -> List[Dict]:
        """Get filtered transactions with enhanced filtering."""
        where_conditions = []
        
        if years:
            year_list = ', '.join(map(str, years))
            where_conditions.append(f"transaction_year IN ({year_list})")
        
        if quarters:
            quarter_list = ', '.join(map(str, quarters))
            where_conditions.append(f"transaction_quarter IN ({quarter_list})")
        
        if months:
            month_list = ', '.join(map(str, months))
            where_conditions.append(f"transaction_month IN ({month_list})")
        
        if account_codes:
            codes_str = "', '".join(account_codes)
            where_conditions.append(f"account_code IN ('{codes_str}')")
        
        if amount_categories:
            cats_str = "', '".join(amount_categories)
            where_conditions.append(f"amount_category IN ('{cats_str}')")
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        query = f"""
        SELECT 
            transaction_id,
            account_code,
            account_name,
            transaction_date,
            booking_number,
            description,
            debit_amount,
            credit_amount,
            net_amount,
            transaction_type,
            amount_category,
            recency_category,
            source_file
        FROM mart_transaction_details
        {where_clause}
        ORDER BY transaction_date DESC, booking_number DESC
        LIMIT {limit} OFFSET {offset}
        """
        return self.query_to_dict_list(query)
    
    def get_transaction_stats(self) -> Dict:
        """Get overall transaction statistics."""
        query = """
        SELECT 
            COUNT(*) as total_transactions,
            COUNT(DISTINCT account_code) as unique_accounts,
            SUM(debit_amount) as total_debit,
            SUM(credit_amount) as total_credit,
            SUM(net_amount) as net_total,
            MIN(transaction_date) as earliest_date,
            MAX(transaction_date) as latest_date,
            COUNT(DISTINCT source_file) as source_files
        FROM mart_transaction_details
        """
        result = self.query_to_dict_list(query)
        return result[0] if result else {}
    
    # Time Travel Methods
    def get_available_versions(self) -> List[Dict]:
        """Get available data versions for time travel."""
        versions = []
        if self.iceberg_warehouse_path.exists():
            for file_path in self.iceberg_warehouse_path.glob("*.parquet"):
                if "financial_transactions" in file_path.name:
                    stat = file_path.stat()
                    versions.append({
                        "file": file_path.name,
                        "path": str(file_path),
                        "size_mb": round(stat.st_size / (1024*1024), 2),
                        "created": datetime.fromtimestamp(stat.st_ctime),
                        "modified": datetime.fromtimestamp(stat.st_mtime)
                    })
        
        return sorted(versions, key=lambda x: x["created"], reverse=True)
    
    def get_data_at_version(self, version_file: str, limit: int = 1000) -> List[Dict]:
        """Get data from a specific version."""
        file_path = self.iceberg_warehouse_path / version_file
        if not file_path.exists():
            return []
        
        # Read parquet file and convert to list of dicts
        df = pl.read_parquet(file_path).limit(limit)
        return df.to_dicts()
    
    # Dashboard Methods
    def get_dashboard_stats(self) -> Dict:
        """Get key statistics for dashboard."""
        account_stats = self.execute_dbt_query("""
            SELECT 
                COUNT(*) as total_accounts,
                COUNT(CASE WHEN activity_status = 'Active' THEN 1 END) as active_accounts,
                SUM(CASE WHEN net_balance > 0 THEN net_balance ELSE 0 END) as total_assets,
                SUM(CASE WHEN net_balance < 0 THEN abs(net_balance) ELSE 0 END) as total_liabilities
            FROM mart_account_summary
        """, fetch_all=False)
        
        transaction_stats = self.get_transaction_stats()
        
        return {
            "accounts": {
                "total": account_stats[0] if account_stats else 0,
                "active": account_stats[1] if account_stats else 0,
                "assets": round(account_stats[2] or 0, 2),
                "liabilities": round(account_stats[3] or 0, 2)
            },
            "transactions": transaction_stats
        }
    
    def get_monthly_trends(self, months: int = 12) -> List[Dict]:
        """Get monthly transaction trends."""
        query = f"""
        SELECT 
            transaction_year,
            transaction_month,
            COUNT(*) as transaction_count,
            SUM(debit_amount) as total_debit,
            SUM(credit_amount) as total_credit,
            SUM(net_amount) as net_amount
        FROM mart_transaction_details
        WHERE transaction_date >= current_date - {months * 30}
        GROUP BY transaction_year, transaction_month
        ORDER BY transaction_year DESC, transaction_month DESC
        LIMIT {months}
        """
        return self.query_to_dict_list(query)
    
    # Data Refresh Methods
    def refresh_dbt_models(self) -> bool:
        """Trigger dbt model refresh (placeholder for actual implementation)."""
        # In a real implementation, this would trigger dbt run
        # For now, return True to indicate success
        return True
    
    def get_last_refresh_time(self) -> Optional[datetime]:
        """Get the last time data was refreshed."""
        try:
            query = "SELECT MAX(last_updated) FROM mart_account_summary"
            result = self.execute_dbt_query(query, fetch_all=False)
            return result[0] if result and result[0] else None
        except:
            return None


# Global instance for use throughout the application
data_access = DataAccessLayer()