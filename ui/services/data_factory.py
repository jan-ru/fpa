"""
Data Service Factory for centralized data loading and management.
Provides unified access to all data sources and caching.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from utils.error_handling import safe_data_fetch, log_performance
from services.data_service import (
    get_sorted_accounts, 
    get_limited_transactions, 
    get_excel_files_data, 
    get_dbt_models_data
)


@dataclass
class FinancialOverviewData:
    """Container for financial overview data."""
    accounts: List[Dict[str, Any]]
    transactions: List[Dict[str, Any]]
    excel_files: List[Dict[str, Any]]
    
    @property
    def total_accounts(self) -> int:
        return len(self.accounts)
    
    @property
    def total_transactions(self) -> int:
        return len(self.transactions)
    
    @property
    def total_files(self) -> int:
        return len(self.excel_files)


@dataclass
class LineageData:
    """Container for data lineage information."""
    dbt_models: List[Dict[str, Any]]
    
    @property
    def total_models(self) -> int:
        return len(self.dbt_models)


class DataServiceFactory:
    """Central factory for all data service operations."""
    
    _instance = None
    _cache = {}
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    @log_performance("Financial Overview Data Loading")
    def get_financial_overview_data(self) -> FinancialOverviewData:
        """
        Get all data needed for the financial overview tab.
        
        Returns:
            FinancialOverviewData containing accounts, transactions, and files
        """
        if 'financial_overview' not in self._cache:
            accounts = get_sorted_accounts()
            transactions = get_limited_transactions()
            excel_files = get_excel_files_data()
            
            self._cache['financial_overview'] = FinancialOverviewData(
                accounts=accounts or [],
                transactions=transactions or [],
                excel_files=excel_files or []
            )
        
        return self._cache['financial_overview']
    
    @log_performance("Lineage Data Loading")
    def get_lineage_data(self) -> LineageData:
        """
        Get all data needed for the lineage tab.
        
        Returns:
            LineageData containing dbt models and dependencies
        """
        if 'lineage' not in self._cache:
            dbt_models = get_dbt_models_data()
            
            self._cache['lineage'] = LineageData(
                dbt_models=dbt_models or []
            )
        
        return self._cache['lineage']
    
    def get_accounts_data(self) -> List[Dict[str, Any]]:
        """Get account summary data."""
        try:
            return get_sorted_accounts() or []
        except Exception:
            return []
    
    def get_transactions_data(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get transaction details with specified limit."""
        try:
            return get_limited_transactions(limit=limit) or []
        except Exception:
            return []
    
    def get_files_data(self) -> List[Dict[str, Any]]:
        """Get Excel files data with processing status."""
        try:
            return get_excel_files_data() or []
        except Exception:
            return []
    
    def get_models_data(self) -> List[Dict[str, Any]]:
        """Get DBT model information."""
        try:
            return get_dbt_models_data() or []
        except Exception:
            return []
    
    def clear_cache(self, key: Optional[str] = None):
        """
        Clear cached data.
        
        Args:
            key: Specific cache key to clear, or None to clear all
        """
        if key:
            self._cache.pop(key, None)
        else:
            self._cache.clear()
    
    def refresh_financial_overview(self) -> FinancialOverviewData:
        """Force refresh of financial overview data."""
        self.clear_cache('financial_overview')
        return self.get_financial_overview_data()
    
    def refresh_lineage(self) -> LineageData:
        """Force refresh of lineage data."""
        self.clear_cache('lineage')
        return self.get_lineage_data()


# Global instance
data_factory = DataServiceFactory()


# Convenience functions for backward compatibility
def get_financial_overview_data() -> FinancialOverviewData:
    """Get financial overview data - convenience function."""
    return data_factory.get_financial_overview_data()


def get_lineage_data() -> LineageData:
    """Get lineage data - convenience function."""
    return data_factory.get_lineage_data()


def refresh_all_data():
    """Refresh all cached data."""
    data_factory.clear_cache()