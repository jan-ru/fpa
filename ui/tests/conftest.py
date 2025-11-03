"""
Pytest configuration and fixtures for the Financial Data Platform tests.
"""

import pytest
import sys
from pathlib import Path

# Add the ui directory to the Python path
ui_dir = Path(__file__).parent.parent
sys.path.insert(0, str(ui_dir))

@pytest.fixture
def sample_excel_data():
    """Sample Excel file data for testing."""
    return [
        {
            "filename": "DUMP2021_24feb25.xlsx",
            "size_mb": 2.5,
            "n_columns": 15,
            "n_rows": 1000,
            "total_debit": 50000.00,
            "total_credit": 45000.00,
            "modified": "2025-01-01 10:00",
            "status": "Processed"
        },
        {
            "filename": "DUMP2022_24feb25.xlsx", 
            "size_mb": 3.2,
            "n_columns": 16,
            "n_rows": 1200,
            "total_debit": 65000.00,
            "total_credit": 58000.00,
            "modified": "2025-01-02 11:00",
            "status": "Processed"
        }
    ]

@pytest.fixture
def sample_account_data():
    """Sample account data for testing."""
    return [
        {
            "account_code": "80",
            "account_name": "Ontwikkelingskosten",
            "total_transactions": 167,
            "total_debit": 9348770.89,
            "total_credit": 696284.18,
            "net_balance": 8652486.71
        },
        {
            "account_code": "81",
            "account_name": "Cum. afschrijving ontwikkelingskosten",
            "total_transactions": 49,
            "total_debit": 50830.00,
            "total_credit": 6800926.00,
            "net_balance": -6750096.00
        }
    ]