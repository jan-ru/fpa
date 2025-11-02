"""
Data schema definitions for consistent data type handling.
Defines expected data types for all data sources.
"""

import polars as pl
from typing import Dict, Any

# Excel file schemas
EXCEL_FINANCIAL_SCHEMA = {
    # Common financial Excel columns
    'account_code': pl.Utf8,  # Keep as string to preserve leading zeros
    'account_name': pl.Utf8,
    'transaction_date': pl.Date,
    'booking_date': pl.Date,
    'description': pl.Utf8,
    'amount': pl.Float64,
    'debit_amount': pl.Float64,
    'credit_amount': pl.Float64,
    'balance': pl.Float64,
    'reference': pl.Utf8,
    'booking_number': pl.Utf8,
    'transaction_id': pl.Utf8,
}

# Database output schemas
DATABASE_ACCOUNT_SCHEMA = {
    'account_code': pl.Utf8,
    'account_name': pl.Utf8,
    'total_transactions': pl.Int64,
    'total_debit': pl.Float64,  # Convert Decimal to Float64 
    'total_credit': pl.Float64,
    'net_balance': pl.Float64,
    'total_vat': pl.Float64,
    'net_amount_last_12_months': pl.Float64,
    'transactions_last_12_months': pl.Int64,
    'first_transaction_date': pl.Date,
    'last_transaction_date': pl.Date,
    'years_active': pl.Int64,
    'account_balance_type': pl.Utf8,
    'activity_status': pl.Utf8,
    'transaction_volume_category': pl.Utf8,
    'balance_value_category': pl.Utf8,
    'last_updated': pl.Datetime,
}

DATABASE_TRANSACTION_SCHEMA = {
    'account_code': pl.Utf8,
    'account_name': pl.Utf8,
    'transaction_date': pl.Date,
    'booking_date': pl.Date,
    'description': pl.Utf8,
    'debit_amount': pl.Float64,
    'credit_amount': pl.Float64,
    'net_amount': pl.Float64,
    'booking_number': pl.Utf8,
    'reference': pl.Utf8,
    'transaction_id': pl.Utf8,
}

# File metadata schema
EXCEL_FILE_METADATA_SCHEMA = {
    'filename': pl.Utf8,
    'size_mb': pl.Float64,
    'n_columns': pl.Int64,
    'n_rows': pl.Int64,
    'total_debit': pl.Float64,
    'total_credit': pl.Float64,
    'modified': pl.Utf8,  # Keep as string for display formatting
    'status': pl.Utf8,
    'path': pl.Utf8,
}

# DBT model schema
DBT_MODEL_SCHEMA = {
    'name': pl.Utf8,
    'type': pl.Utf8,
    'path': pl.Utf8,
    'dependencies': pl.Utf8,
    'sources': pl.Utf8,
}

# Iceberg snapshot schema
ICEBERG_SNAPSHOT_SCHEMA = {
    'snapshot_id': pl.Utf8,
    'timestamp': pl.Utf8,
    'source_file': pl.Utf8,
    'parquet_file': pl.Utf8,
    'records_count': pl.Int64,
    'file_size_mb': pl.Float64,
    'operation': pl.Utf8,
}


def get_excel_read_options(file_path: str) -> Dict[str, Any]:
    """
    Get standardized options for reading Excel files with proper data types.
    
    Args:
        file_path: Path to the Excel file
        
    Returns:
        Dictionary of read options for polars.read_excel()
    """
    return {
        'schema_overrides': EXCEL_FINANCIAL_SCHEMA,
        'read_csv_options': {
            'ignore_errors': True,  # Continue reading even with type errors
            'null_values': ['', 'NULL', 'null', 'None', 'N/A', '#N/A'],
        },
        'infer_schema_length': 1000,  # Sample more rows for better type inference
    }


def standardize_decimal_columns(df: pl.DataFrame, decimal_columns: list) -> pl.DataFrame:
    """
    Convert Decimal columns to Float64 for consistent handling.
    
    Args:
        df: Polars DataFrame with potential Decimal columns
        decimal_columns: List of column names that should be Float64
        
    Returns:
        DataFrame with standardized numeric types
    """
    try:
        expressions = []
        for col in decimal_columns:
            if col in df.columns:
                # Convert Decimal/string to Float64
                expressions.append(
                    pl.col(col).cast(pl.Float64, strict=False).alias(col)
                )
        
        if expressions:
            return df.with_columns(expressions)
        return df
    except Exception as e:
        print(f"Warning: Could not standardize decimal columns: {e}")
        return df


def validate_schema(df: pl.DataFrame, expected_schema: Dict[str, pl.DataType], 
                   strict: bool = False, source_name: str = "Data") -> pl.DataFrame:
    """
    Validate and optionally fix DataFrame schema.
    
    Args:
        df: DataFrame to validate
        expected_schema: Expected column types
        strict: If True, raise error on mismatch. If False, attempt to fix.
        source_name: Name of data source for error messages
        
    Returns:
        DataFrame with corrected schema
    """
    validation_errors = []
    
    if strict:
        # Strict validation - collect all errors before raising
        for col, expected_type in expected_schema.items():
            if col in df.columns:
                actual_type = df[col].dtype
                if actual_type != expected_type:
                    validation_errors.append(
                        f"Column '{col}': found {actual_type}, expected {expected_type}"
                    )
        
        if validation_errors:
            error_msg = f"{source_name} schema validation failed:\n" + "\n".join(validation_errors)
            raise ValueError(error_msg)
    else:
        # Lenient validation - attempt to cast to expected types
        expressions = []
        cast_errors = []
        
        for col, expected_type in expected_schema.items():
            if col in df.columns:
                try:
                    expressions.append(
                        pl.col(col).cast(expected_type, strict=False).alias(col)
                    )
                except Exception as e:
                    cast_errors.append(f"Column '{col}': {str(e)}")
        
        if expressions:
            try:
                df = df.with_columns(expressions)
            except Exception as e:
                print(f"Warning: {source_name} type conversion partially failed: {e}")
        
        if cast_errors:
            print(f"Warning: {source_name} had casting issues:\n" + "\n".join(cast_errors))
    
    return df


# Convenience functions for common operations
def read_excel_with_schema(file_path: str) -> pl.DataFrame:
    """Read Excel file with predefined schema and error handling."""
    try:
        options = get_excel_read_options(file_path)
        df = pl.read_excel(file_path, **options)
        return validate_schema(df, EXCEL_FINANCIAL_SCHEMA, strict=False)
    except Exception as e:
        print(f"Error reading Excel file {file_path}: {e}")
        # Fallback to basic read
        return pl.read_excel(file_path)


def standardize_financial_data(data: list) -> list:
    """
    Standardize financial data from any source to consistent types.
    
    Args:
        data: List of dictionaries with financial data
        
    Returns:
        List of dictionaries with standardized numeric types
    """
    if not data:
        return data
    
    # Convert to DataFrame for easier processing
    df = pl.DataFrame(data)
    
    # Identify financial columns
    financial_columns = [col for col in df.columns 
                        if any(keyword in col.lower() 
                              for keyword in ['debit', 'credit', 'balance', 'amount', 'vat'])]
    
    # Standardize financial columns to Float64
    df = standardize_decimal_columns(df, financial_columns)
    
    # Convert back to list of dicts
    return df.to_dicts()