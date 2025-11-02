"""
Data validation and sanitization utilities.
Provides comprehensive validation for financial data and user inputs.
"""

import re
import html
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from pathlib import Path

from config.constants import ErrorMessages, DataConfig


class ValidationError(Exception):
    """Custom validation error."""
    pass


class DataValidator:
    """Validates and sanitizes various types of data."""
    
    @staticmethod
    def validate_currency_amount(value: Any, allow_negative: bool = True) -> Tuple[bool, Optional[Decimal], str]:
        """
        Validate currency amount.
        
        Returns:
            Tuple of (is_valid, sanitized_value, error_message)
        """
        if value is None:
            return False, None, "Amount cannot be empty"
        
        try:
            # Convert to string and clean
            str_value = str(value).strip()
            
            # Remove currency symbols and common formatting
            cleaned = re.sub(r'[,$€£¥]', '', str_value)
            cleaned = cleaned.replace(' ', '')
            
            # Convert to Decimal for precision
            amount = Decimal(cleaned)
            
            # Check for negative values if not allowed
            if not allow_negative and amount < 0:
                return False, None, "Negative amounts are not allowed"
            
            # Check for reasonable bounds
            if abs(amount) > Decimal('999999999999.99'):
                return False, None, "Amount exceeds maximum allowed value"
            
            return True, amount, ""
            
        except (ValueError, InvalidOperation):
            return False, None, f"Invalid amount format: {value}"
    
    @staticmethod
    def validate_account_code(code: Any) -> Tuple[bool, Optional[str], str]:
        """
        Validate account code format.
        
        Returns:
            Tuple of (is_valid, sanitized_code, error_message)
        """
        if not code:
            return False, None, "Account code cannot be empty"
        
        str_code = str(code).strip().upper()
        
        # Check format (alphanumeric, hyphens, dots allowed)
        if not re.match(r'^[A-Z0-9\-\.]+$', str_code):
            return False, None, "Account code contains invalid characters"
        
        # Check length
        if len(str_code) < 2 or len(str_code) > 20:
            return False, None, "Account code must be 2-20 characters long"
        
        return True, str_code, ""
    
    @staticmethod
    def validate_date(value: Any, min_date: date = None, max_date: date = None) -> Tuple[bool, Optional[date], str]:
        """
        Validate date value.
        
        Returns:
            Tuple of (is_valid, sanitized_date, error_message)
        """
        if not value:
            return False, None, "Date cannot be empty"
        
        try:
            if isinstance(value, str):
                # Try common date formats
                formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%d %H:%M:%S']
                parsed_date = None
                
                for fmt in formats:
                    try:
                        parsed_date = datetime.strptime(value.strip(), fmt).date()
                        break
                    except ValueError:
                        continue
                
                if not parsed_date:
                    return False, None, f"Invalid date format: {value}"
                
                value = parsed_date
            
            elif isinstance(value, datetime):
                value = value.date()
            elif not isinstance(value, date):
                return False, None, f"Invalid date type: {type(value)}"
            
            # Check bounds
            if min_date and value < min_date:
                return False, None, f"Date cannot be before {min_date}"
            
            if max_date and value > max_date:
                return False, None, f"Date cannot be after {max_date}"
            
            return True, value, ""
            
        except Exception as e:
            return False, None, f"Date validation error: {str(e)}"
    
    @staticmethod
    def validate_text(value: Any, min_length: int = 0, max_length: int = 1000, allow_html: bool = False) -> Tuple[bool, Optional[str], str]:
        """
        Validate and sanitize text input.
        
        Returns:
            Tuple of (is_valid, sanitized_text, error_message)
        """
        if value is None:
            value = ""
        
        str_value = str(value).strip()
        
        # Check length
        if len(str_value) < min_length:
            return False, None, f"Text must be at least {min_length} characters long"
        
        if len(str_value) > max_length:
            return False, None, f"Text cannot exceed {max_length} characters"
        
        # Sanitize HTML if not allowed
        if not allow_html:
            str_value = html.escape(str_value)
        
        # Remove potentially dangerous characters
        str_value = re.sub(r'[<>"\']', '', str_value)
        
        return True, str_value, ""
    
    @staticmethod
    def validate_email(email: Any) -> Tuple[bool, Optional[str], str]:
        """
        Validate email address.
        
        Returns:
            Tuple of (is_valid, sanitized_email, error_message)
        """
        if not email:
            return False, None, "Email cannot be empty"
        
        str_email = str(email).strip().lower()
        
        # Basic email regex
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        if not re.match(email_pattern, str_email):
            return False, None, "Invalid email format"
        
        return True, str_email, ""
    
    @staticmethod
    def validate_file_path(path: Any, must_exist: bool = True, allowed_extensions: List[str] = None) -> Tuple[bool, Optional[Path], str]:
        """
        Validate file path.
        
        Returns:
            Tuple of (is_valid, sanitized_path, error_message)
        """
        if not path:
            return False, None, "File path cannot be empty"
        
        try:
            path_obj = Path(str(path))
            
            # Check if file exists
            if must_exist and not path_obj.exists():
                return False, None, f"File does not exist: {path}"
            
            # Check extension
            if allowed_extensions:
                if path_obj.suffix.lower() not in [ext.lower() for ext in allowed_extensions]:
                    return False, None, f"File extension must be one of: {', '.join(allowed_extensions)}"
            
            # Security check - prevent path traversal
            try:
                path_obj.resolve().relative_to(Path.cwd())
            except ValueError:
                return False, None, "Path outside allowed directory"
            
            return True, path_obj, ""
            
        except Exception as e:
            return False, None, f"Invalid file path: {str(e)}"


class FinancialDataValidator:
    """Specialized validator for financial data."""
    
    @staticmethod
    def validate_transaction(transaction: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[str]]:
        """
        Validate a financial transaction record.
        
        Returns:
            Tuple of (is_valid, sanitized_transaction, error_messages)
        """
        errors = []
        sanitized = {}
        
        # Required fields
        required_fields = ['account_code', 'transaction_date', 'description']
        for field in required_fields:
            if field not in transaction or not transaction[field]:
                errors.append(f"Missing required field: {field}")
        
        # Validate account code
        if 'account_code' in transaction:
            is_valid, value, error = DataValidator.validate_account_code(transaction['account_code'])
            if is_valid:
                sanitized['account_code'] = value
            else:
                errors.append(f"Account code: {error}")
        
        # Validate date
        if 'transaction_date' in transaction:
            min_date = date(1900, 1, 1)
            max_date = date.today()
            is_valid, value, error = DataValidator.validate_date(
                transaction['transaction_date'], min_date, max_date
            )
            if is_valid:
                sanitized['transaction_date'] = value
            else:
                errors.append(f"Transaction date: {error}")
        
        # Validate description
        if 'description' in transaction:
            is_valid, value, error = DataValidator.validate_text(
                transaction['description'], min_length=1, max_length=500
            )
            if is_valid:
                sanitized['description'] = value
            else:
                errors.append(f"Description: {error}")
        
        # Validate amounts
        amount_fields = ['debit_amount', 'credit_amount', 'net_amount']
        for field in amount_fields:
            if field in transaction and transaction[field] is not None:
                is_valid, value, error = DataValidator.validate_currency_amount(transaction[field])
                if is_valid:
                    sanitized[field] = value
                else:
                    errors.append(f"{field}: {error}")
        
        # Validate that at least one amount is provided
        has_amount = any(field in sanitized for field in amount_fields)
        if not has_amount:
            errors.append("At least one amount field (debit, credit, or net) must be provided")
        
        # Copy other valid fields
        for key, value in transaction.items():
            if key not in sanitized and key not in ['account_code', 'transaction_date', 'description'] + amount_fields:
                sanitized[key] = value
        
        return len(errors) == 0, sanitized, errors
    
    @staticmethod
    def validate_account(account: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[str]]:
        """
        Validate an account record.
        
        Returns:
            Tuple of (is_valid, sanitized_account, error_messages)
        """
        errors = []
        sanitized = {}
        
        # Validate account code
        if 'account_code' in account:
            is_valid, value, error = DataValidator.validate_account_code(account['account_code'])
            if is_valid:
                sanitized['account_code'] = value
            else:
                errors.append(f"Account code: {error}")
        else:
            errors.append("Missing required field: account_code")
        
        # Validate account name
        if 'account_name' in account:
            is_valid, value, error = DataValidator.validate_text(
                account['account_name'], min_length=1, max_length=200
            )
            if is_valid:
                sanitized['account_name'] = value
            else:
                errors.append(f"Account name: {error}")
        
        # Validate balance amounts
        balance_fields = ['total_debit', 'total_credit', 'net_balance']
        for field in balance_fields:
            if field in account and account[field] is not None:
                is_valid, value, error = DataValidator.validate_currency_amount(account[field])
                if is_valid:
                    sanitized[field] = value
                else:
                    errors.append(f"{field}: {error}")
        
        # Copy other fields
        for key, value in account.items():
            if key not in sanitized:
                sanitized[key] = value
        
        return len(errors) == 0, sanitized, errors


class DataSanitizer:
    """Sanitizes data for safe storage and display."""
    
    @staticmethod
    def sanitize_for_display(data: Any) -> str:
        """Sanitize data for safe HTML display."""
        if data is None:
            return ""
        
        str_data = str(data)
        
        # Escape HTML
        str_data = html.escape(str_data)
        
        # Remove potentially dangerous characters
        str_data = re.sub(r'[<>"\']', '', str_data)
        
        return str_data
    
    @staticmethod
    def sanitize_for_sql(data: Any) -> str:
        """Sanitize data for SQL queries (basic protection)."""
        if data is None:
            return "NULL"
        
        str_data = str(data)
        
        # Escape single quotes
        str_data = str_data.replace("'", "''")
        
        # Remove SQL injection patterns
        dangerous_patterns = [
            r';.*DROP',
            r';.*DELETE',
            r';.*UPDATE',
            r';.*INSERT',
            r'UNION.*SELECT',
            r'--',
            r'/\*.*\*/'
        ]
        
        for pattern in dangerous_patterns:
            str_data = re.sub(pattern, '', str_data, flags=re.IGNORECASE)
        
        return f"'{str_data}'"
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """Sanitize filename for safe file system operations."""
        # Remove path separators and dangerous characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        # Remove control characters
        sanitized = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', sanitized)
        
        # Limit length
        if len(sanitized) > 255:
            name, ext = Path(sanitized).stem, Path(sanitized).suffix
            sanitized = name[:255-len(ext)] + ext
        
        return sanitized


class FilterValidator:
    """Validates filter inputs."""
    
    @staticmethod
    def validate_year_filter(years: List[Any]) -> Tuple[bool, List[int], str]:
        """Validate year filter values."""
        if not years:
            return True, [], ""
        
        validated_years = []
        
        for year in years:
            try:
                year_int = int(year)
                if 1900 <= year_int <= 2100:
                    validated_years.append(year_int)
                else:
                    return False, [], f"Year {year_int} is out of valid range (1900-2100)"
            except (ValueError, TypeError):
                return False, [], f"Invalid year value: {year}"
        
        return True, validated_years, ""
    
    @staticmethod
    def validate_month_filter(months: List[Any]) -> Tuple[bool, List[int], str]:
        """Validate month filter values."""
        if not months:
            return True, [], ""
        
        validated_months = []
        
        for month in months:
            try:
                month_int = int(month)
                if 1 <= month_int <= 12:
                    validated_months.append(month_int)
                else:
                    return False, [], f"Month {month_int} is out of valid range (1-12)"
            except (ValueError, TypeError):
                return False, [], f"Invalid month value: {month}"
        
        return True, validated_months, ""
    
    @staticmethod
    def validate_quarter_filter(quarters: List[Any]) -> Tuple[bool, List[int], str]:
        """Validate quarter filter values."""
        if not quarters:
            return True, [], ""
        
        validated_quarters = []
        
        for quarter in quarters:
            try:
                quarter_int = int(quarter)
                if 1 <= quarter_int <= 4:
                    validated_quarters.append(quarter_int)
                else:
                    return False, [], f"Quarter {quarter_int} is out of valid range (1-4)"
            except (ValueError, TypeError):
                return False, [], f"Invalid quarter value: {quarter}"
        
        return True, validated_quarters, ""


def validate_data_batch(
    data: List[Dict[str, Any]], 
    validator_func: Callable[[Dict[str, Any]], Tuple[bool, Dict[str, Any], List[str]]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    """
    Validate a batch of data records.
    
    Returns:
        Tuple of (valid_records, invalid_records, summary_stats)
    """
    valid_records = []
    invalid_records = []
    total_errors = 0
    
    for i, record in enumerate(data):
        is_valid, sanitized_record, errors = validator_func(record)
        
        if is_valid:
            valid_records.append(sanitized_record)
        else:
            invalid_records.append({
                'original_record': record,
                'errors': errors,
                'row_number': i + 1
            })
            total_errors += len(errors)
    
    summary = {
        'total_records': len(data),
        'valid_records': len(valid_records),
        'invalid_records': len(invalid_records),
        'total_errors': total_errors,
        'validation_rate': len(valid_records) / len(data) if data else 0
    }
    
    return valid_records, invalid_records, summary


def create_validation_report(invalid_records: List[Dict[str, Any]]) -> str:
    """Create a human-readable validation report."""
    if not invalid_records:
        return "All records passed validation."
    
    report = f"Validation Report - {len(invalid_records)} invalid records found:\n\n"
    
    for record in invalid_records[:10]:  # Limit to first 10 for readability
        report += f"Row {record['row_number']}:\n"
        for error in record['errors']:
            report += f"  - {error}\n"
        report += "\n"
    
    if len(invalid_records) > 10:
        report += f"... and {len(invalid_records) - 10} more invalid records.\n"
    
    return report