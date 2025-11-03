"""
Tests for dbt utilities functionality.
"""

import pytest
from unittest.mock import patch, mock_open, Mock
from pathlib import Path
from utils.dbt_utils import get_dbt_command_status, get_all_dbt_command_status, get_dbt_run_status


class TestDbtUtils:
    """Test dbt utility functions."""
    
    def test_get_dbt_command_status_no_log_file(self):
        """Test command status when log file doesn't exist."""
        with patch('utils.dbt_utils.Paths') as mock_paths:
            # Create a mock log file path
            mock_log_path = Mock()
            mock_log_path.exists.return_value = False
            
            # Create a proper path mock that handles the / operations
            mock_logs_path = Mock()
            mock_logs_path.__truediv__ = Mock(return_value=mock_log_path)
            
            mock_dbt_project = Mock()
            mock_dbt_project.__truediv__ = Mock(return_value=mock_logs_path)
            
            mock_paths.DBT_PROJECT = mock_dbt_project
            
            result = get_dbt_command_status('run')
            
            assert result['last_run'] == 'No log file found'
            assert result['status'] == 'unknown'
            assert result['icon'] == 'fas fa-question-circle'
    
    def test_get_dbt_command_status_successful_run(self):
        """Test command status for successful dbt run."""
        log_content = """
        [2024-01-01T10:00:00.000000] Command `dbt debug` succeeded at 10:00:00
        [2024-01-01T11:00:00.000000] Command `dbt run` succeeded at 11:00:00
        [2024-01-01T12:00:00.000000] Command `dbt test` succeeded at 12:00:00
        """
        
        with patch('utils.dbt_utils.Paths') as mock_paths:
            # Create a mock log file path
            mock_log_path = Mock()
            mock_log_path.exists.return_value = True
            mock_log_path.stat.return_value.st_mtime = 1704110400  # 2024-01-01 12:00:00
            
            # Create a proper path mock that handles the / operations
            mock_logs_path = Mock()
            mock_logs_path.__truediv__ = Mock(return_value=mock_log_path)
            
            mock_dbt_project = Mock()
            mock_dbt_project.__truediv__ = Mock(return_value=mock_logs_path)
            
            mock_paths.DBT_PROJECT = mock_dbt_project
            
            with patch('builtins.open', mock_open(read_data=log_content)):
                result = get_dbt_command_status('run')
                
                assert '✓ dbt run at' in result['last_run']
                assert '11:00:00' in result['last_run']
                assert result['status'] == 'success'
                assert result['icon'] == 'fas fa-check-circle'
    
    def test_get_dbt_command_status_failed_run(self):
        """Test command status for failed dbt run."""
        log_content = """
        [2024-01-01T10:00:00.000000] Command `dbt debug` succeeded at 10:00:00
        [2024-01-01T11:00:00.000000] Command `dbt run` failed at 11:00:00
        """
        
        with patch('utils.dbt_utils.Paths') as mock_paths:
            # Create a mock log file path
            mock_log_path = Mock()
            mock_log_path.exists.return_value = True
            mock_log_path.stat.return_value.st_mtime = 1704106800  # 2024-01-01 11:00:00
            
            # Create a proper path mock that handles the / operations
            mock_logs_path = Mock()
            mock_logs_path.__truediv__ = Mock(return_value=mock_log_path)
            
            mock_dbt_project = Mock()
            mock_dbt_project.__truediv__ = Mock(return_value=mock_logs_path)
            
            mock_paths.DBT_PROJECT = mock_dbt_project
            
            with patch('builtins.open', mock_open(read_data=log_content)):
                result = get_dbt_command_status('run')
                
                assert '✗ dbt run at' in result['last_run']
                assert '11:00:00' in result['last_run']
                assert result['status'] == 'error'
                assert result['icon'] == 'fas fa-times-circle'
    
    def test_get_dbt_command_status_command_not_found(self):
        """Test command status when specific command is not found in log."""
        log_content = """
        [2024-01-01T10:00:00.000000] Command `dbt debug` succeeded at 10:00:00
        [2024-01-01T11:00:00.000000] Command `dbt test` succeeded at 11:00:00
        """
        
        with patch('utils.dbt_utils.Paths') as mock_paths:
            # Create a mock log file path
            mock_log_path = Mock()
            mock_log_path.exists.return_value = True
            mock_log_path.stat.return_value.st_mtime = 1704106800
            
            # Create a proper path mock that handles the / operations
            mock_logs_path = Mock()
            mock_logs_path.__truediv__ = Mock(return_value=mock_log_path)
            
            mock_dbt_project = Mock()
            mock_dbt_project.__truediv__ = Mock(return_value=mock_logs_path)
            
            mock_paths.DBT_PROJECT = mock_dbt_project
            
            with patch('builtins.open', mock_open(read_data=log_content)):
                result = get_dbt_command_status('run')  # 'run' not in log
                
                assert result['last_run'] == 'No dbt run found'
                assert result['status'] == 'info'
                assert result['icon'] == 'fas fa-info-circle'
    
    def test_get_all_dbt_command_status(self):
        """Test getting status for all three dbt commands."""
        log_content = """
        [2024-01-01T10:00:00.000000] Command `dbt debug` succeeded at 10:00:00
        [2024-01-01T11:00:00.000000] Command `dbt run` succeeded at 11:00:00
        [2024-01-01T12:00:00.000000] Command `dbt test` failed at 12:00:00
        """
        
        with patch('utils.dbt_utils.Paths') as mock_paths:
            # Create a mock log file path
            mock_log_path = Mock()
            mock_log_path.exists.return_value = True
            mock_log_path.stat.return_value.st_mtime = 1704110400
            
            # Create a proper path mock that handles the / operations
            mock_logs_path = Mock()
            mock_logs_path.__truediv__ = Mock(return_value=mock_log_path)
            
            mock_dbt_project = Mock()
            mock_dbt_project.__truediv__ = Mock(return_value=mock_logs_path)
            
            mock_paths.DBT_PROJECT = mock_dbt_project
            
            with patch('builtins.open', mock_open(read_data=log_content)):
                result = get_all_dbt_command_status()
                
                # Check all three commands are returned
                assert 'debug' in result
                assert 'run' in result
                assert 'test' in result
                
                # Check debug command (successful)
                assert '✓ dbt debug at' in result['debug']['last_run']
                assert result['debug']['status'] == 'success'
                
                # Check run command (successful)
                assert '✓ dbt run at' in result['run']['last_run']
                assert result['run']['status'] == 'success'
                
                # Check test command (failed)
                assert '✗ dbt test at' in result['test']['last_run']
                assert result['test']['status'] == 'error'
    
    def test_get_dbt_run_status_legacy_function(self):
        """Test the legacy get_dbt_run_status function still works."""
        log_content = """
        [2024-01-01T11:00:00.000000] Command `dbt run` succeeded at 11:00:00
        """
        
        with patch('utils.dbt_utils.Paths') as mock_paths:
            # Create a mock log file path
            mock_log_path = Mock()
            mock_log_path.exists.return_value = True
            mock_log_path.stat.return_value.st_mtime = 1704106800
            
            # Create a proper path mock that handles the / operations
            mock_logs_path = Mock()
            mock_logs_path.__truediv__ = Mock(return_value=mock_log_path)
            
            mock_dbt_project = Mock()
            mock_dbt_project.__truediv__ = Mock(return_value=mock_logs_path)
            
            mock_paths.DBT_PROJECT = mock_dbt_project
            
            with patch('builtins.open', mock_open(read_data=log_content)):
                result = get_dbt_run_status()
                
                assert '✓ dbt run at' in result['last_run']
                assert result['status'] == 'success'
                assert result['icon'] == 'fas fa-check-circle'
    
    def test_get_dbt_command_status_with_exception(self):
        """Test command status when file reading raises exception."""
        with patch('utils.dbt_utils.Paths') as mock_paths:
            # Create a mock log file path
            mock_log_path = Mock()
            mock_log_path.exists.return_value = True
            
            # Create a proper path mock that handles the / operations
            mock_logs_path = Mock()
            mock_logs_path.__truediv__ = Mock(return_value=mock_log_path)
            
            mock_dbt_project = Mock()
            mock_dbt_project.__truediv__ = Mock(return_value=mock_logs_path)
            
            mock_paths.DBT_PROJECT = mock_dbt_project
            
            with patch('builtins.open', side_effect=Exception("File read error")):
                result = get_dbt_command_status('run')
                
                assert result['last_run'] == 'Error reading log'
                assert result['status'] == 'error'
                assert result['icon'] == 'fas fa-exclamation-triangle'