"""
DBT utilities for extracting runtime information.
"""

import re
from pathlib import Path
from datetime import datetime
from typing import Optional
from config.constants import Paths


def get_dbt_last_run() -> str:
    """
    Extract the last run time from dbt log file.
    
    Returns:
        Formatted string with last run information
    """
    dbt_log_path = Paths.DBT_PROJECT / "logs" / "dbt.log"
    
    if not dbt_log_path.exists():
        return "No log file found"
    
    try:
        # Read the last few lines of the log file for performance
        with open(dbt_log_path, 'r') as f:
            lines = f.readlines()
        
        # Look for the last successful or failed command
        last_command = None
        last_time = None
        
        # Search backwards through the log for command completion
        for line in reversed(lines[-100:]):  # Check last 100 lines
            # Look for command completion patterns
            if 'Command `dbt' in line and ('failed at' in line or 'succeeded at' in line):
                # Extract command and time
                command_match = re.search(r'Command `(dbt [^`]+)`', line)
                time_match = re.search(r'at (\d{2}:\d{2}:\d{2})', line)
                
                if command_match and time_match:
                    last_command = command_match.group(1)
                    last_time = time_match.group(1)
                    success = 'succeeded' in line
                    break
        
        if last_command and last_time:
            # Get file modification time for date
            mod_time = datetime.fromtimestamp(dbt_log_path.stat().st_mtime)
            status = "✓" if success else "✗"
            return f"{status} {last_command} at {mod_time.strftime('%Y-%m-%d')} {last_time}"
        else:
            # Fallback to file modification time
            mod_time = datetime.fromtimestamp(dbt_log_path.stat().st_mtime)
            return f"Last activity: {mod_time.strftime('%Y-%m-%d %H:%M')}"
    
    except Exception as e:
        return f"Error reading log: {str(e)[:20]}..."


def get_dbt_run_status() -> dict:
    """
    Get detailed dbt run status information.
    
    Returns:
        Dictionary with status details
    """
    dbt_log_path = Paths.DBT_PROJECT / "logs" / "dbt.log"
    
    if not dbt_log_path.exists():
        return {
            'last_run': 'No log file found',
            'status': 'unknown',
            'icon': 'fas fa-question-circle'
        }
    
    try:
        with open(dbt_log_path, 'r') as f:
            lines = f.readlines()
        
        # Look for recent command completions
        recent_commands = []
        
        for line in reversed(lines[-200:]):  # Check last 200 lines
            if 'Command `dbt' in line and ('failed at' in line or 'succeeded at' in line):
                command_match = re.search(r'Command `(dbt [^`]+)`', line)
                time_match = re.search(r'at (\d{2}:\d{2}:\d{2})', line)
                
                if command_match and time_match:
                    success = 'succeeded' in line
                    recent_commands.append({
                        'command': command_match.group(1),
                        'time': time_match.group(1),
                        'success': success
                    })
        
        if recent_commands:
            last_run = recent_commands[0]
            mod_time = datetime.fromtimestamp(dbt_log_path.stat().st_mtime)
            
            if last_run['success']:
                return {
                    'last_run': f"✓ {last_run['command']} at {mod_time.strftime('%m-%d')} {last_run['time']}",
                    'status': 'success',
                    'icon': 'fas fa-check-circle'
                }
            else:
                return {
                    'last_run': f"✗ {last_run['command']} at {mod_time.strftime('%m-%d')} {last_run['time']}",
                    'status': 'error', 
                    'icon': 'fas fa-times-circle'
                }
        else:
            mod_time = datetime.fromtimestamp(dbt_log_path.stat().st_mtime)
            return {
                'last_run': f"Activity: {mod_time.strftime('%m-%d %H:%M')}",
                'status': 'info',
                'icon': 'fas fa-info-circle'
            }
    
    except Exception as e:
        return {
            'last_run': f"Error reading log",
            'status': 'error',
            'icon': 'fas fa-exclamation-triangle'
        }


def get_dbt_command_status(command_type: str) -> dict:
    """
    Get status for a specific dbt command (debug, run, or test).
    
    Args:
        command_type: One of 'debug', 'run', or 'test'
    
    Returns:
        Dictionary with command-specific status details
    """
    dbt_log_path = Paths.DBT_PROJECT / "logs" / "dbt.log"
    
    if not dbt_log_path.exists():
        return {
            'last_run': 'No log file found',
            'status': 'unknown',
            'icon': 'fas fa-question-circle'
        }
    
    try:
        with open(dbt_log_path, 'r') as f:
            lines = f.readlines()
        
        # Look for the most recent run of the specific command
        for line in reversed(lines[-300:]):  # Check last 300 lines
            if f'Command `dbt {command_type}' in line and ('failed at' in line or 'succeeded at' in line):
                command_match = re.search(r'Command `(dbt [^`]+)`', line)
                time_match = re.search(r'at (\d{2}:\d{2}:\d{2})', line)
                
                if command_match and time_match:
                    success = 'succeeded' in line
                    mod_time = datetime.fromtimestamp(dbt_log_path.stat().st_mtime)
                    
                    if success:
                        return {
                            'last_run': f"✓ dbt {command_type} at {mod_time.strftime('%m-%d')} {time_match.group(1)}",
                            'status': 'success',
                            'icon': 'fas fa-check-circle'
                        }
                    else:
                        return {
                            'last_run': f"✗ dbt {command_type} at {mod_time.strftime('%m-%d')} {time_match.group(1)}",
                            'status': 'error',
                            'icon': 'fas fa-times-circle'
                        }
        
        # No specific command found
        return {
            'last_run': f"No dbt {command_type} found",
            'status': 'info',
            'icon': 'fas fa-info-circle'
        }
    
    except Exception as e:
        return {
            'last_run': f"Error reading log",
            'status': 'error',
            'icon': 'fas fa-exclamation-triangle'
        }


def get_all_dbt_command_status() -> dict:
    """
    Get status for all three dbt commands: debug, run, and test.
    
    Returns:
        Dictionary with status for each command
    """
    return {
        'debug': get_dbt_command_status('debug'),
        'run': get_dbt_command_status('run'), 
        'test': get_dbt_command_status('test')
    }