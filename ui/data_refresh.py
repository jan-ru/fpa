#!/usr/bin/env python3
"""
Data Refresh Workflow for Financial Data Platform

Handles data ingestion, dbt model refresh, and UI updates.
"""

import asyncio
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import sys
import os

class DataRefreshManager:
    """Manages data refresh workflows."""
    
    def __init__(self):
        """Initialize refresh manager."""
        self.project_root = Path(__file__).parent.parent
        self.pipelines_dir = self.project_root / "pipelines"
        self.dbt_dir = self.project_root / "dbt_project"
        
    async def refresh_all_data(self, progress_callback=None) -> Dict:
        """Complete data refresh workflow."""
        results = {
            "start_time": datetime.now(),
            "steps": [],
            "success": True,
            "error": None
        }
        
        try:
            # Step 1: Check for new Excel files
            if progress_callback:
                await progress_callback("🔍 Checking for new Excel files...")
            
            excel_check = await self._check_new_excel_files()
            results["steps"].append({
                "step": "check_excel",
                "success": excel_check["success"],
                "message": excel_check["message"],
                "details": excel_check.get("details", {})
            })
            
            # Step 2: Run Excel ingestion if needed
            if excel_check["has_new_files"]:
                if progress_callback:
                    await progress_callback("📁 Ingesting new Excel files...")
                
                ingest_result = await self._run_excel_ingestion()
                results["steps"].append({
                    "step": "ingest_excel",
                    "success": ingest_result["success"],
                    "message": ingest_result["message"],
                    "details": ingest_result.get("details", {})
                })
            else:
                results["steps"].append({
                    "step": "ingest_excel",
                    "success": True,
                    "message": "No new Excel files to process",
                    "details": {}
                })
            
            # Step 3: Refresh dbt models
            if progress_callback:
                await progress_callback("🔄 Refreshing dbt models...")
            
            dbt_result = await self._run_dbt_refresh()
            results["steps"].append({
                "step": "dbt_refresh", 
                "success": dbt_result["success"],
                "message": dbt_result["message"],
                "details": dbt_result.get("details", {})
            })
            
            # Step 4: Run data quality tests
            if progress_callback:
                await progress_callback("🧪 Running data quality tests...")
            
            test_result = await self._run_dbt_tests()
            results["steps"].append({
                "step": "data_tests",
                "success": test_result["success"],
                "message": test_result["message"],
                "details": test_result.get("details", {})
            })
            
            if progress_callback:
                await progress_callback("✅ Data refresh completed!")
            
        except Exception as e:
            results["success"] = False
            results["error"] = str(e)
            
            if progress_callback:
                await progress_callback(f"❌ Error: {str(e)}")
        
        results["end_time"] = datetime.now()
        results["duration"] = (results["end_time"] - results["start_time"]).total_seconds()
        
        return results
    
    async def _check_new_excel_files(self) -> Dict:
        """Check for new Excel files to process."""
        try:
            raw_data_dir = self.project_root / "data" / "raw"
            
            if not raw_data_dir.exists():
                return {
                    "success": True,
                    "has_new_files": False,
                    "message": "No raw data directory found",
                    "details": {}
                }
            
            excel_files = list(raw_data_dir.glob("*.xlsx"))
            
            # Check ingestion log to see what's been processed
            log_file = self.pipelines_dir / "data" / "iceberg" / "warehouse" / "ingestion_log.txt"
            
            processed_files = set()
            if log_file.exists():
                with open(log_file, 'r') as f:
                    for line in f:
                        if '|' in line:
                            filename = line.split('|')[1].strip()
                            processed_files.add(filename)
            
            new_files = [f for f in excel_files if f.name not in processed_files]
            
            return {
                "success": True,
                "has_new_files": len(new_files) > 0,
                "message": f"Found {len(new_files)} new Excel files to process",
                "details": {
                    "total_files": len(excel_files),
                    "new_files": [f.name for f in new_files],
                    "processed_files": list(processed_files)
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "has_new_files": False,
                "message": f"Error checking Excel files: {str(e)}",
                "details": {}
            }
    
    async def _run_excel_ingestion(self) -> Dict:
        """Run Excel file ingestion."""
        try:
            # Run the ingestion script
            process = await asyncio.create_subprocess_exec(
                sys.executable, "ingest_excel.py",
                cwd=self.pipelines_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                output = stdout.decode()
                # Parse output for summary
                lines = output.split('\\n')
                summary_lines = [line for line in lines if 'Processed:' in line or 'Failed:' in line]
                
                return {
                    "success": True,
                    "message": "Excel ingestion completed successfully",
                    "details": {
                        "output": output,
                        "summary": summary_lines
                    }
                }
            else:
                return {
                    "success": False,
                    "message": f"Excel ingestion failed: {stderr.decode()}",
                    "details": {"stderr": stderr.decode()}
                }
                
        except Exception as e:
            return {
                "success": False,
                "message": f"Error running Excel ingestion: {str(e)}",
                "details": {}
            }
    
    async def _run_dbt_refresh(self) -> Dict:
        """Run dbt model refresh."""
        try:
            # Run dbt models
            process = await asyncio.create_subprocess_exec(
                "uv", "run", "dbt", "run",
                cwd=self.dbt_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                output = stdout.decode()
                # Parse for success/failure counts
                lines = output.split('\\n')
                done_line = [line for line in lines if 'Done.' in line]
                
                return {
                    "success": True,
                    "message": "dbt models refreshed successfully",
                    "details": {
                        "output": output,
                        "summary": done_line[0] if done_line else "Models refreshed"
                    }
                }
            else:
                return {
                    "success": False,
                    "message": f"dbt refresh failed: {stderr.decode()}",
                    "details": {"stderr": stderr.decode()}
                }
                
        except Exception as e:
            return {
                "success": False,
                "message": f"Error running dbt refresh: {str(e)}",
                "details": {}
            }
    
    async def _run_dbt_tests(self) -> Dict:
        """Run dbt data quality tests."""
        try:
            # Run dbt tests
            process = await asyncio.create_subprocess_exec(
                "uv", "run", "dbt", "test",
                cwd=self.dbt_dir,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            
            output = stdout.decode()
            
            # Parse test results
            lines = output.split('\\n')
            done_line = [line for line in lines if 'Done.' in line]
            
            # dbt test returns 0 even with test failures, so check output
            has_failures = any('FAIL' in line for line in lines)
            
            return {
                "success": not has_failures,
                "message": "Data quality tests completed" if not has_failures else "Some data quality tests failed",
                "details": {
                    "output": output,
                    "summary": done_line[0] if done_line else "Tests completed"
                }
            }
                
        except Exception as e:
            return {
                "success": False,
                "message": f"Error running dbt tests: {str(e)}",
                "details": {}
            }
    
    async def quick_refresh_dbt_only(self) -> Dict:
        """Quick refresh of just dbt models (no Excel ingestion)."""
        results = {
            "start_time": datetime.now(),
            "success": True,
            "error": None
        }
        
        try:
            dbt_result = await self._run_dbt_refresh()
            results.update(dbt_result)
            
        except Exception as e:
            results["success"] = False
            results["error"] = str(e)
        
        results["end_time"] = datetime.now()
        results["duration"] = (results["end_time"] - results["start_time"]).total_seconds()
        
        return results
    
    async def get_refresh_status(self) -> Dict:
        """Get current refresh status and last refresh info."""
        try:
            # Check dbt warehouse last modified time
            dbt_db_path = self.project_root / "data" / "warehouse" / "dev.duckdb"
            
            if dbt_db_path.exists():
                last_modified = datetime.fromtimestamp(dbt_db_path.stat().st_mtime)
            else:
                last_modified = None
            
            # Check for running processes (simplified)
            return {
                "last_refresh": last_modified,
                "is_refreshing": False,  # Would need more complex tracking
                "status": "ready"
            }
            
        except Exception as e:
            return {
                "last_refresh": None,
                "is_refreshing": False,
                "status": f"error: {str(e)}"
            }


# Global instance
refresh_manager = DataRefreshManager()