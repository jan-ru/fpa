#!/usr/bin/env python3
"""
Time Travel and Versioning Module

Provides time travel capabilities for the financial data platform.
"""

import polars as pl
from pathlib import Path
from datetime import datetime, date
from typing import Optional, List, Dict, Any
import argparse
from iceberg_manager import IcebergManager

class TimeTravel:
    """Time travel and versioning operations for financial data."""
    
    def __init__(self, iceberg_manager: IcebergManager):
        """Initialize time travel manager."""
        self.iceberg_manager = iceberg_manager
        
    def query_at_timestamp(self, timestamp: datetime) -> pl.DataFrame:
        """Get data as it existed at a specific timestamp."""
        versions = self.iceberg_manager.list_versions()
        
        # Find the latest version at or before the timestamp
        valid_versions = [v for v in versions if v["created"] <= timestamp]
        
        if not valid_versions:
            raise ValueError(f"No data available at timestamp {timestamp}")
        
        # Get the latest valid version
        target_version = max(valid_versions, key=lambda x: x["created"])
        print(f"📅 Using version: {target_version['file']} (created: {target_version['created']})")
        
        return self.iceberg_manager.get_data_at_version(target_version["file"])
    
    def query_at_date(self, target_date: date) -> pl.DataFrame:
        """Get data as it existed on a specific date."""
        # Convert date to end of day datetime
        timestamp = datetime.combine(target_date, datetime.max.time())
        return self.query_at_timestamp(timestamp)
    
    def compare_versions(self, version1: str, version2: str) -> Dict[str, Any]:
        """Compare two data versions and return differences."""
        df1 = self.iceberg_manager.get_data_at_version(version1)
        df2 = self.iceberg_manager.get_data_at_version(version2)
        
        comparison = {
            "version1": {
                "file": version1,
                "rows": len(df1),
                "columns": len(df1.columns)
            },
            "version2": {
                "file": version2,
                "rows": len(df2),
                "columns": len(df2.columns)
            },
            "differences": {}
        }
        
        # Row count difference
        row_diff = len(df2) - len(df1)
        comparison["differences"]["row_change"] = row_diff
        
        # Column differences
        cols1 = set(df1.columns)
        cols2 = set(df2.columns)
        
        comparison["differences"]["new_columns"] = list(cols2 - cols1)
        comparison["differences"]["removed_columns"] = list(cols1 - cols2)
        comparison["differences"]["common_columns"] = list(cols1 & cols2)
        
        # Data range comparison (if date columns exist)
        if "Boekdatum" in df1.columns and "Boekdatum" in df2.columns:
            date_range1 = self._get_date_range(df1)
            date_range2 = self._get_date_range(df2)
            
            comparison["differences"]["date_ranges"] = {
                "version1": date_range1,
                "version2": date_range2
            }
        
        return comparison
    
    def _get_date_range(self, df: pl.DataFrame) -> Dict[str, str]:
        """Get date range from a dataframe."""
        if "Boekdatum" not in df.columns:
            return {"min": None, "max": None}
            
        date_stats = df.select([
            pl.col("Boekdatum").min().alias("min_date"),
            pl.col("Boekdatum").max().alias("max_date")
        ]).row(0)
        
        return {
            "min": str(date_stats[0]) if date_stats[0] else None,
            "max": str(date_stats[1]) if date_stats[1] else None
        }
    
    def get_changes_since(self, since_version: str) -> Dict[str, Any]:
        """Get all changes since a specific version."""
        versions = self.iceberg_manager.list_versions()
        
        # Find the since_version in the list
        since_index = None
        for i, v in enumerate(versions):
            if v["file"] == since_version:
                since_index = i
                break
        
        if since_index is None:
            raise ValueError(f"Version not found: {since_version}")
        
        # Get all newer versions
        newer_versions = versions[:since_index]
        
        changes = {
            "since_version": since_version,
            "newer_versions": len(newer_versions),
            "versions": []
        }
        
        for v in newer_versions:
            version_info = {
                "file": v["file"],
                "created": v["created"],
                "size_mb": v["size_mb"]
            }
            
            # Get basic stats
            df = self.iceberg_manager.get_data_at_version(v["file"])
            version_info["rows"] = len(df)
            version_info["date_range"] = self._get_date_range(df)
            
            changes["versions"].append(version_info)
        
        return changes
    
    def create_consolidated_view(self, version_files: List[str]) -> pl.DataFrame:
        """Create a consolidated view from multiple versions."""
        dataframes = []
        
        for version_file in version_files:
            df = self.iceberg_manager.get_data_at_version(version_file)
            dataframes.append(df)
        
        # Concatenate all dataframes
        consolidated = pl.concat(dataframes, how="vertical_relaxed")
        
        # Remove duplicates based on business key
        business_key = ["CodeGrootboekrekening", "Boekdatum", "Boekingsnummer"]
        available_keys = [k for k in business_key if k in consolidated.columns]
        
        if available_keys:
            # Keep the latest version of each record
            consolidated = consolidated.sort("_loaded_at", descending=True)
            consolidated = consolidated.unique(subset=available_keys, keep="first")
        
        return consolidated.sort("Boekdatum")
    
    def audit_trail(self, account_code: str = None, date_from: date = None, date_to: date = None) -> pl.DataFrame:
        """Create an audit trail showing all changes to specific records."""
        versions = self.iceberg_manager.list_versions()
        audit_records = []
        
        for version in versions:
            df = self.iceberg_manager.get_data_at_version(version["file"])
            
            # Apply filters
            filtered_df = df
            
            if account_code:
                if "CodeGrootboekrekening" in df.columns:
                    filtered_df = filtered_df.filter(
                        pl.col("CodeGrootboekrekening") == account_code
                    )
            
            if date_from and "Boekdatum" in df.columns:
                filtered_df = filtered_df.filter(
                    pl.col("Boekdatum") >= date_from
                )
            
            if date_to and "Boekdatum" in df.columns:
                filtered_df = filtered_df.filter(
                    pl.col("Boekdatum") <= date_to
                )
            
            # Add version info to each record
            if len(filtered_df) > 0:
                filtered_df = filtered_df.with_columns([
                    pl.lit(version["file"]).alias("_version_file"),
                    pl.lit(version["created"]).alias("_version_created")
                ])
                audit_records.append(filtered_df)
        
        if not audit_records:
            return pl.DataFrame()
        
        # Combine all audit records
        audit_df = pl.concat(audit_records, how="vertical_relaxed")
        
        return audit_df.sort(["_version_created", "Boekdatum"])


def main():
    """CLI interface for time travel operations."""
    parser = argparse.ArgumentParser(description="Time travel operations for financial data")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Query at date command
    date_parser = subparsers.add_parser("at-date", help="Query data at specific date")
    date_parser.add_argument("date", help="Date in YYYY-MM-DD format")
    date_parser.add_argument("--output", "-o", help="Output file for results")
    
    # Compare versions command
    compare_parser = subparsers.add_parser("compare", help="Compare two versions")
    compare_parser.add_argument("version1", help="First version file")
    compare_parser.add_argument("version2", help="Second version file")
    
    # Changes since command
    changes_parser = subparsers.add_parser("changes", help="Show changes since version")
    changes_parser.add_argument("version", help="Version file to compare from")
    
    # Audit trail command
    audit_parser = subparsers.add_parser("audit", help="Create audit trail")
    audit_parser.add_argument("--account", "-a", help="Filter by account code")
    audit_parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    audit_parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    audit_parser.add_argument("--output", "-o", help="Output file for audit trail")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize managers
    iceberg_manager = IcebergManager()
    time_travel = TimeTravel(iceberg_manager)
    
    if args.command == "at-date":
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        df = time_travel.query_at_date(target_date)
        
        print(f"📊 Data at {target_date}: {len(df)} rows")
        print(f"🗓️  Date range: {time_travel._get_date_range(df)}")
        
        if args.output:
            df.write_parquet(args.output)
            print(f"💾 Saved to: {args.output}")
    
    elif args.command == "compare":
        comparison = time_travel.compare_versions(args.version1, args.version2)
        
        print(f"\\n📊 Version Comparison:")
        print(f"Version 1: {comparison['version1']}")
        print(f"Version 2: {comparison['version2']}")
        print(f"\\n🔍 Differences:")
        for key, value in comparison["differences"].items():
            print(f"  {key}: {value}")
    
    elif args.command == "changes":
        changes = time_travel.get_changes_since(args.version)
        
        print(f"\\n📈 Changes since {args.version}:")
        print(f"Newer versions: {changes['newer_versions']}")
        
        for v in changes["versions"]:
            print(f"\\n📄 {v['file']}")
            print(f"  Created: {v['created']}")
            print(f"  Rows: {v['rows']:,}")
            print(f"  Size: {v['size_mb']} MB")
            print(f"  Date Range: {v['date_range']}")
    
    elif args.command == "audit":
        date_from = datetime.strptime(args.from_date, "%Y-%m-%d").date() if args.from_date else None
        date_to = datetime.strptime(args.to_date, "%Y-%m-%d").date() if args.to_date else None
        
        audit_df = time_travel.audit_trail(args.account, date_from, date_to)
        
        print(f"\\n🔍 Audit Trail: {len(audit_df)} records")
        
        if args.output:
            audit_df.write_parquet(args.output)
            print(f"💾 Audit trail saved to: {args.output}")
        else:
            # Show summary
            if len(audit_df) > 0:
                versions = audit_df.select("_version_file").unique().sort("_version_file")
                print(f"📄 Versions involved: {len(versions)}")
                for version in versions.iter_rows():
                    print(f"  - {version[0]}")


if __name__ == "__main__":
    main()