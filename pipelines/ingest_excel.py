#!/usr/bin/env python3
"""
Excel to Iceberg Ingestion Pipeline

Automated pipeline for loading Excel financial data into versioned Iceberg tables.
"""

import os
import sys
from pathlib import Path
import polars as pl
from datetime import datetime
import argparse
from iceberg_manager import IcebergManager

class ExcelIngestionPipeline:
    """Pipeline for ingesting Excel files into Iceberg tables."""
    
    def __init__(self, iceberg_manager: IcebergManager):
        """Initialize the ingestion pipeline."""
        self.iceberg_manager = iceberg_manager
        self.raw_data_path = Path("../data/raw")
        
    def discover_excel_files(self) -> list[Path]:
        """Discover all Excel files in the raw data directory."""
        excel_files = []
        for pattern in ["*.xlsx", "*.xls"]:
            excel_files.extend(self.raw_data_path.glob(pattern))
        
        return sorted(excel_files)
    
    def validate_excel_schema(self, df: pl.DataFrame, file_path: Path) -> bool:
        """Validate that Excel file has the expected schema."""
        expected_columns = {
            'CodeAdministratie', 'NaamAdministratie', 'CodeGrootboekrekening',
            'NaamGrootboekrekening', 'Code', 'Boekingsnummer', 'Boekdatum',
            'Periode', 'Code1', 'Code2', 'Omschrijving', 'Debet', 'Credit',
            'Saldo', 'Btwbedrag', 'Btwcode', 'Boekingsstatus', 'Nummer',
            'Factuurnummer'
        }
        
        actual_columns = set(df.columns)
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        if missing_columns:
            print(f"⚠️  {file_path.name}: Missing columns: {missing_columns}")
            
        if extra_columns:
            print(f"ℹ️  {file_path.name}: Extra columns: {extra_columns}")
            
        # Accept if we have at least the core columns
        core_columns = {'CodeGrootboekrekening', 'Boekdatum', 'Debet', 'Credit'}
        has_core = core_columns.issubset(actual_columns)
        
        return has_core
    
    def clean_and_transform(self, df: pl.DataFrame, source_file: str) -> pl.DataFrame:
        """Clean and transform Excel data for Iceberg storage."""
        
        # Ensure required columns exist
        required_columns = [
            'CodeAdministratie', 'NaamAdministratie', 'CodeGrootboekrekening',
            'NaamGrootboekrekening', 'Code', 'Boekingsnummer', 'Boekdatum',
            'Periode', 'Code1', 'Code2', 'Omschrijving', 'Debet', 'Credit',
            'Saldo', 'Btwbedrag', 'Btwcode', 'Boekingsstatus', 'Nummer',
            'Factuurnummer'
        ]
        
        # Add missing columns with null values
        for col in required_columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        
        # Clean and cast data types
        df = df.with_columns([
            # String columns
            pl.col("CodeAdministratie").cast(pl.Utf8, strict=False),
            pl.col("NaamAdministratie").cast(pl.Utf8, strict=False),
            pl.col("CodeGrootboekrekening").cast(pl.Utf8, strict=False),
            pl.col("NaamGrootboekrekening").cast(pl.Utf8, strict=False),
            pl.col("Code").cast(pl.Utf8, strict=False),
            pl.col("Periode").cast(pl.Utf8, strict=False),
            pl.col("Code1").cast(pl.Utf8, strict=False),
            pl.col("Code2").cast(pl.Utf8, strict=False),
            pl.col("Omschrijving").cast(pl.Utf8, strict=False),
            pl.col("Btwcode").cast(pl.Utf8, strict=False),
            pl.col("Boekingsstatus").cast(pl.Utf8, strict=False),
            pl.col("Factuurnummer").cast(pl.Utf8, strict=False),
            
            # Numeric columns
            pl.col("Boekingsnummer").cast(pl.Int64, strict=False),
            pl.col("Nummer").cast(pl.Int64, strict=False),
            pl.col("Debet").cast(pl.Float64, strict=False),
            pl.col("Credit").cast(pl.Float64, strict=False),
            pl.col("Saldo").cast(pl.Float64, strict=False),
            pl.col("Btwbedrag").cast(pl.Float64, strict=False),
            
            # Date column
            pl.col("Boekdatum").cast(pl.Date, strict=False),
        ])
        
        # Add metadata columns
        df = df.with_columns([
            pl.lit(datetime.now()).alias("_loaded_at"),
            pl.lit(source_file).alias("_source_file"),
            pl.lit(int(datetime.now().timestamp())).alias("_data_version")
        ])
        
        # Remove rows with null dates (invalid records)
        df = df.filter(pl.col("Boekdatum").is_not_null())
        
        return df
    
    def ingest_file(self, file_path: Path, force: bool = False) -> bool:
        """Ingest a single Excel file into Iceberg storage."""
        print(f"\\n🔄 Processing: {file_path.name}")
        
        try:
            # Check if already processed
            if not force and self._is_file_processed(file_path):
                print(f"⏭️  {file_path.name}: Already processed (use --force to reprocess)")
                return True
            
            # Read Excel file
            print(f"📖 Reading Excel file...")
            df = pl.read_excel(file_path)
            print(f"📊 Read {len(df)} rows, {len(df.columns)} columns")
            
            # Validate schema
            if not self.validate_excel_schema(df, file_path):
                print(f"❌ {file_path.name}: Schema validation failed")
                return False
            
            # Clean and transform
            print(f"🧹 Cleaning and transforming data...")
            df_clean = self.clean_and_transform(df, file_path.name)
            
            valid_rows = len(df_clean)
            if valid_rows == 0:
                print(f"❌ {file_path.name}: No valid rows after cleaning")
                return False
            
            print(f"✅ {valid_rows} valid rows after cleaning")
            
            # Save to versioned Iceberg storage
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"financial_transactions_{file_path.stem}_{timestamp}.parquet"
            output_path = self.iceberg_manager.warehouse_path / output_file
            
            df_clean.write_parquet(output_path)
            print(f"💾 Saved to: {output_file}")
            
            # Update processing log
            self._log_processed_file(file_path, output_file, valid_rows)
            
            return True
            
        except Exception as e:
            print(f"❌ Error processing {file_path.name}: {e}")
            return False
    
    def _is_file_processed(self, file_path: Path) -> bool:
        """Check if file has already been processed."""
        log_file = self.iceberg_manager.warehouse_path / "ingestion_log.txt"
        if not log_file.exists():
            return False
            
        with open(log_file, 'r') as f:
            content = f.read()
            return file_path.name in content
    
    def _log_processed_file(self, file_path: Path, output_file: str, row_count: int):
        """Log processed file information."""
        log_file = self.iceberg_manager.warehouse_path / "ingestion_log.txt"
        
        log_entry = f"{datetime.now().isoformat()} | {file_path.name} | {output_file} | {row_count} rows\\n"
        
        with open(log_file, 'a') as f:
            f.write(log_entry)
    
    def ingest_all(self, force: bool = False) -> dict:
        """Ingest all Excel files in the raw data directory."""
        excel_files = self.discover_excel_files()
        print(f"🔍 Found {len(excel_files)} Excel files")
        
        results = {
            "processed": 0,
            "skipped": 0,
            "failed": 0,
            "files": []
        }
        
        for file_path in excel_files:
            success = self.ingest_file(file_path, force)
            
            if success:
                results["processed"] += 1
                results["files"].append({"file": file_path.name, "status": "success"})
            else:
                results["failed"] += 1
                results["files"].append({"file": file_path.name, "status": "failed"})
        
        # Summary
        print(f"\\n📈 Ingestion Summary:")
        print(f"  ✅ Processed: {results['processed']}")
        print(f"  ⏭️  Skipped: {results['skipped']}")
        print(f"  ❌ Failed: {results['failed']}")
        
        return results


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Ingest Excel files into Iceberg tables")
    parser.add_argument("--file", "-f", help="Process specific file")
    parser.add_argument("--force", action="store_true", help="Force reprocessing of already processed files")
    parser.add_argument("--list", "-l", action="store_true", help="List available versions")
    
    args = parser.parse_args()
    
    # Initialize managers
    iceberg_manager = IcebergManager()
    pipeline = ExcelIngestionPipeline(iceberg_manager)
    
    if args.list:
        versions = iceberg_manager.list_versions()
        print(f"\\n📊 Available data versions: {len(versions)}")
        for v in versions:
            print(f"  📄 {v['file']} ({v['size_mb']} MB, {v['created']})")
        return
    
    if args.file:
        # Process specific file
        file_path = Path(args.file)
        if not file_path.exists():
            file_path = pipeline.raw_data_path / args.file
            
        if not file_path.exists():
            print(f"❌ File not found: {args.file}")
            sys.exit(1)
            
        pipeline.ingest_file(file_path, args.force)
    else:
        # Process all files
        pipeline.ingest_all(args.force)


if __name__ == "__main__":
    main()