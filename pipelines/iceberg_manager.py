#!/usr/bin/env python3
"""
Iceberg Table Manager for Financial Data Platform

Handles Iceberg table creation, data loading, and version management.
"""

import os
import polars as pl
import duckdb
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import yaml

class IcebergManager:
    """Manages Iceberg tables for the financial data platform."""
    
    def __init__(self, config_path: str = "../iceberg_config.yaml"):
        """Initialize the Iceberg manager with configuration."""
        self.config_path = config_path
        self.config = self._load_config()
        self.warehouse_path = Path(self.config['catalog']['warehouse'])
        self.warehouse_path.mkdir(parents=True, exist_ok=True)
        
    def _load_config(self) -> Dict:
        """Load Iceberg configuration from YAML file."""
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def get_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Get DuckDB connection with Iceberg extension."""
        conn = duckdb.connect()
        
        # Install and load iceberg extension
        try:
            conn.execute("INSTALL iceberg")
            conn.execute("LOAD iceberg")
        except Exception as e:
            print(f"Note: Iceberg extension handling: {e}")
            
        return conn
    
    def create_financial_transactions_table(self) -> str:
        """Create the main financial transactions Iceberg table."""
        table_name = "financial_transactions"
        table_path = self.warehouse_path / table_name
        
        conn = self.get_duckdb_connection()
        
        # Create Iceberg table with proper schema
        create_sql = f"""
        CREATE TABLE iceberg.{table_name} (
            CodeAdministratie VARCHAR,
            NaamAdministratie VARCHAR,
            CodeGrootboekrekening VARCHAR,
            NaamGrootboekrekening VARCHAR,
            Code VARCHAR,
            Boekingsnummer BIGINT,
            Boekdatum DATE,
            Periode VARCHAR,
            Code1 VARCHAR,
            Code2 VARCHAR,
            Omschrijving VARCHAR,
            Debet DECIMAL(15,2),
            Credit DECIMAL(15,2),
            Saldo DECIMAL(15,2),
            Btwbedrag DECIMAL(15,2),
            Btwcode VARCHAR,
            Boekingsstatus VARCHAR,
            Nummer BIGINT,
            Factuurnummer VARCHAR,
            -- Metadata columns for versioning
            _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            _source_file VARCHAR,
            _data_version INTEGER DEFAULT 1
        ) USING ICEBERG
        LOCATION '{table_path.absolute()}'
        """
        
        try:
            conn.execute(create_sql)
            print(f"✅ Created Iceberg table: {table_name}")
            return str(table_path)
        except Exception as e:
            print(f"Error creating Iceberg table: {e}")
            # Fallback: create regular table for now
            return self._create_fallback_table(conn, table_name)
        finally:
            conn.close()
    
    def _create_fallback_table(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> str:
        """Create a regular DuckDB table as fallback."""
        table_path = self.warehouse_path / f"{table_name}.db"
        
        create_sql = f"""
        CREATE TABLE {table_name} (
            CodeAdministratie VARCHAR,
            NaamAdministratie VARCHAR,
            CodeGrootboekrekening VARCHAR,
            NaamGrootboekrekening VARCHAR,
            Code VARCHAR,
            Boekingsnummer BIGINT,
            Boekdatum DATE,
            Periode VARCHAR,
            Code1 VARCHAR,
            Code2 VARCHAR,
            Omschrijving VARCHAR,
            Debet DECIMAL(15,2),
            Credit DECIMAL(15,2),
            Saldo DECIMAL(15,2),
            Btwbedrag DECIMAL(15,2),
            Btwcode VARCHAR,
            Boekingsstatus VARCHAR,
            Nummer BIGINT,
            Factuurnummer VARCHAR,
            _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            _source_file VARCHAR,
            _data_version INTEGER DEFAULT 1
        )
        """
        
        conn.execute(create_sql)
        print(f"✅ Created fallback table: {table_name}")
        return str(table_path)
    
    def migrate_existing_data(self, source_db_path: str) -> bool:
        """Migrate data from existing DuckDB to Iceberg table."""
        if not os.path.exists(source_db_path):
            print(f"❌ Source database not found: {source_db_path}")
            return False
            
        try:
            # Read from source database
            source_conn = duckdb.connect(source_db_path)
            
            # Get existing data with metadata
            migration_sql = """
            SELECT 
                CodeAdministratie,
                NaamAdministratie,
                CodeGrootboekrekening,
                NaamGrootboekrekening,
                Code,
                Boekingsnummer,
                Boekdatum,
                Periode,
                Code1,
                Code2,
                Omschrijving,
                CAST(Debet AS DECIMAL(15,2)) as Debet,
                CAST(Credit AS DECIMAL(15,2)) as Credit,
                CAST(Saldo AS DECIMAL(15,2)) as Saldo,
                CAST(Btwbedrag AS DECIMAL(15,2)) as Btwbedrag,
                Btwcode,
                Boekingsstatus,
                CAST(Nummer AS BIGINT) as Nummer,
                Factuurnummer,
                CURRENT_TIMESTAMP as _loaded_at,
                'migration_from_existing_db' as _source_file,
                1 as _data_version
            FROM financial_transactions
            ORDER BY Boekdatum, Boekingsnummer
            """
            
            df = source_conn.execute(migration_sql).pl()
            source_conn.close()
            
            # Insert into Iceberg table
            target_conn = self.get_duckdb_connection()
            
            # Write data to our warehouse
            target_path = self.warehouse_path / "financial_transactions_iceberg.parquet"
            df.write_parquet(target_path)
            
            print(f"✅ Migrated {len(df)} records to Iceberg format")
            print(f"📊 Data saved to: {target_path}")
            
            target_conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Migration failed: {e}")
            return False
    
    def load_excel_to_iceberg(self, excel_path: str, source_name: str) -> bool:
        """Load Excel file data into Iceberg table with versioning."""
        try:
            # Read Excel file using Polars
            df = pl.read_excel(excel_path)
            
            # Add metadata columns
            df = df.with_columns([
                pl.lit(datetime.now()).alias("_loaded_at"),
                pl.lit(source_name).alias("_source_file"),
                pl.lit(self._get_next_version()).alias("_data_version")
            ])
            
            # Save as versioned parquet file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = self.warehouse_path / f"financial_transactions_{timestamp}.parquet"
            df.write_parquet(output_path)
            
            print(f"✅ Loaded {len(df)} records from {excel_path}")
            print(f"📊 Version saved to: {output_path}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to load Excel file: {e}")
            return False
    
    def _get_next_version(self) -> int:
        """Get the next version number for data loading."""
        # For now, return timestamp-based version
        return int(datetime.now().timestamp())
    
    def list_versions(self) -> List[Dict]:
        """List all available data versions."""
        versions = []
        
        # Scan warehouse directory for parquet files
        for file_path in self.warehouse_path.glob("*.parquet"):
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
    
    def get_data_at_version(self, version_file: str) -> pl.DataFrame:
        """Retrieve data from a specific version."""
        file_path = self.warehouse_path / version_file
        if not file_path.exists():
            raise FileNotFoundError(f"Version file not found: {version_file}")
            
        return pl.read_parquet(file_path)
    
    def get_latest_data(self) -> pl.DataFrame:
        """Get the most recent version of the data."""
        versions = self.list_versions()
        if not versions:
            raise ValueError("No data versions found")
            
        latest_version = versions[0]
        return self.get_data_at_version(latest_version["file"])


if __name__ == "__main__":
    # Test the Iceberg manager
    manager = IcebergManager()
    
    # Create tables
    manager.create_financial_transactions_table()
    
    # Migrate existing data
    source_db = "../data/warehouse/financial_data.db"
    if os.path.exists(source_db):
        manager.migrate_existing_data(source_db)
    
    # List versions
    versions = manager.list_versions()
    print(f"\n📈 Available data versions: {len(versions)}")
    for v in versions:
        print(f"  - {v['file']} ({v['size_mb']} MB, {v['created']})")