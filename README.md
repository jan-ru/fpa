# Financial Data Platform v0.0.4

Modern financial data analysis platform with integrated dashboard, analytics, and lineage visualization. Built with NiceGUI, dbt, DuckDB, and Apache Iceberg for enterprise-grade data management.

**v0.0.4**: Enhanced with Bulma CSS styling, modular architecture, and improved code maintainability.

## 🚀 Key Features

### 📊 **Unified Dashboard**
- Real-time filtered statistics for accounts and transactions  
- Professional Bulma-styled tables with consistent formatting
- Dynamic filtering (years, quarters, months) with instant updates
- Enhanced UI with professional button styling and date pickers

### 📈 **Advanced Analytics** 
- Plotly-powered interactive charts and visualizations
- Lightdash integration mockup for BI reporting
- Financial statement preparation (Balance Sheet, Income, Cash Flow)
- Trend analysis and KPI monitoring

### 🔍 **Data Lineage & Documentation**
- Embedded dbt model lineage visualization
- Interactive dependency mapping and model details
- Direct access to dbt documentation and column lineage
- Visual data flow from sources to marts

### 🗂️ **Enhanced Data Management**
- Excel file inventory with detailed metrics (columns, rows, size)
- Clean, streamlined interface without unnecessary filters
- Processing status tracking and file analysis
- Iceberg-based version control and time travel
- dbt-powered data transformations

## 🚀 Quick Start

### Prerequisites
- Python 3.12+ (compatible with 3.13)
- uv package manager

### Installation & Setup
```bash
# Install dependencies
uv sync

# Run the application (v0.0.4)
cd ui && uv run python main_v0_0_4.py

# Or run v0.0.3 (legacy)
cd ui && uv run python main_v0_0_3.py
```

**Access at:** 
- v0.0.4: http://localhost:8091
- v0.0.3: http://localhost:8088

### Application Tabs
- **Inputs**: Enhanced Excel file inventory with Bulma tables
- **Dashboard**: Professional UI with filtered statistics and enhanced styling  
- **Analytics**: Interactive charts with Lightdash integration mockups
- **Lineage**: dbt model visualization with consistent table styling
- **Admin**: System administration interface

## 📁 Project Structure

```
financial-data-platform/
├── data/                          # Data storage
│   ├── raw/                      # Excel source files
│   ├── iceberg/warehouse/        # Versioned Iceberg tables
│   └── warehouse/                # DuckDB databases
├── dbt_project/                  # dbt transformation project
│   ├── models/
│   │   ├── staging/             # Data cleaning & standardization
│   │   ├── intermediate/        # Business logic & aggregations
│   │   └── marts/              # Final analytics tables
│   └── tests/                   # Data quality tests
├── pipelines/                    # Data ingestion & management
│   ├── iceberg_manager.py       # Iceberg operations
│   ├── ingest_excel.py         # Excel file processing
│   └── time_travel.py          # Version control & time travel
├── ui/                          # NiceGUI application
│   ├── main_v0_0_4.py         # v0.0.4 with Bulma CSS & modular architecture
│   ├── main_v0_0_3.py         # v0.0.3 legacy version
│   ├── components/            # UI components (tables, forms, buttons)
│   ├── services/              # Data processing services
│   ├── utils/                 # Utility functions (filters, stats, version)
│   ├── config/                # Application configuration
│   ├── data_access.py         # Data access layer
│   └── data_refresh.py        # Automated refresh workflows
└── docs/                        # Documentation
```

## 💾 Data Management

### Excel File Processing
```bash
# Import new Excel files
cd pipelines && uv run python ingest_excel.py

# List available data versions
uv run python ingest_excel.py --list

# Process specific file
uv run python ingest_excel.py --file new_data.xlsx
```

### Time Travel Operations
```bash
# Query data at specific date
cd pipelines && uv run python time_travel.py at-date 2024-01-01

# Compare two versions
uv run python time_travel.py compare version1.parquet version2.parquet

# Show changes since version
uv run python time_travel.py changes base_version.parquet
```

### dbt Operations
```bash
# Run data transformations
cd dbt_project && uv run dbt run

# Test data quality
uv run dbt test

# Generate documentation
uv run dbt docs generate && uv run dbt docs serve
```

## 🏦 Current Data Status

- **📊 Accounts**: 260 accounts with comprehensive analytics
- **💳 Transactions**: 42,000+ enriched transaction records
- **🕐 Versions**: 5 historical data versions available
- **📈 Coverage**: Financial data spanning 2020-2025
- **✅ Quality**: All data quality tests passing

## 🛠️ Tech Stack

### Core Technologies
- **[NiceGUI](https://nicegui.io/)**: Modern Python web UI framework
- **[dbt](https://www.getdbt.com/)**: Data transformation and modeling  
- **[DuckDB](https://duckdb.org/)**: High-performance analytics database
- **[Apache Iceberg](https://iceberg.apache.org/)**: Table format with versioning
- **[Polars](https://pola.rs/)**: Fast DataFrame library
- **[Plotly](https://plotly.com/python/)**: Interactive visualization library

### Supporting Tools
- **Bulma CSS**: Professional component styling framework
- **PyArrow**: Data interchange between systems
- **FastExcel**: Efficient Excel file processing  
- **uv**: Fast Python package manager

## 📚 Documentation

- **[Migration Guide](docs/MIGRATION_GUIDE.md)**: Phase A implementation details
- **[Phase B Complete](docs/PHASE_B_COMPLETE.md)**: Iceberg & dbt setup
- **[Phase C Complete](docs/PHASE_C_COMPLETE.md)**: UI integration & workflows

## 🔧 Administration

### Data Refresh
Use the **Admin** tab in the web interface or:
```bash
# Full data pipeline refresh
# (Available through UI - Admin tab)

# Quick dbt model refresh only
cd dbt_project && uv run dbt run
```

### System Health
- Navigate to **Admin** tab for system status
- Monitor data versions and refresh timestamps
- Access data quality test results

## 🎯 Key Benefits

✅ **Enterprise-Grade**: Production-ready data platform  
✅ **Version Control**: Complete data history and audit trails  
✅ **Time Travel**: Query data at any historical point  
✅ **Quality Assurance**: Automated testing and validation  
✅ **Performance**: Sub-second queries on large datasets  
✅ **User-Friendly**: Intuitive interface for non-technical users  

## 🚀 Future Enhancements

- Advanced visualizations and charting
- Multi-user authentication and role-based access
- API development for external integrations
- Scheduled automation and alerting
- Advanced forecasting and anomaly detection

---

**Version**: 0.0.4  
**Status**: ✅ Production Ready  
**Last Updated**: November 2025  
**Python Version**: 3.12+ (3.13 compatible)  
**Architecture**: Modular with DRY principles
