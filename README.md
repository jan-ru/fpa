# Financial Data Viewer

A NiceGUI web application for viewing and analyzing financial data with AG-Grid, powered by DuckDB and Polars for high-performance data processing.

## Features

- **AG-Grid Interface**: Interactive data table with sorting, filtering, pagination, and selection
- **DuckDB Backend**: Fast SQL analytics engine for financial data
- **Real-time Statistics**: Database insights including totals, date ranges, and account summaries
- **Excel Import**: Loads financial data from Excel files using Polars
- **Responsive Design**: Clean, modern web interface

## Getting Started

```bash
# Install dependencies
uv install

# Run the application
uv run python main.py
```

The application will be available at http://localhost:8081

## Data Structure

Place Excel files in the `data/` directory. The application expects financial transaction data with columns for:
- Administration and account codes
- Transaction details (dates, amounts, descriptions)
- Financial amounts (debit, credit, balance, VAT)

## Tech Stack

- **NiceGUI**: Web UI framework
- **DuckDB**: Analytics database
- **Polars**: Fast DataFrame processing  
- **PyArrow**: Data interchange format
- **FastExcel**: Excel file reading
