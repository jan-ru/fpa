"""
Chart components for the Financial Data Platform.
Centralized chart creation using Plotly for data visualization.
"""

from typing import List, Dict, Any, Optional
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from nicegui import ui

from config.constants import UIConfig, ErrorMessages
from utils.error_handling import safe_ui_operation, log_performance


class ChartBuilder:
    """Builder class for creating standardized charts."""
    
    def __init__(self):
        self.fig = None
        self.height = UIConfig.CHART_HEIGHT
        self.title = ""
    
    def set_height(self, height: int) -> 'ChartBuilder':
        """Set chart height."""
        self.height = height
        return self
    
    def set_title(self, title: str) -> 'ChartBuilder':
        """Set chart title."""
        self.title = title
        return self
    
    def create_subplots(self, rows: int, cols: int, subplot_titles: List[str] = None) -> 'ChartBuilder':
        """Create subplot structure."""
        self.fig = make_subplots(
            rows=rows, 
            cols=cols,
            subplot_titles=subplot_titles,
            specs=[[{"secondary_y": True} for _ in range(cols)] for _ in range(rows)]
        )
        return self
    
    def add_bar_chart(self, x_data: List, y_data: List, name: str, row: int = 1, col: int = 1) -> 'ChartBuilder':
        """Add a bar chart to the figure."""
        if self.fig is None:
            self.create_subplots(1, 1)
        
        self.fig.add_trace(
            go.Bar(x=x_data, y=y_data, name=name, marker_color='lightblue'),
            row=row, col=col
        )
        return self
    
    def add_line_chart(self, x_data: List, y_data: List, name: str, row: int = 1, col: int = 1) -> 'ChartBuilder':
        """Add a line chart to the figure."""
        if self.fig is None:
            self.create_subplots(1, 1)
        
        self.fig.add_trace(
            go.Scatter(x=x_data, y=y_data, mode='lines+markers', name=name),
            row=row, col=col
        )
        return self
    
    def add_pie_chart(self, labels: List, values: List, name: str, row: int = 1, col: int = 1) -> 'ChartBuilder':
        """Add a pie chart to the figure."""
        if self.fig is None:
            self.create_subplots(1, 1)
        
        self.fig.add_trace(
            go.Pie(labels=labels, values=values, name=name),
            row=row, col=col
        )
        return self
    
    def build(self) -> go.Figure:
        """Build and return the final figure."""
        if self.fig is None:
            raise ValueError("No chart data added. Use add_* methods first.")
        
        self.fig.update_layout(
            height=self.height,
            showlegend=True,
            title_text=self.title
        )
        
        return self.fig


@log_performance("Financial Charts Creation")
@safe_ui_operation
def create_financial_charts(data: Dict[str, List[Dict[str, Any]]]) -> Optional[ui.plotly]:
    """
    Create comprehensive financial charts from data.
    
    Args:
        data: Dictionary containing accounts, transactions, and files data
        
    Returns:
        NiceGUI plotly component or None if creation fails
    """
    accounts = data.get('accounts', [])
    transactions = data.get('transactions', [])
    
    if not accounts and not transactions:
        return ui.label(ErrorMessages.NO_DATA_AVAILABLE)
    
    try:
        chart_builder = ChartBuilder().set_height(UIConfig.CHART_HEIGHT).set_title("Financial Analytics Dashboard")
        
        # Create subplot structure
        chart_builder.create_subplots(2, 2, [
            "Account Balances", 
            "Transaction Volume", 
            "Account Type Distribution", 
            "Monthly Trends"
        ])
        
        # Chart 1: Account Balances (Bar Chart)
        if accounts:
            account_codes = [acc.get('account_code', 'Unknown') for acc in accounts[:10]]
            balances = [acc.get('balance', 0) for acc in accounts[:10]]
            chart_builder.add_bar_chart(account_codes, balances, "Account Balances", 1, 1)
        
        # Chart 2: Transaction Volume (Line Chart) 
        if transactions:
            # Group transactions by booking date
            transaction_dates = {}
            for trans in transactions:
                date = trans.get('booking_date', 'Unknown')
                amount = trans.get('amount', 0)
                if date in transaction_dates:
                    transaction_dates[date] += abs(amount)
                else:
                    transaction_dates[date] = abs(amount)
            
            dates = list(transaction_dates.keys())[:10]
            volumes = list(transaction_dates.values())[:10]
            chart_builder.add_line_chart(dates, volumes, "Daily Volume", 1, 2)
        
        # Chart 3: Account Type Distribution (Pie Chart)
        if accounts:
            account_types = {}
            for acc in accounts:
                acc_type = acc.get('account_type', 'Other')
                if acc_type in account_types:
                    account_types[acc_type] += 1
                else:
                    account_types[acc_type] = 1
            
            type_labels = list(account_types.keys())
            type_counts = list(account_types.values())
            chart_builder.add_pie_chart(type_labels, type_counts, "Account Types", 2, 1)
        
        # Chart 4: Transaction Amount Distribution
        if transactions:
            amounts = [abs(trans.get('amount', 0)) for trans in transactions]
            amount_ranges = ['0-100', '100-500', '500-1000', '1000+']
            range_counts = [
                len([a for a in amounts if 0 <= a < 100]),
                len([a for a in amounts if 100 <= a < 500]),
                len([a for a in amounts if 500 <= a < 1000]),
                len([a for a in amounts if a >= 1000])
            ]
            chart_builder.add_bar_chart(amount_ranges, range_counts, "Amount Distribution", 2, 2)
        
        fig = chart_builder.build()
        return ui.plotly(fig).classes('w-full')
        
    except Exception as e:
        return ui.label(f"Error creating charts: {str(e)}")


@safe_ui_operation
def create_plotly_sample() -> Optional[ui.plotly]:
    """
    Create a sample Plotly chart for demonstration.
    
    Returns:
        NiceGUI plotly component or None if creation fails
    """
    try:
        chart_builder = ChartBuilder().set_height(300).set_title("Sample Interactive Chart")
        chart_builder.create_subplots(1, 1)
        
        # Sample data
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun']
        revenue = [10000, 12000, 8000, 15000, 18000, 20000]
        
        chart_builder.add_line_chart(months, revenue, "Revenue Trend")
        
        fig = chart_builder.build()
        return ui.plotly(fig).classes('w-full')
        
    except Exception as e:
        return ui.label(f"Error creating sample chart: {str(e)}")


def create_simple_bar_chart(x_data: List, y_data: List, title: str = "Bar Chart") -> ui.plotly:
    """
    Create a simple bar chart.
    
    Args:
        x_data: X-axis data
        y_data: Y-axis data  
        title: Chart title
        
    Returns:
        NiceGUI plotly component
    """
    chart_builder = ChartBuilder().set_title(title)
    chart_builder.add_bar_chart(x_data, y_data, "Data")
    
    fig = chart_builder.build()
    return ui.plotly(fig).classes('w-full')


def create_simple_line_chart(x_data: List, y_data: List, title: str = "Line Chart") -> ui.plotly:
    """
    Create a simple line chart.
    
    Args:
        x_data: X-axis data
        y_data: Y-axis data
        title: Chart title
        
    Returns:
        NiceGUI plotly component
    """
    chart_builder = ChartBuilder().set_title(title)
    chart_builder.add_line_chart(x_data, y_data, "Data")
    
    fig = chart_builder.build()
    return ui.plotly(fig).classes('w-full')