"""
Source file filtering system.
Allows users to select specific Excel files and filter all data accordingly.
"""

from typing import List, Set, Optional
from functools import wraps


class SourceFileFilter:
    """Manages filtering based on selected source files."""
    
    def __init__(self):
        self.selected_files: Set[str] = set()
        self.all_files: List[str] = []
        self.filter_enabled: bool = False
    
    def set_available_files(self, files: List[str]):
        """Set the list of available files."""
        self.all_files = files.copy()
        # If no files selected, select all by default
        if not self.selected_files and files:
            self.selected_files = set(files)
    
    def select_files(self, filenames: List[str]):
        """Select specific files for filtering."""
        self.selected_files = set(filenames)
        self.filter_enabled = len(self.selected_files) < len(self.all_files)
    
    def select_all_files(self):
        """Select all available files."""
        self.selected_files = set(self.all_files)
        self.filter_enabled = False
    
    def clear_selection(self):
        """Clear all selected files."""
        self.selected_files.clear()
        self.filter_enabled = True
    
    def is_file_selected(self, filename: str) -> bool:
        """Check if a file is selected."""
        return filename in self.selected_files
    
    def get_selected_files(self) -> List[str]:
        """Get list of selected files."""
        return list(self.selected_files)
    
    def get_filter_condition(self) -> Optional[str]:
        """Get SQL filter condition for selected files."""
        if not self.filter_enabled or not self.selected_files:
            return None
        
        # Create SQL IN condition for source files
        files_str = "', '".join(self.selected_files)
        return f"source_file IN ('{files_str}')"
    
    def apply_filter_to_query(self, base_query: str) -> str:
        """Apply source file filter to a SQL query."""
        filter_condition = self.get_filter_condition()
        if not filter_condition:
            return base_query
        
        # Add WHERE clause or extend existing WHERE
        if 'WHERE' in base_query.upper():
            return f"{base_query} AND {filter_condition}"
        else:
            return f"{base_query} WHERE {filter_condition}"
    
    def get_status_summary(self) -> dict:
        """Get summary of current filter status."""
        return {
            'total_files': len(self.all_files),
            'selected_files': len(self.selected_files),
            'filter_enabled': self.filter_enabled,
            'selected_file_names': list(self.selected_files)
        }


# Global filter instance
source_filter = SourceFileFilter()


def filtered_data(func):
    """Decorator to apply source file filtering to data functions."""
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Get the original data
        data = func(*args, **kwargs)
        
        # If filtering is not enabled, return all data
        if not source_filter.filter_enabled:
            return data
        
        # Apply filtering based on data type
        if isinstance(data, list) and data:
            # Check if data has source_file information
            first_item = data[0]
            if isinstance(first_item, dict) and 'source_file' in first_item:
                # Filter data by selected source files
                filtered = [
                    item for item in data 
                    if source_filter.is_file_selected(item.get('source_file', ''))
                ]
                return filtered
        
        return data
    
    return wrapper


def update_source_selection_from_ui():
    """Update source selection based on UI checkbox states."""
    # This will be called by JavaScript when checkboxes change
    from nicegui import ui
    
    # Get checked checkboxes via JavaScript
    ui.run_javascript('''
        const checkboxes = document.querySelectorAll('.row-selector:checked');
        const selectedFiles = Array.from(checkboxes).map(cb => cb.dataset.key);
        return selectedFiles;
    ''', callback=lambda result: source_filter.select_files(result or []))


def create_filter_status_indicator():
    """Create a UI indicator showing current filter status."""
    from nicegui import ui
    
    status = source_filter.get_status_summary()
    
    if status['filter_enabled']:
        filter_text = f"Filtered: {status['selected_files']}/{status['total_files']} files"
        color = "orange"
    else:
        filter_text = f"All files: {status['total_files']}"
        color = "blue"
    
    with ui.row().classes('items-center gap-2 mb-2'):
        ui.icon('filter_list').style(f'color: {color}')
        ui.label(filter_text).style(f'color: {color}; font-weight: 500;')
        
        if status['filter_enabled']:
            ui.button('Clear Filter', 
                     on_click=lambda: source_filter.select_all_files()).classes('is-small')


def add_selection_javascript():
    """Add JavaScript for handling checkbox selection."""
    from nicegui import ui
    
    # JavaScript to handle checkbox changes
    ui.run_javascript('''
        function updateSourceFilter() {
            const checkboxes = document.querySelectorAll('.row-selector');
            const checkedBoxes = document.querySelectorAll('.row-selector:checked');
            const selectedFiles = Array.from(checkedBoxes).map(cb => cb.dataset.key);
            
            // Send selected files to Python
            fetch('/update_source_filter', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({selectedFiles: selectedFiles})
            });
            
            // Update filter indicator
            const filterIndicator = document.getElementById('filter-status');
            if (filterIndicator) {
                const totalFiles = checkboxes.length;
                const selectedCount = selectedFiles.length;
                
                if (selectedCount < totalFiles && selectedCount > 0) {
                    filterIndicator.innerHTML = `<i class="fas fa-filter" style="color: orange;"></i> Filtered: ${selectedCount}/${totalFiles} files`;
                    filterIndicator.style.color = 'orange';
                } else {
                    filterIndicator.innerHTML = `<i class="fas fa-list" style="color: blue;"></i> All files: ${totalFiles}`;
                    filterIndicator.style.color = 'blue';
                }
            }
        }
        
        // Add event listeners to checkboxes
        document.addEventListener('change', function(e) {
            if (e.target.classList.contains('row-selector')) {
                updateSourceFilter();
            }
        });
        
        // Add select all/none buttons
        function addSelectionButtons() {
            const tableContainer = document.querySelector('.table-container');
            if (tableContainer && !document.getElementById('selection-controls')) {
                const controlsDiv = document.createElement('div');
                controlsDiv.id = 'selection-controls';
                controlsDiv.style.marginBottom = '10px';
                controlsDiv.innerHTML = `
                    <button onclick="selectAllFiles()" class="button is-small">Select All</button>
                    <button onclick="selectNoFiles()" class="button is-small">Select None</button>
                `;
                tableContainer.parentNode.insertBefore(controlsDiv, tableContainer);
            }
        }
        
        function selectAllFiles() {
            document.querySelectorAll('.row-selector').forEach(cb => cb.checked = true);
            updateSourceFilter();
        }
        
        function selectNoFiles() {
            document.querySelectorAll('.row-selector').forEach(cb => cb.checked = false);
            updateSourceFilter();
        }
        
        // Initialize when page loads
        setTimeout(addSelectionButtons, 500);
    ''')