import pyspark

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.dataframe import Column

from functools import wraps

import seaborn as sns
import plotly.graph_objects as go
import matplotlib.pyplot as plt

import pandas as pd
import plotly.express as px
import numpy as np
from typing import Union, List, Optional, Callable
from typing import Union, Optional, List, Any, Dict

from plotly.subplots import make_subplots

import re
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from pyspark.sql import DataFrame
from pyspark.sql.column import Column


__spark__ = SparkSession.getActiveSession()


def bar(self, y:str, x:str=None,  _global_fig = None, /,
        color=None, title=None, sort_by=None, ascending=False):
    """
    Add a bar plot for the specified column to the current figure.
    
    Parameters:
    -----------
    y, x : str
        Name of the columns to plot
    color : str, optional
        Color for the bars
    title : str, optional
        Title for the plot
    sort_by : str, optional
        Column name to sort by (defaults to None, maintaining original order)
    ascending : bool, optional
        Sort in ascending order if True (default False)
    
    Returns:
    --------
    self : DataFrame
        Returns self for method chaining
    """

    # Convert to pandas if it's a Spark DataFrame
    pdf = self.select(*[x,y]).toPandas()
    
    # Create a new figure if none exists
    if _global_fig is None:
        _global_fig = go.Figure()
        # Set layout
        _global_fig.update_layout(
            barmode='group',
            xaxis_title=x,
            yaxis_title="Count",
            legend_title="Columns",
            margin=dict(l=20, r=20, t=60, b=20),
        )
    
    # Automatically generate color if not provided
    if color is None:
        # Simple color cycling
        colors = [  '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
                    '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
        bar_count = len([trace for trace in _global_fig.data if isinstance(trace, go.Bar)])
        color = colors[bar_count % len(colors)]
    
    # Add the new bar trace
    _global_fig.add_trace(
        go.Bar(
            x=pdf[x],
            y=pdf[y],
            name=y,
            marker_color=color
        )
    )
    
    # Update title if provided
    if title:
        _global_fig.update_layout(title=title)
    
    # Show the figure
    # _global_fig.show()
    
    return _global_fig

def line(self, y: str, x: Optional[str] = None, fig = None, *,
        grp: Optional[Union[str, Column, List[str]]] = None,
        color: Optional[str] = None, 
        title: Optional[str] = None, 
        sort_by: Optional[str] = None, 
        ascending: bool = False,
        marker_symbol: Optional[str] = None, 
        line_width: int = 2, 
        mode: str = 'lines+markers',
        colors: Optional[List[str]] = None,
        **kwargs) -> go.Figure:
    """
    Add a line plot for the specified column to the current figure.
    
    Parameters:
    -----------
    y : str
        Name of the column to plot on y-axis
    x : str, optional
        Name of the column to plot on x-axis. If None, generates a sequence of integers
    fig : plotly.graph_objects.Figure, optional
        Existing figure to add the plot to. If None, creates a new figure
    grp : str, Column, or List[str], optional
        Column(s) to group by, creating separate lines for each group.
        If None, plots a single line
    color : str, optional
        Color for the line (used when grp is None)
    title : str, optional
        Title for the plot
    sort_by : str, optional
        Column name to sort by (defaults to x if provided)
    ascending : bool, optional
        Sort in ascending order if True (default False)
    marker_symbol : str, optional
        Symbol for markers (circle, square, diamond, etc.)
    line_width : int, optional
        Width of the line (default: 2)
    mode : str, optional
        Plot mode ('lines', 'markers', 'lines+markers')
    colors : List[str], optional
        Custom color list for multiple traces. If None, uses default colors
    **kwargs : 
        Additional keyword arguments passed to plotly's update_layout
        
    Returns:
    --------
    fig : plotly.graph_objects.Figure
        Returns the figure object for further customization
        
    Examples:
    ---------
    # Basic usage
    fig = df.line(y='sales')
    
    # Multiple groups with custom settings
    fig = df.line(y='sales', x='date', grp='region', 
                title='Regional Sales', line_width=3)
    
    # Adding to existing figure
    fig = df.line(y='sales', x='date')
    fig = df.line(y='profit', x='date', fig=fig, color='red')
    
    # Using custom colors
    custom_colors = ['#4C78A8', '#F58518', '#54A24B', '#BA0C2F', '#9467BD']
    fig = df.line(y='sales', grp='region', colors=custom_colors)
    
    # Building a complex figure with multiple calls
    fig = df.line(y='sales', x='month')
    fig = df.line(y='costs', x='month', fig=fig)
    fig = df.line(y='profit', x='month', fig=fig)
    """
    # Input validation
    if y is None:
        raise ValueError("Parameter 'y' is required")
    
    # Default colors if not provided
    if colors is None:
        # Default Plotly colors - can be extended if needed
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
                '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
    
    # Ensure colors is not empty
    if not colors:
        raise ValueError("Colors list cannot be empty")
    
    # Build the selector list for columns to extract
    selector = [y]
    if x is not None:
        selector.append(x)
    
    # Handle grouping parameter
    has_groups = grp is not None
    if has_groups:
        if isinstance(grp, Column):
            # Extract column name from pyspark Column
            grp_cols = re.findall(r"(?<=AS )\w+(?='>$)|\w+(?='>$)", str(grp))
            selector.extend(grp_cols)
            grp = grp_cols[0] if grp_cols else None
        elif isinstance(grp, list):
            selector.extend(grp)
            grp = grp[0] if grp else None
        else:
            selector.append(grp)
    
    # Convert to pandas
    try:
        pdf = self.select(*selector).toPandas()
    except Exception as e:
        raise ValueError(f"Error selecting columns from DataFrame: {e}")
    
    # Generate x values if not provided
    if x is None:
        pdf['_index'] = np.arange(len(pdf))
        x = '_index'
        
    # Sort the data if requested
    if sort_by is not None:
        if sort_by in pdf.columns:
            pdf = pdf.sort_values(by=sort_by, ascending=ascending)
        else:
            raise ValueError(f"Sort column '{sort_by}' not found in DataFrame")
    elif x is not None:
        pdf = pdf.sort_values(by=x, ascending=ascending)
    
    # Create a new figure if none exists
    if fig is None:
        fig = go.Figure()
    
    # Set layout
    layout_params = {
        'xaxis_title': x if x != '_index' else 'Index',
        'yaxis_title': y,
        'margin': dict(l=20, r=20, t=60, b=20)
    }
    
    # Add title and legend title if provided
    if title:
        layout_params['title'] = title
    
    if has_groups:
        layout_params['legend_title'] = grp
    
    # Add any additional kwargs to layout
    layout_params.update(kwargs)
    fig.update_layout(**layout_params)
    
    # Create marker dictionary
    marker_dict = {}
    if marker_symbol:
        marker_dict['symbol'] = marker_symbol
    
    # Count existing scatter traces for proper color cycling
    # This ensures new traces get appropriate colors when adding to an existing figure
    line_count = len([trace for trace in fig.data if isinstance(trace, go.Scatter)])
    
    # Add traces to the figure
    if has_groups:
        # Get unique groups
        try:
            unique_groups = pdf[grp].unique()
        except KeyError:
            raise ValueError(f"Group column '{grp}' not found in DataFrame")
        
        for i, group_val in enumerate(unique_groups):
            group_color = colors[(i + line_count) % len(colors)]
            marker_dict['color'] = group_color
            
            # Filter data for this group
            group_data = pdf[pdf[grp] == group_val]
            
            if len(group_data) == 0:
                continue
                
            # Add trace for this group
            fig.add_trace(
                go.Scatter(
                    name=str(group_val),
                    x=group_data[x],
                    y=group_data[y],
                    mode=mode,
                    line=dict(width=line_width, color=group_color),
                    marker=marker_dict.copy(),
                )
            )
    else:
        # No grouping, just add a single trace
        if color is None:
            color = colors[line_count % len(colors)]
        
        marker_dict['color'] = color
        
        fig.add_trace(
            go.Scatter(
                name=y,
                x=pdf[x],
                y=pdf[y],
                mode=mode,
                line=dict(width=line_width, color=color),
                marker=marker_dict,
            )
        )
    
    return fig
