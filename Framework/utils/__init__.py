import plotly.io as pio
import plotly.graph_objects as go
from copy import deepcopy

# Start with the plotly_white template
# Create a new template instead of trying to copy
custom_template = go.layout.Template()

# You can either start from scratch or use the existing template's dict format
# Here we'll use the existing template as a starting point
template_dict = pio.templates['plotly_white'].to_plotly_json()
custom_template = go.layout.Template(template_dict)

# Configure the paper and plot background colors
custom_template.layout.paper_bgcolor = 'white'
custom_template.layout.plot_bgcolor = 'white'

# Configure the grid lines
custom_template.layout.xaxis = custom_template.layout.xaxis or {}
custom_template.layout.yaxis = custom_template.layout.yaxis or {}

# Set grid properties
custom_template.layout.xaxis.gridcolor = 'lightgray'  # Horizontal grid lines
custom_template.layout.yaxis.gridcolor = 'lightgray'  # Vertical grid lines
custom_template.layout.xaxis.gridwidth = 1
custom_template.layout.yaxis.gridwidth = 1
custom_template.layout.xaxis.griddash = 'dot'  # 'solid', 'dot', 'dash', 'longdash', 'dashdot', 'longdashdot'
custom_template.layout.yaxis.griddash = 'dot'

# Configure zero lines
custom_template.layout.xaxis.zeroline = True
custom_template.layout.yaxis.zeroline = True
custom_template.layout.xaxis.zerolinecolor = 'darkgray'
custom_template.layout.yaxis.zerolinecolor = 'darkgray'
custom_template.layout.xaxis.zerolinewidth = 1
custom_template.layout.yaxis.zerolinewidth = 1

# Font settings
custom_template.layout.font = custom_template.layout.font or {}
custom_template.layout.font.family = "Arial, sans-serif"
custom_template.layout.font.size = 12

# Legend settings
custom_template.layout.legend = custom_template.layout.legend or {}
custom_template.layout.legend.bgcolor = 'rgba(255,255,255,0.9)'
custom_template.layout.legend.bordercolor = 'lightgray'
custom_template.layout.legend.borderwidth = 1

# Register the custom template
pio.templates['custom_white'] = custom_template

# Set it as the default template
pio.templates.default = 'custom_white'

# Function to quickly toggle grid visibility
def set_grid_visibility(visible=True, fig=None):
    """
    Toggle grid visibility for all figures or a specific figure
    
    Parameters:
    -----------
    visible : bool
        Whether the grid lines should be visible
    fig : plotly.graph_objects.Figure, optional
        Specific figure to modify. If None, updates the default template
    """
    if fig is None:
        # Update the default template
        template = pio.templates['custom_white']
        template.layout.xaxis.showgrid = visible
        template.layout.yaxis.showgrid = visible
        pio.templates['custom_white'] = template
    else:
        # Update the specific figure
        fig.update_xaxes(showgrid=visible)
        fig.update_yaxes(showgrid=visible)
        
# Function to change grid properties
def set_grid_properties(color='lightgray', width=1, dash='dot', fig=None):
    """
    Change grid properties for all figures or a specific figure
    
    Parameters:
    -----------
    color : str
        Color of the grid lines
    width : int
        Width of the grid lines
    dash : str
        Dash pattern ('solid', 'dot', 'dash', 'longdash', 'dashdot', 'longdashdot')
    fig : plotly.graph_objects.Figure, optional
        Specific figure to modify. If None, updates the default template
    """
    if fig is None:
        # Update the default template
        template = pio.templates['custom_white']
        template.layout.xaxis.gridcolor = color
        template.layout.yaxis.gridcolor = color
        template.layout.xaxis.gridwidth = width
        template.layout.yaxis.gridwidth = width
        template.layout.xaxis.griddash = dash
        template.layout.yaxis.griddash = dash
        pio.templates['custom_white'] = template
    else:
        # Update the specific figure
        fig.update_xaxes(gridcolor=color, gridwidth=width, griddash=dash)
        fig.update_yaxes(gridcolor=color, gridwidth=width, griddash=dash)