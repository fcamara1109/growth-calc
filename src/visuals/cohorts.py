import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np

def plot_cohorts(data, period="month"):
    if data.empty:
        st.info("No data available for visualization. Please upload some data first.")
        return
    
    df = pd.DataFrame(data)
    
    if df.empty:
        st.info("No data available for the selected date range.")
        return
    
    # Convert retention rate to percentage
    df['retention_rate'] = df['retention_rate'] * 100
    
    # Add color scale range controls
    col1, col2 = st.columns(2)
    with col1:
        min_value = st.number_input(
            "Heatmap Min. Retention (%)", 
            value=0.0,
            min_value=0.0,
            max_value=100.0,
            step=5.0,
            key=f"retention_min_{period}"
        )
    with col2:
        max_value = st.number_input(
            "Heatmap Max. Retention (%)", 
            value=100.0,
            min_value=0.0,
            max_value=100.0,
            step=5.0,
            key=f"retention_max_{period}"
        )
    
    # Use appropriate column names based on period
    if period == "month":
        first_period = 'first_month'
        periods_since = 'months_since_first'
        max_periods = 24
    elif period == "week":
        first_period = 'first_week'
        periods_since = 'weeks_since_first'
        max_periods = 52
    else:  # daily
        first_period = 'first_dt'
        periods_since = 'days_since_first'
        max_periods = 90
    
    # Convert dates to datetime before pivot
    df[first_period] = pd.to_datetime(df[first_period])
    
    # Sort the dataframe before pivot
    df = df.sort_values(by=[first_period, periods_since])
    
    # Pivot the data for the heatmap
    pivot_df = df.pivot(
        index=first_period,
        columns=periods_since,
        values='retention_rate'
    )
    
    # Sort index explicitly
    pivot_df.index = pd.to_datetime(pivot_df.index)
    pivot_df = pivot_df.sort_index(ascending=True)
    
    # Limit to appropriate number of periods
    pivot_df = pivot_df.loc[:, pivot_df.columns <= max_periods]
    
    # Format dates for display
    y_dates = pivot_df.index.strftime('%Y-%m-%d')
    
    # Create text array with proper formatting
    text_array = []
    for row in pivot_df.values:
        text_row = []
        for val in row:
            if np.isnan(val):
                text_row.append("")
            else:
                text_row.append(f"{val:.1f}%")
        text_array.append(text_row)
    text_array = np.array(text_array)
    
    # Adjust text size and format based on period
    text_size = 10 if period == "month" else 8
    date_format = '%Y-%m' if period == "month" else '%Y-%m-%d'
    
    # Update layout with explicit font settings
    period_title = {
        "month": "Months",
        "week": "Weeks",
        "day": "Days"
    }[period]
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=pivot_df.values,
        x=pivot_df.columns,
        y=y_dates,  # Use formatted dates
        colorscale='RdYlBu',
        text=text_array,
        texttemplate="%{text}",
        textfont={"size": text_size, "family": "JetBrains Mono"},
        hoverongaps=False,
        hovertemplate=f'Cohort: %{{y}}<br>{period_title}: %{{x}}<br>Retention: %{{z:.1f}}%<extra></extra>',
        zmin=min_value,
        zmax=max_value
    ))
    
    fig.update_layout(
        title=f'{period_title} Cohort Retention Rates',
        xaxis_title=f'{period_title} Since First Purchase',
        yaxis_title='Cohort',
        height=600,
        yaxis={
            'autorange': 'reversed',
            'categoryorder': 'category ascending',
            'tickfont': {'size': text_size}  # Adjust y-axis label size
        },
        xaxis={
            'tickfont': {'size': text_size}  # Adjust x-axis label size
        },
        font={'family': 'JetBrains Mono'},
        uniformtext={'minsize': text_size, 'mode': 'show'}
    )
    
    # Force font for all text elements
    fig.update_xaxes(tickfont={'family': 'JetBrains Mono'})
    fig.update_yaxes(tickfont={'family': 'JetBrains Mono'})
    
    # Add styling config
    layout_config = {
        'font': {
            'family': 'JetBrains Mono',
            'color': '#a6ebc9'
        },
        'plot_bgcolor': '#242424',  # Inner plot background
        'paper_bgcolor': '#242424',  # Outer chart background
        'xaxis': {
            'gridcolor': '#393424',
            'linecolor': '#393424',
            'title_font': {'family': 'JetBrains Mono'},
            'tickfont': {'family': 'JetBrains Mono'}
        },
        'yaxis': {
            'gridcolor': '#393424',
            'linecolor': '#393424',
            'title_font': {'family': 'JetBrains Mono'},
            'tickfont': {'family': 'JetBrains Mono'}
        },
        'title_font': {'family': 'JetBrains Mono'}
    }
    
    fig.update_layout(**layout_config)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show raw data in expandable section
    with st.expander("Show Raw Data"):
        st.subheader("Retention Rates (%)")
        st.dataframe(pivot_df)
        
        # Create pivot table for cohort sizes
        cohort_sizes_df = df.pivot(
            index=first_period,
            columns=periods_since,
            values='cohort_num_users'
        ).sort_index(ascending=True)
        
        st.subheader("Cohort Sizes (Number of Users at Start)")
        st.dataframe(cohort_sizes_df)
        
        # Create pivot table for active users
        active_users_df = df.pivot(
            index=first_period,
            columns=periods_since,
            values='users'
        ).sort_index(ascending=True)
        
        st.subheader("Active Users Over Time")
        st.dataframe(active_users_df) 